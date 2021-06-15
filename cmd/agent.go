package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rueian/pgcapture/pkg/dblog"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/sink"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	AgentListenAddr string
	AgentRenice     int64
)

func init() {
	rootCmd.AddCommand(agent)
	agent.Flags().StringVarP(&AgentListenAddr, "ListenAddr", "", ":10000", "the tcp address for agent server to listen")
	agent.Flags().Int64VarP(&AgentRenice, "Renice", "", -10, "try renice the sink pg process")
}

var agent = &cobra.Command{
	Use:   "agent",
	Short: "run as a agent accepting remote config",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		logrus.WithFields(logrus.Fields{
			"AgentListenAddr": AgentListenAddr,
		}).Info("starting agent")
		return serveGRPC(&pb.Agent_ServiceDesc, AgentListenAddr, &Agent{})
	},
}

type Agent struct {
	pb.UnimplementedAgentServer

	mu        sync.Mutex
	params    *structpb.Struct
	dumper    *dblog.PGXSourceDumper
	sinkErr   error
	sourceErr error
}

func (a *Agent) Configure(ctx context.Context, request *pb.AgentConfigRequest) (*pb.AgentConfigResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.params != nil {
		return a.report(a.params)
	}

	var params *structpb.Struct

	if params = request.GetParameters(); params == nil {
		return nil, errors.New("parameter is required")
	}

	if v, err := extract(params, "Command"); err != nil {
		return nil, err
	} else {
		switch v["Command"] {
		case "pg2pulsar":
			return a.pg2pulsar(params)
		case "pulsar2pg":
			return a.pulsar2pg(params)
		case "status":
			return a.report(a.params)
		default:
			return nil, errors.New("'Command' should be one of [pg2pulsar|pulsar2pg|status]")
		}
	}
}

func (a *Agent) Dump(ctx context.Context, req *pb.AgentDumpRequest) (*pb.AgentDumpResponse, error) {
	var dumper *dblog.PGXSourceDumper
	a.mu.Lock()
	dumper = a.dumper
	a.mu.Unlock()

	if dumper == nil {
		return nil, status.Error(codes.Aborted, "dumper is not ready")
	}

	dump, err := dumper.LoadDump(req.MinLsn, req.Info)
	if err != nil {
		switch err {
		case dblog.ErrMissingTable:
			return nil, status.Error(codes.NotFound, err.Error())
		case dblog.ErrLSNMissing:
			return nil, status.Error(codes.Unavailable, err.Error())
		case dblog.ErrLSNFallBehind:
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, err
	}
	return &pb.AgentDumpResponse{Change: dump}, nil
}

func (a *Agent) StreamDump(req *pb.AgentDumpRequest, server pb.Agent_StreamDumpServer) error {
	resp, err := a.Dump(server.Context(), req)
	if err != nil {
		return err
	}
	for _, change := range resp.Change {
		if err = server.Send(change); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) pg2pulsar(params *structpb.Struct) (*pb.AgentConfigResponse, error) {
	v, err := extract(params, "PGConnURL", "PGReplURL", "PulsarURL", "PulsarTopic")
	if err != nil {
		return nil, err
	}
	pgSrc := &source.PGXSource{SetupConnStr: v["PGConnURL"], ReplConnStr: v["PGReplURL"], ReplSlot: trimSlot(v["PulsarTopic"]), CreateSlot: true}
	pulsarSink := &sink.PulsarSink{PulsarOption: pulsar.ClientOptions{URL: v["PulsarURL"]}, PulsarTopic: v["PulsarTopic"]}

	logger := logrus.WithFields(logrus.Fields{
		"PulsarURL":   v["PulsarURL"],
		"PulsarTopic": v["PulsarTopic"],
	})
	logger.Info("start pg2pulsar")

	if err := a.sourceToSink(pgSrc, pulsarSink); err != nil {
		logger.Fatalf("sourceToSink err: %v", err)
		return nil, err
	}

	a.params = params
	return a.report(a.params)
}

func (a *Agent) pulsar2pg(params *structpb.Struct) (*pb.AgentConfigResponse, error) {
	v, err := extract(params, "PGConnURL", "PulsarURL", "PulsarTopic")
	if err != nil {
		return nil, err
	}

	pgSink := &sink.PGXSink{ConnStr: v["PGConnURL"], SourceID: trimSlot(v["PulsarTopic"]), Renice: AgentRenice, LogReader: nil}
	if v, err := extract(params, "PGLogPath"); err == nil {
		pgLog, err := os.Open(v["PGLogPath"])
		if err != nil {
			return nil, err
		}
		defer pgLog.Close()
		pgSink.LogReader = pgLog
	}

	dumper, err := dblog.NewPGXSourceDumper(context.Background(), v["PGConnURL"])
	if err != nil {
		return nil, err
	}

	a.dumper = dumper

	logger := logrus.WithFields(logrus.Fields{
		"PulsarURL":   v["PulsarURL"],
		"PulsarTopic": v["PulsarTopic"],
		"PGLogPath":   v["PGLogPath"],
	})
	logger.Info("start pulsar2pg")

	pulsarSrc := &source.PulsarReaderSource{PulsarOption: pulsar.ClientOptions{URL: v["PulsarURL"]}, PulsarTopic: v["PulsarTopic"]}
	if err = a.sourceToSink(pulsarSrc, pgSink); err != nil {
		logger.Fatalf("sourceToSink error: %v", err)
		return nil, err
	}

	a.params = params
	return a.report(a.params)
}

func (a *Agent) sourceToSink(src source.Source, sk sink.Sink) (err error) {
	lastCheckPoint, err := sk.Setup()
	if err != nil {
		return err
	}

	changes, err := src.Capture(lastCheckPoint)
	if err != nil {
		sk.Stop()
		return err
	}

	go func() {
		checkpoints := sk.Apply(changes)
		for cp := range checkpoints {
			src.Commit(cp)
		}
	}()
	go func() {
		check := func() bool {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.sinkErr = sk.Error()
			a.sourceErr = src.Error()
			if a.sinkErr != nil {
				a.params = nil
				logrus.Errorf("sink error: %v", a.sinkErr)
			}
			if a.sourceErr != nil {
				a.params = nil
				logrus.Errorf("source error: %v", a.sourceErr)
			}
			if a.dumper != nil && (a.sourceErr != nil || a.sinkErr != nil) {
				a.dumper.Stop()
				a.dumper = nil
			}
			return a.sinkErr == nil && a.sourceErr == nil
		}
		for check() {
			time.Sleep(time.Second)
		}
		sk.Stop()
		src.Stop()
	}()

	a.sinkErr = nil
	a.sourceErr = nil

	return nil
}

func (a *Agent) report(params *structpb.Struct) (*pb.AgentConfigResponse, error) {
	if a.sinkErr != nil || a.sourceErr != nil {
		return nil, fmt.Errorf("sinkErr: %v, sourceErr: %v", a.sinkErr, a.sourceErr)
	}
	return &pb.AgentConfigResponse{Report: params}, nil
}

func extract(params *structpb.Struct, keys ...string) (map[string]string, error) {
	values := map[string]string{}
	for _, k := range keys {
		if fields := params.GetFields(); fields == nil || fields[k] == nil || fields[k].GetStringValue() == "" {
			return nil, fmt.Errorf("%s key is required in parameters", k)
		} else {
			values[k] = fields[k].GetStringValue()
		}
	}
	return values, nil
}
