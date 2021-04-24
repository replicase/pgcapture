package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/sink"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	AgentListenAddr string
)

func init() {
	rootCmd.AddCommand(agent)
	agent.Flags().StringVarP(&AgentListenAddr, "ListenAddr", "", ":10000", "the tcp address for agent server to listen")
}

var agent = &cobra.Command{
	Use:   "agent",
	Short: "run as a agent accepting remote config",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		return serveGRPC(&pb.Agent_ServiceDesc, AgentListenAddr, &Agent{})
	},
}

type Agent struct {
	pb.UnimplementedAgentServer

	mu        sync.Mutex
	params    *structpb.Struct
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

func (a *Agent) pg2pulsar(params *structpb.Struct) (*pb.AgentConfigResponse, error) {
	v, err := extract(params, "PGConnURL", "PGReplURL", "PulsarURL", "PulsarTopic")
	if err != nil {
		return nil, err
	}
	pgSrc := &source.PGXSource{SetupConnStr: v["PGConnURL"], ReplConnStr: v["PGReplURL"], ReplSlot: v["PulsarTopic"], CreateSlot: true}
	pulsarSink := &sink.PulsarSink{PulsarOption: pulsar.ClientOptions{URL: v["PulsarURL"]}, PulsarTopic: v["PulsarTopic"]}
	if err := a.sourceToSink(pgSrc, pulsarSink); err != nil {
		return nil, err
	}

	a.params = params
	return a.report(a.params)
}

func (a *Agent) pulsar2pg(params *structpb.Struct) (*pb.AgentConfigResponse, error) {
	v, err := extract(params, "PGConnURL", "PGLogPath", "PulsarURL", "PulsarTopic")
	if err != nil {
		return nil, err
	}

	pgLog, err := os.Open(SinkPGLogPath)
	if err != nil {
		return nil, err
	}
	pulsarSrc := &source.PulsarReaderSource{PulsarOption: pulsar.ClientOptions{URL: v["PulsarURL"]}, PulsarTopic: v["PulsarTopic"]}
	pgSink := &sink.PGXSink{ConnStr: v["PGConnURL"], SourceID: v["PulsarTopic"], LogReader: pgLog}
	if err = a.sourceToSink(pulsarSrc, pgSink); err != nil {
		pgLog.Close()
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
		return err
	}

	go func() {
		checkpoints := sk.Apply(changes)
		for cp := range checkpoints {
			src.Commit(cp)
		}
		a.mu.Lock()
		defer a.mu.Unlock()

		sk.Stop()
		src.Stop()
		a.params = nil
		a.sinkErr = sk.Error()
		a.sourceErr = src.Error()
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
