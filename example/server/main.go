package main

import (
	"context"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/example"
	"github.com/rueian/pgcapture/internal/test"
	"github.com/rueian/pgcapture/pkg/dblog"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/sink"
	"github.com/rueian/pgcapture/pkg/source"
	"google.golang.org/grpc"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	dbSrc, err := test.CreateDB(example.DefaultDB, example.SrcDB)
	if err != nil {
		panic(err)
	}

	dbSink, err := test.CreateDB(example.DefaultDB, example.SinkDB)
	if err != nil {
		panic(err)
	}

	shutdown := make(chan struct{})
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		<-signals
		close(shutdown)
	}()

	var wg sync.WaitGroup

	// logical replication to pulsar topic
	wg.Add(1)
	go func() {
		defer wg.Done()

		pgSrc := &source.PGXSource{SetupConnStr: dbSrc.URL(), ReplConnStr: dbSrc.Repl(), ReplSlot: dbSrc.DB, CreateSlot: true}
		pulsarSink := &sink.PulsarSink{PulsarOption: pulsar.ClientOptions{URL: example.PulsarURL}, PulsarTopic: dbSrc.DB}
		if err := test.SourceToSink(pgSrc, pulsarSink, shutdown); err != nil {
			panic(err)
		}
	}()

	// apply logical replication to dbSink from pulsar
	wg.Add(1)
	go func() {
		defer wg.Done()

		pulsarSrc := &source.PulsarReaderSource{PulsarOption: pulsar.ClientOptions{URL: example.PulsarURL}, PulsarTopic: dbSrc.DB}
		pgSink := &sink.PGXSink{ConnStr: dbSink.URL(), SourceID: dbSrc.DB}
		if err := test.SourceToSink(pulsarSrc, pgSink, shutdown); err != nil {
			panic(err)
		}
	}()

	// start dblog control server
	control := &dblog.Controller{Scheduler: dblog.NewMemoryScheduler(time.Millisecond * 100)}
	controlAddr, controlCancel := test.NewGRPCServer(&pb.DBLogController_ServiceDesc, example.ControlAddr, control)
	defer controlCancel()

	// start dblog gateway
	controlConn, err := grpc.Dial(controlAddr.String(), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	pgConn, err := pgx.Connect(context.Background(), dbSink.URL())
	if err != nil {
		panic(err)
	}
	gateway := &dblog.Gateway{
		SourceResolver: &PulsarSourceResolver{},
		SourceDumper:   &dblog.PGXSourceDumper{Conn: pgConn},
		DumpInfoPuller: &dblog.GRPCDumpInfoPuller{Client: pb.NewDBLogControllerClient(controlConn)},
	}
	_, gatewayCancel := test.NewGRPCServer(&pb.DBLogGateway_ServiceDesc, example.GatewayAddr, gateway)

	<-shutdown
	gatewayCancel()
	controlCancel()

	wg.Wait()
}

type PulsarSourceResolver struct {
}

func (r *PulsarSourceResolver) Resolve(ctx context.Context, uri string) (source.RequeueSource, error) {
	return &source.PulsarConsumerSource{
		PulsarOption:       pulsar.ClientOptions{URL: example.PulsarURL},
		PulsarTopic:        uri,
		PulsarSubscription: uri,
	}, nil
}
