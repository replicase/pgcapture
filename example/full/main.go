package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/internal/test"
	"github.com/rueian/pgcapture/pkg/dblog"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/sink"
	"github.com/rueian/pgcapture/pkg/source"
	"google.golang.org/grpc"
)

const PGHost = "127.0.0.1"
const PulsarURL = "pulsar://127.0.0.1:6650"
const TestTable = "test"

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	url := test.DBURL{Host: PGHost, DB: "postgres"}

	dbSrc, err := test.RandomDB(url)
	if err != nil {
		panic(err)
	}

	dbSink, err := test.RandomDB(url)
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

		pgSrc := source.PGXSource{SetupConnStr: dbSrc.URL(), ReplConnStr: dbSrc.Repl(), ReplSlot: dbSrc.DB, CreateSlot: true}
		pulsarSink := sink.PulsarSink{PulsarOption: pulsar.ClientOptions{URL: PulsarURL}, PulsarTopic: dbSrc.DB}

		lastCheckPoint, err := pulsarSink.Setup()
		if err != nil {
			panic(err)
		}

		changes, err := pgSrc.Capture(lastCheckPoint)
		if err != nil {
			panic(err)
		}

		go func() {
			checkpoints := pulsarSink.Apply(changes)
			for cp := range checkpoints {
				pgSrc.Commit(cp)
			}
		}()

		// insert random data
		go func() {
			for {
				select {
				case <-shutdown:
					return
				default:
				}
				if err := dbSrc.RandomData(TestTable); err != nil {
					panic(err)
				}
				if rand.Intn(20) == 0 {
					if err = dbSrc.CleanData(TestTable); err != nil {
						panic(err)
					}
				}
			}
		}()

		<-shutdown
		pulsarSink.Stop()
		pgSrc.Stop()
		if err := pulsarSink.Error(); err != nil {
			panic(err)
		}
		if err := pgSrc.Error(); err != nil {
			panic(err)
		}
	}()

	// apply logical replication to dbSink from pulsar
	wg.Add(1)
	go func() {
		defer wg.Done()

		pulsarSrc := source.PulsarReaderSource{PulsarOption: pulsar.ClientOptions{URL: PulsarURL}, PulsarTopic: dbSrc.DB}

		pgSink := sink.PGXSink{ConnStr: dbSink.URL(), SourceID: dbSrc.DB}

		lastCheckPoint, err := pgSink.Setup()
		if err != nil {
			panic(err)
		}

		lastCheckPoint.Time = time.Now().Add(-5 * time.Second)

		changes, err := pulsarSrc.Capture(lastCheckPoint)
		if err != nil {
			panic(err)
		}

		go func() {
			checkpoints := pgSink.Apply(changes)
			for cp := range checkpoints {
				pulsarSrc.Commit(cp)
			}
		}()

		<-shutdown
		pgSink.Stop()
		pulsarSrc.Stop()
		if err := pgSink.Error(); err != nil {
			panic(err)
		}
		if err := pulsarSrc.Error(); err != nil {
			panic(err)
		}
	}()

	// start dblog control server
	control := &dblog.Controller{Scheduler: dblog.NewMemoryScheduler(time.Millisecond * 100)}
	controlAddr, controlCancel := test.NewGRPCServer(&pb.DBLogController_ServiceDesc, control)
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
	gatewayAddr, gatewayCancel := test.NewGRPCServer(&pb.DBLogGateway_ServiceDesc, gateway)

	// consume random data
	go func() {
		conn, err := grpc.Dial(gatewayAddr.String(), grpc.WithInsecure())
		if err != nil {
			panic(err)
		}
		defer conn.Close()
		client := pb.NewDBLogGatewayClient(conn)
		stream, err := client.Capture(context.Background())
		if err != nil {
			panic(err)
		}
		if err = stream.Send(&pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: dbSrc.DB}}}); err != nil {
			panic(err)
		}
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			if err = stream.Send(&pb.CaptureRequest{Type: &pb.CaptureRequest_Ack{Ack: &pb.CaptureAck{Checkpoint: msg.Checkpoint}}}); err != nil {
				return
			}
			if msg.Checkpoint == 0 {
				fmt.Println("DUMP", msg.Change.String())
			} else {
				fmt.Println("LAST", msg.Change.String())
			}
		}
	}()

	go func() {
		conn, err := grpc.Dial(controlAddr.String(), grpc.WithInsecure())
		if err != nil {
			panic(err)
		}
		defer conn.Close()
		client := pb.NewDBLogControllerClient(conn)

		for {
			pages, _ := dbSink.TablePages(TestTable)
			dumps := make([]*pb.DumpInfoResponse, pages)
			for i := uint32(0); i < uint32(pages); i++ {
				dumps[i] = &pb.DumpInfoResponse{Namespace: "public", Table: "test", PageBegin: i, PageEnd: i}
			}
			if pages > 0 {
				_, _ = client.Schedule(context.Background(), &pb.ScheduleRequest{Uri: dbSrc.DB, Dumps: dumps})
			}
			time.Sleep(time.Second * 5)
		}
	}()

	<-shutdown
	gatewayCancel()
	controlCancel()

	wg.Wait()
}

type PulsarSourceResolver struct {
}

func (r *PulsarSourceResolver) Resolve(ctx context.Context, uri string) (source.RequeueSource, error) {
	return &source.PulsarConsumerSource{
		PulsarOption:       pulsar.ClientOptions{URL: PulsarURL},
		PulsarTopic:        uri,
		PulsarSubscription: uri,
	}, nil
}
