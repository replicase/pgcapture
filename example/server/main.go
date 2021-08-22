package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rueian/pgcapture/example"
	"github.com/rueian/pgcapture/internal/test"
	"github.com/rueian/pgcapture/pkg/dblog"
	"github.com/rueian/pgcapture/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

func main() {
	if _, err := test.CreateDB(example.DefaultDB, example.SrcDB); err != nil {
		panic(err)
	}

	if _, err := test.CreateDB(example.DefaultDB, example.SinkDB); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	ctx := context.Background()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	resolver := map[string]dblog.StaticAgentPulsarURIConfig{
		example.SrcDB.DB: {
			AgentURL:           example.AgentAddr2,
			PulsarURL:          example.PulsarURL,
			PulsarTopic:        example.SrcDB.DB,
			PulsarSubscription: example.SrcDB.DB,
		},
	}
	cfg, _ := json.Marshal(resolver)

	for _, cmd := range []cmd{
		{
			Name: "agent",
			Flags: map[string]string{
				"ListenAddr": example.AgentAddr1,
			},
		},
		{
			Name: "agent",
			Flags: map[string]string{
				"ListenAddr": example.AgentAddr2,
			},
		},
		{
			Name: "controller",
			Flags: map[string]string{
				"ListenAddr": example.ControlAddr,
			},
		},
		{
			Name: "gateway",
			Flags: map[string]string{
				"ListenAddr":     example.GatewayAddr,
				"ControllerAddr": example.ControlAddr,
				"ResolverConfig": string(cfg),
			},
		},
	} {
		wg.Add(1)
		go run(ctx, cmd, &wg)
	}

	var conn1, conn2 *grpc.ClientConn
	var agent1, agent2 pb.AgentClient

	conn1, err := grpc.Dial(example.AgentAddr1, grpc.WithBlock(), grpc.WithInsecure())
	if err != nil {
		log.Println(err)
		goto wait
	}
	conn2, err = grpc.Dial(example.AgentAddr2, grpc.WithBlock(), grpc.WithInsecure())
	if err != nil {
		log.Println(err)
		goto wait
	}

	agent1 = pb.NewAgentClient(conn1)
	agent2 = pb.NewAgentClient(conn2)

	for {
		time.Sleep(time.Second)
		params, _ := structpb.NewStruct(map[string]interface{}{
			"Command":     "pg2pulsar",
			"PGConnURL":   example.SrcDB.URL(),
			"PGReplURL":   example.SrcDB.Repl(),
			"PulsarURL":   example.PulsarURL,
			"PulsarTopic": example.SrcDB.DB,
		})
		if _, err := agent1.Configure(ctx, &pb.AgentConfigRequest{Parameters: params}); err != nil {
			log.Println(err)
			goto wait
		}
		params, _ = structpb.NewStruct(map[string]interface{}{
			"Command":     "pulsar2pg",
			"PGConnURL":   example.SinkDB.URL(),
			"PulsarURL":   example.PulsarURL,
			"PulsarTopic": example.SrcDB.DB,
		})
		if _, err := agent2.Configure(ctx, &pb.AgentConfigRequest{Parameters: params}); err != nil {
			log.Println(err)
			goto wait
		}
	}
wait:
	wg.Wait()
}

type cmd struct {
	Name  string
	Flags map[string]string
}

func run(ctx context.Context, cmd cmd, wg *sync.WaitGroup) {
	args := []string{"run", "main.go", cmd.Name}
	for k, v := range cmd.Flags {
		args = append(args, fmt.Sprintf("--%s=%s", k, v))
	}
	c := exec.CommandContext(ctx, "go", args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	log.Println(cmd.Name, c.Run())
	wg.Done()
}
