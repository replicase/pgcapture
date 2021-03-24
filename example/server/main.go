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

	"github.com/rueian/pgcapture/example"
	"github.com/rueian/pgcapture/internal/test"
	"github.com/rueian/pgcapture/pkg/dblog"
)

func main() {
	if _, err := test.CreateDB(example.DefaultDB, example.SrcDB); err != nil {
		panic(err)
	}

	if _, err := test.CreateDB(example.DefaultDB, example.SinkDB); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	ctx, _ := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	resolver := map[string]dblog.StaticPGXPulsarURIConfig{
		example.SrcDB.DB: {
			PostgresURL:        example.SinkDB.URL(),
			PulsarURL:          example.PulsarURL,
			PulsarTopic:        example.SrcDB.DB,
			PulsarSubscription: example.SrcDB.DB,
		},
	}
	cfg, _ := json.Marshal(resolver)

	for _, cmd := range []cmd{
		{
			Name: "pg2pulsar",
			Flags: map[string]string{
				"PGConnURL":   example.SrcDB.URL(),
				"PGReplURL":   example.SrcDB.Repl(),
				"PulsarURL":   example.PulsarURL,
				"PulsarTopic": example.SrcDB.DB,
			},
		},
		{
			Name: "pulsar2pg",
			Flags: map[string]string{
				"PGConnURL":   example.SinkDB.URL(),
				"PulsarURL":   example.PulsarURL,
				"PulsarTopic": example.SrcDB.DB,
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
