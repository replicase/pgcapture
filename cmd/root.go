package cmd

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/rueian/pgcapture/pkg/sink"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var rootCmd = &cobra.Command{
	Use: "pgcapture",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func sourceToSink(src source.Source, sk sink.Sink) (err error) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

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
	}()

	<-signals
	sk.Stop()
	src.Stop()
	if err := sk.Error(); err != nil {
		return err
	}
	if err := src.Error(); err != nil {
		return err
	}
	return nil
}

func serveGRPC(desc *grpc.ServiceDesc, addr string, impl interface{}) (err error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	server.RegisterService(desc, impl)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		<-signals
		server.GracefulStop()
	}()

	return server.Serve(lis)
}

func trimSlot(topic string) string {
	topic = strings.TrimPrefix(topic, "persistent://public/")
	topic = strings.ReplaceAll(topic, "/", "_")
	topic = strings.ReplaceAll(topic, "-", "_")
	return topic
}
