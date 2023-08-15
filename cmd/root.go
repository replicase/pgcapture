package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rueian/pgcapture/pkg/sink"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var ProfilerListenAddr string

func init() {
	rootCmd.Flags().StringVarP(&ProfilerListenAddr, "ProfilerListenAddr", "", "localhost:6060", "golang profiler http endpoint")
}

var rootCmd = &cobra.Command{
	Use: "pgcapture",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		go func() {
			if ProfilerListenAddr != "" {
				log.Println(http.ListenAndServe(ProfilerListenAddr, nil))
			}
		}()
	},
}

func main() {
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
	logrus.Info("receive signal, stopping...")
	sk.Stop()
	src.Stop()
	logrus.Info("receive signal, stopped")
	if err := sk.Error(); err != nil {
		return err
	}
	if err := src.Error(); err != nil {
		return err
	}
	return nil
}

func serveGRPC(desc *grpc.ServiceDesc, addr string, impl interface{}, clean func()) (err error) {
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
		logrus.Info("receive signal, stopping grpc server...")
		server.GracefulStop()
		logrus.Info("receive signal, cleaning up...")
		clean()
		logrus.Info("receive signal, stopped")
	}()

	return server.Serve(lis)
}

func trimSlot(topic string) string {
	topic = strings.TrimPrefix(topic, "persistent://public/")
	topic = strings.ReplaceAll(topic, "/", "_")
	topic = strings.ReplaceAll(topic, "-", "_")
	return topic
}

func startPrometheusServer(addr string) {
	handler := promhttp.Handler()
	server := &http.Server{
		Addr: addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodGet && req.URL.Path == "/metrics" {
				handler.ServeHTTP(w, req)
			} else {
				http.NotFound(w, req)
			}
		}),
	}

	logrus.WithFields(logrus.Fields{"addr": addr}).Info("starting prometheus server")
	go func() {
		if err := server.ListenAndServe(); err != nil {
			logrus.Error(err)
		}
	}()
}
