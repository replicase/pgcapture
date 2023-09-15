package dblog

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/replicase/pgcapture/pkg/pb"
	"google.golang.org/grpc"
)

func TestStaticPGXPulsarResolver_ErrURINotFound(t *testing.T) {
	ctx := context.Background()
	r := NewStaticAgentPulsarResolver(nil)
	if _, err := r.Source(ctx, "any"); !errors.Is(err, ErrURINotFound) {
		t.Fatal("unexpected")
	}
	if _, err := r.Dumper(ctx, "any"); !errors.Is(err, ErrURINotFound) {
		t.Fatal("unexpected")
	}
}

type agent struct {
	pb.UnimplementedAgentServer
}

func TestStaticAgentPulsarResolver(t *testing.T) {
	ctx := context.Background()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server := grpc.NewServer()
	server.RegisterService(&pb.Agent_ServiceDesc, &agent{})
	go server.Serve(lis)

	r := NewStaticAgentPulsarResolver(map[string]StaticAgentPulsarURIConfig{
		URI1: {
			PulsarURL:          "",
			PulsarTopic:        "",
			PulsarSubscription: "",
			AgentURL:           lis.Addr().String(),
		},
	})
	source, err := r.Source(ctx, URI1)
	if source == nil || err != nil {
		t.Fatal(err)
	}
	dumper, err := r.Dumper(ctx, URI1)
	if err != nil {
		t.Fatal(err)
	}
	dumper.Stop()
	server.Stop()
}
