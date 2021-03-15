package dblog

import (
	"context"
	"testing"
)

func TestStaticPGXPulsarResolver_ErrURINotFound(t *testing.T) {
	ctx := context.Background()
	r := NewStaticPGXPulsarResolver(nil)
	if _, err := r.Source(ctx, "any"); err != ErrURINotFound {
		t.Fatal("unexpected")
	}
	if _, err := r.Dumper(ctx, "any"); err != ErrURINotFound {
		t.Fatal("unexpected")
	}
}

func TestStaticPGXPulsarResolver(t *testing.T) {
	ctx := context.Background()
	r := NewStaticPGXPulsarResolver(map[string]StaticPGXPulsarURIConfig{
		URI1: {
			PulsarURL:          "",
			PulsarTopic:        "",
			PulsarSubscription: "",
			PostgresURL:        "postgres://postgres@127.0.0.1/postgres?sslmode=disable",
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
}
