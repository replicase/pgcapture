package dblog

import (
	"context"
	"errors"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rueian/pgcapture/pkg/source"
)

type SourceResolver interface {
	Source(ctx context.Context, uri string) (source.RequeueSource, error)
	Dumper(ctx context.Context, uri string) (SourceDumper, error)
}

type StaticPGXPulsarURIConfig struct {
	PulsarURL          string
	PulsarTopic        string
	PulsarSubscription string
	PostgresURL        string
}

func NewStaticPGXPulsarResolver(config map[string]StaticPGXPulsarURIConfig) *StaticPGXPulsarResolver {
	return &StaticPGXPulsarResolver{config: config}
}

type StaticPGXPulsarResolver struct {
	config map[string]StaticPGXPulsarURIConfig
}

func (r *StaticPGXPulsarResolver) Source(ctx context.Context, uri string) (source.RequeueSource, error) {
	config, ok := r.config[uri]
	if !ok {
		return nil, ErrURINotFound
	}
	return &source.PulsarConsumerSource{
		PulsarOption:       pulsar.ClientOptions{URL: config.PulsarURL},
		PulsarTopic:        config.PulsarTopic,
		PulsarSubscription: config.PulsarSubscription,
	}, nil
}

func (r *StaticPGXPulsarResolver) Dumper(ctx context.Context, uri string) (SourceDumper, error) {
	config, ok := r.config[uri]
	if !ok {
		return nil, ErrURINotFound
	}
	return NewPGXSourceDumper(ctx, config.PostgresURL)
}

var ErrURINotFound = errors.New("requested uri not found")
