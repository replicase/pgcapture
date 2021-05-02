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

type StaticAgentPulsarURIConfig struct {
	PulsarURL          string
	PulsarTopic        string
	PulsarSubscription string
	AgentURL           string
}

func NewStaticAgentPulsarResolver(config map[string]StaticAgentPulsarURIConfig) *StaticAgentPulsarResolver {
	return &StaticAgentPulsarResolver{config: config}
}

type StaticAgentPulsarResolver struct {
	config map[string]StaticAgentPulsarURIConfig
}

func (r *StaticAgentPulsarResolver) Source(ctx context.Context, uri string) (source.RequeueSource, error) {
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

func (r *StaticAgentPulsarResolver) Dumper(ctx context.Context, uri string) (SourceDumper, error) {
	config, ok := r.config[uri]
	if !ok {
		return nil, ErrURINotFound
	}
	return NewAgentSourceDumper(ctx, config.AgentURL)
}

var ErrURINotFound = errors.New("requested uri not found")
