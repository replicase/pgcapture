package dblog

import (
	"context"

	"github.com/rueian/pgcapture/pkg/source"
)

type SourceResolver interface {
	Source(ctx context.Context, uri string) (source.RequeueSource, error)
	Dumper(ctx context.Context, uri string) (SourceDumper, error)
}
