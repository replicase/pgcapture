package pgcapture

import (
	"context"

	"github.com/replicase/pgcapture/pkg/dblog"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/replicase/pgcapture/pkg/pgcapture"
	"github.com/replicase/pgcapture/pkg/source"
	"google.golang.org/grpc"
)

var (
	CommitSHA string
	Version   string
)

type (
	Model            = pgcapture.Model
	Handlers         = pgcapture.Handlers
	Table            = pgcapture.Table
	Message          = pgcapture.Message
	Change           = pgcapture.Change
	ModelHandlerFunc = pgcapture.ModelHandlerFunc
	HandlerFunc      = pgcapture.HandlerFunc
	AsyncHandlerFunc = pgcapture.AsyncHandlerFunc
	ConsumerOption   = pgcapture.ConsumerOption
	SourceResolver   = dblog.SourceResolver
	SourceDumper     = dblog.SourceDumper
	RequeueSource    = source.RequeueSource
)

func NewDBLogConsumer(ctx context.Context, conn *grpc.ClientConn, option ConsumerOption) *pgcapture.Consumer {
	return pgcapture.NewDBLogConsumer(ctx, conn, option)
}

func NewDBLogGateway(conn *grpc.ClientConn, sourceResolver SourceResolver) *dblog.Gateway {
	return &dblog.Gateway{
		SourceResolver: sourceResolver,
		DumpInfoPuller: &dblog.GRPCDumpInfoPuller{Client: pb.NewDBLogControllerClient(conn)},
	}
}

func NewDBLogControllerClient(conn *grpc.ClientConn) pb.DBLogControllerClient {
	return pb.NewDBLogControllerClient(conn)
}

func MarshalJSON(m Model) ([]byte, error) {
	return pgcapture.MarshalJSON(m)
}
