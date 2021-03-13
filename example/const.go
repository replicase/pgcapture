package example

import "github.com/rueian/pgcapture/internal/test"

const (
	PGHost      = "127.0.0.1"
	PulsarURL   = "pulsar://127.0.0.1:6650"
	TestTable   = "test"
	TestDBSrc   = "db_src"
	TestDBSink  = "db_sink"
	ControlAddr = "localhost:10000"
	GatewayAddr = "localhost:10001"
)

var (
	DefaultDB = test.DBURL{Host: PGHost, DB: "postgres"}
	SinkDB    = test.DBURL{Host: PGHost, DB: TestDBSink}
	SrcDB     = test.DBURL{Host: PGHost, DB: TestDBSrc}
)
