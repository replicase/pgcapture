module github.com/rueian/pgcapture

go 1.15

replace github.com/pganalyze/pg_query_go/v2 v2.0.2 => github.com/rueian/pg_query_go/v2 v2.0.3-0.20210404160231-00fbdb47649c

require (
	github.com/apache/pulsar-client-go v0.6.1-0.20210818190554-bbffae2bfbdc
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/jackc/pgconn v1.8.1
	github.com/jackc/pglogrepl v0.0.0-20210109153808-a78a685a0bff
	github.com/jackc/pgproto3/v2 v2.0.7
	github.com/jackc/pgtype v1.7.0
	github.com/jackc/pgx/v4 v4.10.1
	github.com/pganalyze/pg_query_go/v2 v2.0.2
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/cobra v0.0.5
	github.com/streamnative/pulsarctl v0.4.3-0.20230131034553-1b71e953e372
	google.golang.org/grpc v1.36.0
	google.golang.org/protobuf v1.26.0
)
