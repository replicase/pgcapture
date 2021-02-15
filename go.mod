module github.com/rueian/pgcapture

go 1.15

replace github.com/apache/pulsar-client-go => github.com/rueian/pulsar-client-go v0.4.0

require (
	github.com/apache/pulsar-client-go v0.4.0
	github.com/golang/protobuf v1.4.3
	github.com/jackc/pgconn v1.8.0
	github.com/jackc/pglogrepl v0.0.0-20210109153808-a78a685a0bff
	github.com/jackc/pgproto3/v2 v2.0.7
	github.com/jackc/pgtype v1.6.3-0.20210115001048-6830cc09847c
	github.com/jackc/pgx/v4 v4.6.1-0.20200606145419-4e5062306904
	golang.org/x/crypto v0.0.0-20201221181555-eec23a3978ad // indirect
	golang.org/x/text v0.3.5 // indirect
	google.golang.org/protobuf v1.25.0
)
