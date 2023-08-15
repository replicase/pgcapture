# pgcapture

A scalable Netflix DBLog implementation for PostgreSQL

![circleci](https://circleci.com/gh/rueian/pgcapture.svg?style=shield)
[![Maintainability](https://api.codeclimate.com/v1/badges/efd0f50a92233b34ae5e/maintainability)](https://codeclimate.com/github/rueian/pgcapture/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/efd0f50a92233b34ae5e/test_coverage)](https://codeclimate.com/github/rueian/pgcapture/test_coverage)

![overview](./hack/images/overview.png)

## Features
* DDL commands are also captured
* One unified gRPC Streaming API for consuming the latest changes and on-demand dumps
* The changes and dumps are streamed in Postgres Binary Representation to save bandwidth

## Improvements to Netflix DBLog
* Dumps are neither queried from source database nor injected into the source CDC stream.
  Instead, They are dumped from the logical replicas and are injected into the selected downstreams by the grpc gateway service.
* Therefore, the on-demand dump process can be scaled by adding more logical replicas and consumers.
  And most importantly, dumps process will not have impact to source database as well as other downstream consumers who don't need those dumps.   
* Primary keys of tables aren't limited to be a single numeric column, because dumps are performed by PostgreSQL TID Scan instead of performed on the primary key.
  
## Use cases
* Robust Microservice Event Sourcing
* Data synchronization, Moving data to other databases (ex. for OLAP)
* Upgrade PostgreSQL with minimum downtime

## Decode Plugin
The decode plugin is a shared library that can be loaded into PostgreSQL to decode logical changes.
Currently, pgcapture can work with the following plugins:
1. pglogical_output 

     The pglogical_output plugin is come from [pglogical](https://github.com/2ndQuadrant/pglogical) which can support binary representation of the changes. In order to use pglogical_output plugin, you must install pglogical extension first. Also, pgcapture would not create pglogical local node automatically, so you must create it manually. Or you can add a [patch](hack/postgres/14/pglogical/pglogical.patch) to change pglogical source code to ignore checking local node.

2. pgoutput

     The pgoutput plugin is standard PostgreSQL plugin which can support binary representation of the changes only when PostgreSQL version >= 14. So, if you want to use pgoutput plugin, your PostgreSQL version must be >= 14. Currently, pgoutput plugin is default plugin of pgcapture.

## Dependencies
* pglogical postgresql extension (it's optional unless you want to use pglogical_output plugin)
* pgcapture postgresql extension (it's required because it captures DDL commands)

See [./hack/postgres](./hack/postgres) for installation guide of each PostgreSQL version.

## Consume changes with Golang

```golang
package main

import (
    "context"

    "github.com/jackc/pgtype"
    "github.com/rueian/pgcapture"
    "google.golang.org/grpc"
)

// MyTable implements pgcapture.Model interface
// and will be decoded from change that matching the TableName()
type MyTable struct {
    ID    pgtype.Int4 `pg:"id"`       // the field needed to be decoded should be a pgtype struct, 
    Value pgtype.Text `pg:"my_value"` // and has a 'pg' tag specifying the name mapping explicitly
}

func (t *MyTable) TableName() (schema, table string) {
    return "public", "my_table"
}

func (t MyTable) MarshalJSON() ([]byte, error) {
    return pgcapture.MarshalJSON(&t) // ignore unchanged TOAST field
}

func main() {
    ctx := context.Background()

    conn, _ := grpc.Dial("127.0.0.1:1000", grpc.WithInsecure())
    defer conn.Close()

    consumer := pgcapture.NewDBLogConsumer(ctx, conn, pgcapture.ConsumerOption{
        // the uri identify which change stream you want.
        // you can implement dblog.SourceResolver to customize gateway behavior based on uri
        URI: "my_subscription_id", 
    })
    defer consumer.Stop()

    consumer.Consume(map[pgcapture.Model]pgcapture.ModelHandlerFunc{
        &MyTable{}: func(change pgcapture.Change) error {
            row := change.New.(*MyTable) 
            // and then handle the decoded change event

            if row.Value.Status == pgtype.Undefined {
                // handle the unchanged toast field
            }

            return nil
        },
    })
}
```

### Handling unchanged TOAST field

Since unchanged TOAST fields will not be present in the change stream, the corresponding model fields will remain undefined and have no value.
Users should verify them by checking the field status.

The `pgcapture.MarshalJSON` is a handy `json.Marshaler` that just ignore those undefined fields.

## Customize the `dblog.SourceResolver`

The provided `gateway` sub command will start a gateway server with `dblog.StaticAgentPulsarResolver` which reads a static URI resolving config.
However, it is recommended to implement your own `dblog.SourceResolver` based on the URI consumer provided, 

```golang
package main

import (
    "context"
    "net"
	
    "github.com/rueian/pgcapture"
    "google.golang.org/grpc"
)

type MySourceResolver struct {}

func (m *MySourceResolver) Source(ctx context.Context, uri string) (pgcapture.RequeueSource, error) {
    // decide where to fetch latest change based on uri
}

func (m *MySourceResolver) Dumper(ctx context.Context, uri string) (pgcapture.SourceDumper, error) {
    // decide where to fetch on-demand dumps based on uri
}

func main() {
    // connect to dump controller
    controlConn, _ := grpc.Dial("127.0.0.1:10001", grpc.WithInsecure())

    gateway := pgcapture.NewDBLogGateway(controlConn, &MySourceResolver{})

    ln, _ := net.Listen("tcp", "0.0.0.0:10000")
    gateway.Serve(context.Background(), ln)
}

```
