# pgcapture

A scalable Netflix DBLog implementation for PostgreSQL

![circleci](https://circleci.com/gh/rueian/pgcapture.svg?style=shield)
[![Maintainability](https://api.codeclimate.com/v1/badges/efd0f50a92233b34ae5e/maintainability)](https://codeclimate.com/github/rueian/pgcapture/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/efd0f50a92233b34ae5e/test_coverage)](https://codeclimate.com/github/rueian/pgcapture/test_coverage)

![overview](./hack/images/overview.png)

## Features
* DDL replication
* Replicate in Postgres Binary Representation
* gRPC API for consuming dumps and changes

## Difference to Netflix DBLog
* PostgreSQL Only
* Not inject dumps into the source CDC stream, but merge dumps in the later grpc gateway service.
  In this way the dump process can be scaled by adding more logical replicas and consumers.
  Most importantly, dumps process will not have impact to other downstream consumers that don't need to apply old dumps.  
* The replication requires a unique key on every table. However, the key isn't limited to be a single and incremental column,
  because dumps are performed by PostgreSQL TID Scan not on the unique key.