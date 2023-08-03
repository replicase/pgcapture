#!/bin/bash

set -e

export LOCAL_GOPATH=$(echo "${GOPATH:-~/go}" | cut -d ':' -f 1) # take the first GOPATH to mount into docker compose
export PLATFORM=${PLATFORM:-linux/amd64}
export PG_VERSION=${PG_VERSION:-14}
export PGCAPTURE_SHA=$(git rev-parse --short HEAD)
export PGCAPTURE_VERSION=$(git describe --tags --abbrev=0)

[ ! -d "$LOCAL_GOPATH/pkg/mod/cache" ] && mkdir -p "$LOCAL_GOPATH/pkg/mod/cache"

case "$1" in
  build)
    docker-compose run --rm build
    docker-compose build --force-rm pgcapture
    ;;
  test)
    docker-compose run --rm test-deps
    docker-compose run --rm test
    ;;
  codegen)
    docker-compose run --rm codegen
    ;;
  clean)
    docker-compose down
    ;;
  *)
    echo "\"$1\" is an unknown command"
    ;;
esac
