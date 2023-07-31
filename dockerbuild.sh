#!/bin/bash

set -e

export LOCAL_GOPATH=$(echo "${GOPATH:-~/go}" | cut -d ':' -f 1) # take the first GOPATH to mount into docker compose
export PLATFORM=${PLATFORM:-linux/amd64}
export PG_VERSION=${PG_VERSION:-14}

[ ! -d "$LOCAL_GOPATH/pkg/mod/cache" ] && mkdir -p "$LOCAL_GOPATH/pkg/mod/cache"

case "$1" in
  test)
    docker-compose run --rm test-deps
    docker-compose run --rm test
    ;;
  clean)
    docker-compose down
    ;;
  *)
    echo "\"$1\" is an unknown command"
    ;;
esac
