#!/bin/bash

SHA=$(git rev-parse --short HEAD)
VERSION=$(git describe --tags)
PLATFORM=${1:-linux/amd64}

echo "building rueian/pgcapture:$VERSION in $PLATFORM platform"

docker buildx build --platform $PLATFORM --build-arg SHA=$SHA  --build-arg VERSION=$VERSION -t rueian/pgcapture:latest -t rueian/pgcapture:$VERSION .
