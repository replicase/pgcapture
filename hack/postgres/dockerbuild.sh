#!/bin/sh

export DOCKER_BUILDKIT=1

VERSION="${VERSION:-11}"
TAG="dcard/postgres:$VERSION-logical"

echo "build for $TAG"
docker buildx build --push --platform=linux/amd64 -t $TAG -f "Dockerfile.$VERSION" .
