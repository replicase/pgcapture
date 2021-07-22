#!/bin/bash

SHA=$(git rev-parse --short HEAD)

echo "building rueian/pgcapture:$SHA"

docker build -t rueian/pgcapture:$SHA --build-arg SHA=$SHA .