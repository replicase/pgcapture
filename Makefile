PATH := ${CURDIR}/bin:$(PATH)
go_exe = $(shell go env GOEXE)
protoc_version = 3.19.1
protoc_arch = x86_64

ifeq ($(shell uname -s),Darwin)
	protoc_os = osx
else
	protoc_os = linux

	ifeq ($(shell uname -m),aarch64)
		protoc_arch = aarch_64
	endif
endif

.PHONY: build
build:
	go build \
		-ldflags="-X github.com/replicase/pgcapture.CommitSHA=${PGCAPTURE_SHA} -X github.com/replicase/pgcapture.Version=${PGCAPTURE_VERSION}" \
		-x -o bin/out/pgcapture ./cmd

.PHONY: test
test:
	go test -v -race -p 1 -coverprofile=./c.out ./...

.PHONY: codegen
codegen: bin/protoc-gen-go$(go_exe) bin/protoc-gen-go-grpc$(go_exe) bin/mockgen$(go_exe) proto generate

PHONY: proto
proto: bin/protoc
	protoc --go_out=pkg --go_opt=paths=source_relative --go-grpc_out=pkg --go-grpc_opt=paths=source_relative pb/*.proto
	python3 -m grpc_tools.protoc -I. --python_out=./python --grpc_python_out=./python pb/*.proto

.PHONY: generate
generate:
	go generate ./...

bin/mockgen$(go_exe): go.sum
	go build -o $@ github.com/golang/mock/mockgen

bin/protoc-gen-go$(go_exe): go.sum
	go build -o $@ google.golang.org/protobuf/cmd/protoc-gen-go

bin/protoc-gen-go-grpc$(go_exe): go.sum
	go build -o $@ google.golang.org/grpc/cmd/protoc-gen-go-grpc

bin/protoc-$(protoc_version).zip:
	mkdir -p $(dir $@)
	curl -o $@ --location https://github.com/protocolbuffers/protobuf/releases/download/v$(protoc_version)/protoc-$(protoc_version)-$(protoc_os)-$(protoc_arch).zip

bin/protoc-$(protoc_version): bin/protoc-$(protoc_version).zip
	mkdir -p $@
	unzip -d $@ -o $<

bin/protoc: bin/protoc-$(protoc_version)
	ln -s -f ./protoc-$(protoc_version)/bin/protoc $@
