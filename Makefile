.PHONY: proto
proto: pb/*.proto
	protoc --go_out=pkg --go_opt=paths=source_relative --go-grpc_out=pkg --go-grpc_opt=paths=source_relative $<