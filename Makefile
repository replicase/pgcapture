.PHONY: proto
proto: pb/*.proto
	protoc --go_out=pkg --go_opt=paths=source_relative $<