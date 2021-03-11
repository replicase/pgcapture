package test

import (
	"net"

	"google.golang.org/grpc"
)

func NewGRPCServer(desc *grpc.ServiceDesc, impl interface{}) (net.Addr, func()) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()
	server.RegisterService(desc, impl)
	go server.Serve(lis)
	return lis.Addr(), func() {
		server.Stop()
		lis.Close()
	}
}
