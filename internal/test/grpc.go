package test

import (
	"net"

	"google.golang.org/grpc"
)

func NewGRPCServer(desc *grpc.ServiceDesc, addr string, impl interface{}) (net.Addr, func()) {
	lis, err := net.Listen("tcp", addr)
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
