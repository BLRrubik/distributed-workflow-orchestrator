package main

import (
	"net"

	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/server"
	"google.golang.org/grpc"
)

func main() {
	grpcServer := grpc.NewServer()
	server.RegisterGRPCServer(grpcServer)

	grpcListener, err := net.Listen("tcp", ":8081")
	if err != nil {
		panic(err)
	}

	if err = grpcServer.Serve(grpcListener); err != nil {
		panic(err)
	}
}
