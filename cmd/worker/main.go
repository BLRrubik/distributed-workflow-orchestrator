package main

import (
	"net"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/config"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/server"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	config.Print(cfg)

	log := logger.New(logger.INFO, true)

	grpcServer := grpc.NewServer()
	server.RegisterGRPCServer(grpcServer, log)

	grpcListener, err := net.Listen("tcp", cfg.GRPC.Port)
	if err != nil {
		panic(err)
	}

	if err = grpcServer.Serve(grpcListener); err != nil {
		panic(err)
	}
}
