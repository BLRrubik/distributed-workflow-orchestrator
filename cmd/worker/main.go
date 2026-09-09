package main

import (
	"context"
	"net"

	"google.golang.org/grpc"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/config"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/server"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/service"
	wp "github.com/blrrubik/distributed-workflow-orchestrator/pkg/worker_pool"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	config.Print(cfg)

	log := logger.New(logger.INFO, true)
	ctx := context.Background()

	workerPoolOpts := []wp.WorkerPoolOpt{
		wp.WithWorkerCount(8),
		wp.WithCapacity(128),
	}
	workerPool := wp.NewWorkerPool(workerPoolOpts...)

	executors := executor.NewExecutors()

	workerService := service.NewWorkerService(ctx, workerPool, executors, log)

	grpcServer := grpc.NewServer()
	server.RegisterGRPCServer(grpcServer, log, workerService)

	var listenConfig net.ListenConfig

	grpcListener, err := listenConfig.Listen(ctx, "tcp", cfg.GRPC.Port)
	if err != nil {
		panic(err)
	}

	if err = grpcServer.Serve(grpcListener); err != nil {
		panic(err)
	}
}
