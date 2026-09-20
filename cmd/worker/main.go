package main

import (
	"context"
	"net"

	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor"
	executrorregistry "github.com/blrrubik/distributed-workflow-orchestrator/pkg/executror_registry"
	"google.golang.org/grpc"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/client"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/config"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/server"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/service"
	wp "github.com/blrrubik/distributed-workflow-orchestrator/pkg/worker_pool"
)

const workerCapacity = 8

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	config.Print(cfg)

	log := logger.New(logger.INFO, true)
	ctx := context.Background()

	workerPoolOpts := []wp.WorkerPoolOpt{
		wp.WithWorkerCount(cfg.Worker.Pool),
		wp.WithCapacity(128),
	}
	workerPool := wp.NewWorkerPool(workerPoolOpts...)

	clusterClient := client.NewClusterClient(cfg.ControlPlane.Address)

	executionRegistry := executrorregistry.New()
	if err = executor.RegisterExecutors(executionRegistry, cfg.Worker.Capabilities); err != nil {
		panic(err)
	}

	node := &domain.WorkerNode{
		ID:           cfg.Worker.ID,
		Address:      cfg.Worker.Address,
		Capacity:     cfg.Worker.Capacity,
		Labels:       cfg.Worker.Labels,
		Capabilities: cfg.Worker.Capabilities,
	}

	workerService := service.NewWorkerService(node, workerPool, executionRegistry, clusterClient, log)

	// без успешной регистрации воркер бесполезен — control-plane о нём не знает,
	// слать ему heartbeat/задачи некому; поднимать сервис в таком состоянии нельзя
	if err = workerService.Start(ctx); err != nil {
		panic(err)
	}

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
