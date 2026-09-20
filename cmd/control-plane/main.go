package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/client"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/config"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/engine"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/orchestration"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/scheduler"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/server"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	config.Print(cfg)

	log := logger.New(logger.INFO, true)

	ctx := context.Background()

	workerRegistry := orchestration.NewWorkerRegistry(log)

	go workerRegistry.Run(ctx) // dead man's switch — без него зависшие воркеры никогда не помечаются мёртвыми

	workerClient := client.NewWorkerClient(workerRegistry)

	taskScheduler := scheduler.New(workerRegistry, workerClient, log)
	go taskScheduler.Run(ctx)

	eng := engine.NewWorkflowEngine(log, taskScheduler)

	workerRegistry.OnWorkerDead(func(workerID string) {
		workerClient.CloseConn(workerID)
		eng.ReassignDeadWorkerTasks(workerID)
	})

	srv := grpc.NewServer()
	server.RegisterServer(srv, eng, log, workerRegistry)

	var listenConfig net.ListenConfig

	grpcListener, err := listenConfig.Listen(ctx, "tcp", cfg.GRPC.Port)
	if err != nil {
		panic(err)
	}

	go func() {
		if err := srv.Serve(grpcListener); err != nil {
			log.Error("grpc server stopped", logger.Error(err))
		}
	}()

	termChan := make(chan os.Signal, 1)

	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	<-termChan
}
