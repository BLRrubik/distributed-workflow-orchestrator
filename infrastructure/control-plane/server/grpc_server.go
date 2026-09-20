package server

import (
	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/client"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/engine"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/orchestration"
	"google.golang.org/grpc"
)

type grpcServer struct {
	protogen.UnsafeClusterServiceServer
	protogen.UnsafeOrchestratorAPIServer

	engine         *engine.WorkflowEngine
	workerRegistry *orchestration.WorkerRegistry
	workerClient   *client.GRPCWorkerClient

	log *logger.Logger
}

func RegisterServer(
	server *grpc.Server,
	engine *engine.WorkflowEngine,
	log *logger.Logger,
	workerRegistry *orchestration.WorkerRegistry,
	workerClient *client.GRPCWorkerClient,
) {
	grpcSerever := &grpcServer{
		engine:         engine,
		workerRegistry: workerRegistry,
		workerClient:   workerClient,
		log:            log,
	}

	protogen.RegisterClusterServiceServer(server, grpcSerever)

	protogen.RegisterOrchestratorAPIServer(server, grpcSerever)
}
