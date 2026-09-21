package server

import (
	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/engine"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/orchestration"
	"google.golang.org/grpc"
)

type grpcServer struct {
	protogen.UnsafeClusterServiceServer
	protogen.UnsafeOrchestratorAPIServer

	engine         *engine.WorkflowEngine
	workerRegistry *orchestration.WorkerRegistry

	log *logger.Logger
}

func RegisterServer(
	server *grpc.Server,
	engine *engine.WorkflowEngine,
	log *logger.Logger,
	workerRegistry *orchestration.WorkerRegistry,
) {
	grpcSerever := &grpcServer{
		engine:         engine,
		workerRegistry: workerRegistry,
		log:            log,
	}

	protogen.RegisterClusterServiceServer(server, grpcSerever)

	protogen.RegisterOrchestratorAPIServer(server, grpcSerever)
}
