package server

import (
	"google.golang.org/grpc"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/engine"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/orchestration"
	"github.com/blrrubik/distributed-workflow-orchestrator/pkg/event_bus"
)

type grpcServer struct {
	protogen.UnsafeClusterServiceServer
	protogen.UnsafeOrchestratorAPIServer

	engine         *engine.WorkflowEngine
	workerRegistry *orchestration.WorkerRegistry
	eventBus       *eventbus.Bus

	log *logger.Logger
}

func RegisterServer(
	server *grpc.Server,
	engine *engine.WorkflowEngine,
	log *logger.Logger,
	workerRegistry *orchestration.WorkerRegistry,
	eventBus *eventbus.Bus,
) {
	grpcSerever := &grpcServer{
		engine:         engine,
		workerRegistry: workerRegistry,
		log:            log,
		eventBus:       eventBus,
	}

	protogen.RegisterClusterServiceServer(server, grpcSerever)

	protogen.RegisterOrchestratorAPIServer(server, grpcSerever)
}
