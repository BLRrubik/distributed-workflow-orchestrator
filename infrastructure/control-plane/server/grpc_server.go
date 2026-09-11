package server

import (
	"context"
	"fmt"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/engine"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/orchestration"
	"google.golang.org/grpc"
)

type grpcServer struct {
	protogen.UnsafeClusterServiceServer

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
	protogen.RegisterClusterServiceServer(server, &grpcServer{
		engine:         engine,
		workerRegistry: workerRegistry,
		log:            log,
	})
}

func (g *grpcServer) Register(ctx context.Context, request *protogen.RegisterRequest) (*protogen.RegisterResponse, error) {
	wNode := domain.WorkerNode{
		ID:       request.GetWorkerId(),
		Address:  request.GetAddress(),
		Labels:   request.GetLabels(),
		Capacity: int(request.GetCapacity()),
	}

	if err := g.workerRegistry.Register(wNode); err != nil {
		return &protogen.RegisterResponse{
			Accepted: false,
			Reason:   err.Error(),
		}, fmt.Errorf("failed to register worker: %w", err)
	}

	return &protogen.RegisterResponse{
		Accepted: true,
	}, nil
}

func (g *grpcServer) Heartbeat(ctx context.Context, request *protogen.HeartbeatRequest) (*protogen.HeartbeatResponse, error) {
	if err := g.workerRegistry.Heartbeat(request.GetWorkerId(), int(request.GetRunningTasks())); err != nil {
		return &protogen.HeartbeatResponse{
			Acknowledged: false,
		}, fmt.Errorf("failed to heartbeat worker: %w", err)
	}

	return &protogen.HeartbeatResponse{
		Acknowledged: true,
	}, nil
}

func (g *grpcServer) ReportResult(ctx context.Context, request *protogen.ResultRequest) (*protogen.ResultResponse, error) {
	resp := &protogen.ResultResponse{
		Acknowledged: true,
	}

	result := domain.TaskResult{
		Status:   domain.FromProtoTaskStatus(request.GetStatus()),
		Error:    request.GetError(),
		ExitCode: int(request.GetExitCode()),
		Stdout:   request.GetStdout(),
		Stderr:   request.GetStderr(),
		Duration: time.Duration(request.GetDuration()),
	}

	if err := g.engine.OnTaskResponse(ctx, request.GetWorkflowId(), request.GetTaskId(), result); err != nil {
		g.log.Error("failed to report result",
			logger.String("worker", request.GetWorkerId()),
			logger.Any("result", result),
			logger.String("workflow", request.GetWorkflowId()),
			logger.String("task", request.GetTaskId()),
		)

		return resp, nil
	}

	return resp, nil
}
