package server

import (
	"context"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor/shell"
	"google.golang.org/grpc"
)

type GRPCServer struct {
	protogen.UnsafeWorkerServiceServer
	shellExecutor *shell.Executor

	log *logger.Logger
}

func RegisterGRPCServer(s *grpc.Server, log *logger.Logger) {
	protogen.RegisterWorkerServiceServer(s, &GRPCServer{
		shellExecutor: &shell.Executor{},
		log:           log,
	})
}

func (g *GRPCServer) Dispatch(ctx context.Context, request *protogen.DispatchRequest) (*protogen.DispatchResponse, error) {
	taskSpec := domain.TaskSpec{
		Type:    request.GetType(),
		Payload: request.GetPayload(),
	}

	_, err := g.shellExecutor.Execute(ctx, taskSpec, time.Duration(request.TimeoutSeconds)*time.Second)
	if err != nil {
		g.log.Error("error due to executing task",
			logger.String("task_id", request.GetTaskId()),
			logger.Any("task", taskSpec),
			logger.Error(err),
		)

		return &protogen.DispatchResponse{
			Accepted: false,
		}, nil
	}

	return &protogen.DispatchResponse{
		Accepted: true,
	}, nil
}
