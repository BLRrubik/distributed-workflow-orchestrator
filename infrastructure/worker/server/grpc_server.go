package server

import (
	"context"

	"google.golang.org/grpc"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/service"
)

type GRPCServer struct {
	protogen.UnsafeWorkerServiceServer

	service *service.WorkerService

	log *logger.Logger
}

func RegisterGRPCServer(s *grpc.Server, log *logger.Logger, service *service.WorkerService) {
	protogen.RegisterWorkerServiceServer(s, &GRPCServer{
		service: service,
		log:     log,
	})
}

func (g *GRPCServer) Dispatch(ctx context.Context, request *protogen.DispatchRequest) (*protogen.DispatchResponse, error) {
	if err := g.service.DispatchTask(ctx, request); err != nil {
		g.log.Error("dispatch rejected",
			logger.String("task_id", request.GetTaskId()),
			logger.Error(err),
		)

		// ошибка диспатча — не ошибка RPC, а бизнес-ответ "не принято"
		return &protogen.DispatchResponse{
			Accepted: false,
		}, nil
	}

	return &protogen.DispatchResponse{
		Accepted: true,
	}, nil
}
