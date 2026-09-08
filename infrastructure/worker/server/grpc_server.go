package server

import (
	"context"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"google.golang.org/grpc"
)

type GRPCServer struct {
	protogen.UnsafeWorkerServiceServer
}

func RegisterGRPCServer(s *grpc.Server) {
	protogen.RegisterWorkerServiceServer(s, &GRPCServer{})
}

func (G *GRPCServer) Dispatch(ctx context.Context, request *protogen.DispatchRequest) (*protogen.DispatchResponse, error) {
	return &protogen.DispatchResponse{
		Accepted: true,
	}, nil
}
