package client

import (
	"context"
	"sync"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type AddressResolver interface {
	AddressOf(workerID string) string
}

type GRPCWorkerClient struct {
	conns        map[string]*grpc.ClientConn
	mu           sync.Mutex
	addrResolver AddressResolver
}

func NewWorkerClient(addrResolver AddressResolver) *GRPCWorkerClient {
	return &GRPCWorkerClient{
		conns:        make(map[string]*grpc.ClientConn),
		addrResolver: addrResolver,
	}
}

func (c *GRPCWorkerClient) Dispatch(
	ctx context.Context,
	workerID string,
	in *protogen.DispatchRequest,
) (*protogen.DispatchResponse, error) {
	conn, err := c.getOrDial(workerID)
	if err != nil {
		return nil, err
	}

	client := protogen.NewWorkerServiceClient(conn)

	return client.Dispatch(ctx, in)
}

func (c *GRPCWorkerClient) getOrDial(workerID string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.conns[workerID]; ok {
		return conn, nil
	}

	addr := c.addrResolver.AddressOf(workerID)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	c.conns[workerID] = conn

	return conn, nil
}

// CloseConn вызывается из WorkerRegistry.markDead — закрыть и убрать протухшее соединение
func (c *GRPCWorkerClient) CloseConn(workerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.conns[workerID]; ok {
		conn.Close()

		delete(c.conns, workerID)
	}
}
