package client

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
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

	resp, err := client.Dispatch(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("dispatch to worker %s: %w", workerID, err)
	}

	return resp, nil
}

func (c *GRPCWorkerClient) CancelTasks(
	ctx context.Context,
	workerID string,
	taskIDs []string,
) (*protogen.CancelTasksResponse, error) {
	conn, err := c.getOrDial(workerID)
	if err != nil {
		return nil, err
	}

	client := protogen.NewWorkerServiceClient(conn)

	resp, err := client.CancelTasks(ctx, &protogen.CancelTasksRequest{TaskIds: taskIDs})
	if err != nil {
		return nil, fmt.Errorf("cancel tasks on worker %s: %w", workerID, err)
	}

	return resp, nil
}

// CloseConn вызывается из WorkerRegistry.markDead — закрыть и убрать протухшее соединение.
func (c *GRPCWorkerClient) CloseConn(workerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.conns[workerID]; ok {
		_ = conn.Close() // Best effort: discard the stale connection regardless of close result.

		delete(c.conns, workerID)
	}
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
		return nil, fmt.Errorf("dial worker %s: %w", workerID, err)
	}

	c.conns[workerID] = conn

	return conn, nil
}
