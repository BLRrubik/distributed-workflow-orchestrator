package client

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
)

// GRPCClusterClient — клиент воркера к control-plane (ClusterService):.
type GRPCClusterClient struct {
	addr string

	mu   sync.Mutex
	conn *grpc.ClientConn
}

func NewClusterClient(addr string) *GRPCClusterClient {
	return &GRPCClusterClient{addr: addr}
}

func (c *GRPCClusterClient) Register(ctx context.Context, in *protogen.RegisterRequest) (*protogen.RegisterResponse, error) {
	conn, err := c.getOrDial()
	if err != nil {
		return nil, err
	}

	resp, err := protogen.NewClusterServiceClient(conn).Register(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("register: %w", err)
	}

	return resp, nil
}

func (c *GRPCClusterClient) Heartbeat(ctx context.Context, in *protogen.HeartbeatRequest) (*protogen.HeartbeatResponse, error) {
	conn, err := c.getOrDial()
	if err != nil {
		return nil, err
	}

	resp, err := protogen.NewClusterServiceClient(conn).Heartbeat(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("heartbeat: %w", err)
	}

	return resp, nil
}

func (c *GRPCClusterClient) ReportResult(ctx context.Context, in *protogen.ResultRequest) (*protogen.ResultResponse, error) {
	conn, err := c.getOrDial()
	if err != nil {
		return nil, err
	}

	resp, err := protogen.NewClusterServiceClient(conn).ReportResult(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("report result: %w", err)
	}

	return resp, nil
}

// Close закрывает соединение с control-plane, если оно было установлено.
func (c *GRPCClusterClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil
	}

	err := c.conn.Close()
	c.conn = nil

	if err != nil {
		return fmt.Errorf("close connection: %w", err)
	}

	return nil
}

func (c *GRPCClusterClient) getOrDial() (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return c.conn, nil
	}

	conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", c.addr, err)
	}

	c.conn = conn

	return c.conn, nil
}
