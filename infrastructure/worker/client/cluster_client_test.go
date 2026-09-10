package client

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
)

type fakeClusterServer struct {
	protogen.UnimplementedClusterServiceServer

	mu             sync.Mutex
	registerCalls  int
	heartbeatCalls int
	resultCalls    int
	lastWorkerID   string
}

func (f *fakeClusterServer) Register(_ context.Context, req *protogen.RegisterRequest) (*protogen.RegisterResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.registerCalls++
	f.lastWorkerID = req.GetWorkerId()

	return &protogen.RegisterResponse{Accepted: true}, nil
}

func (f *fakeClusterServer) Heartbeat(_ context.Context, req *protogen.HeartbeatRequest) (*protogen.HeartbeatResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.heartbeatCalls++
	f.lastWorkerID = req.GetWorkerId()

	return &protogen.HeartbeatResponse{Acknowledged: true}, nil
}

func (f *fakeClusterServer) ReportResult(_ context.Context, req *protogen.ResultRequest) (*protogen.ResultResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.resultCalls++
	f.lastWorkerID = req.GetWorkerId()

	return &protogen.ResultResponse{}, nil
}

func (f *fakeClusterServer) calls(t *testing.T) (register, heartbeat, result int) {
	t.Helper()

	f.mu.Lock()
	defer f.mu.Unlock()

	return f.registerCalls, f.heartbeatCalls, f.resultCalls
}

// newTestClusterClient поднимает fake ClusterService на bufconn и возвращает
// GRPCClusterClient, у которого getOrDial идёт через bufconn-диалер.
func newTestClusterClient(t *testing.T) (*GRPCClusterClient, *fakeClusterServer) {
	t.Helper()

	const bufSize = 1024 * 1024

	lis := bufconn.Listen(bufSize)

	srv := grpc.NewServer()
	fake := &fakeClusterServer{}
	protogen.RegisterClusterServiceServer(srv, fake)

	go func() { _ = srv.Serve(lis) }()

	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = conn.Close() })

	// подсовываем уже установленное bufconn-соединение напрямую, минуя getOrDial/grpc.NewClient(addr)
	return &GRPCClusterClient{conn: conn}, fake
}

func TestGRPCClusterClient_Register(t *testing.T) {
	c, fake := newTestClusterClient(t)

	resp, err := c.Register(context.Background(), &protogen.RegisterRequest{WorkerId: "w1"})
	require.NoError(t, err)
	assert.True(t, resp.GetAccepted())

	register, _, _ := fake.calls(t)
	assert.Equal(t, 1, register)
	assert.Equal(t, "w1", fake.lastWorkerID)
}

func TestGRPCClusterClient_Heartbeat(t *testing.T) {
	c, fake := newTestClusterClient(t)

	resp, err := c.Heartbeat(context.Background(), &protogen.HeartbeatRequest{WorkerId: "w1", RunningTasks: 3})
	require.NoError(t, err)
	assert.True(t, resp.GetAcknowledged())

	_, heartbeat, _ := fake.calls(t)
	assert.Equal(t, 1, heartbeat)
}

func TestGRPCClusterClient_ReportResult(t *testing.T) {
	c, fake := newTestClusterClient(t)

	_, err := c.ReportResult(context.Background(), &protogen.ResultRequest{WorkerId: "w1", TaskId: "task-1"})
	require.NoError(t, err)

	_, _, result := fake.calls(t)
	assert.Equal(t, 1, result)
}

func TestGRPCClusterClient_Close_Idempotent(t *testing.T) {
	c := NewClusterClient("127.0.0.1:0")

	assert.NoError(t, c.Close()) // ни разу не дозванивались — Close не должен падать
	assert.NoError(t, c.Close()) // повторный Close тоже безопасен
}

func TestGRPCClusterClient_GetOrDial_ReusesConnection(t *testing.T) {
	c := NewClusterClient("127.0.0.1:0")

	conn1, err := c.getOrDial()
	require.NoError(t, err)

	conn2, err := c.getOrDial()
	require.NoError(t, err)

	assert.Same(t, conn1, conn2)

	assert.NoError(t, c.Close())
}

func TestGRPCClusterClient_GetOrDial_RedialsAfterShutdown(t *testing.T) {
	c := NewClusterClient("127.0.0.1:0")

	conn1, err := c.getOrDial()
	require.NoError(t, err)

	require.NoError(t, c.Close()) // conn1 переходит в Shutdown

	conn2, err := c.getOrDial()
	require.NoError(t, err)

	assert.NotSame(t, conn1, conn2, "getOrDial должен передайлить мёртвый (Shutdown) conn")

	assert.NoError(t, c.Close())
}
