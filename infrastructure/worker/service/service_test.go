package service

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/client"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor"
	wp "github.com/blrrubik/distributed-workflow-orchestrator/pkg/worker_pool"
)

type fakeClusterServer struct {
	protogen.UnimplementedClusterServiceServer
}

func (fakeClusterServer) Register(context.Context, *protogen.RegisterRequest) (*protogen.RegisterResponse, error) {
	return &protogen.RegisterResponse{Accepted: true}, nil
}

func (fakeClusterServer) Heartbeat(context.Context, *protogen.HeartbeatRequest) (*protogen.HeartbeatResponse, error) {
	return &protogen.HeartbeatResponse{Acknowledged: true}, nil
}

func (fakeClusterServer) ReportResult(context.Context, *protogen.ResultRequest) (*protogen.ResultResponse, error) {
	return &protogen.ResultResponse{Acknowledged: true}, nil
}

// newFakeClusterClient поднимает настоящий TCP-листенер с fake ClusterService —
// WorkerService.Start больше не стартует пул без успешного Register.
func newFakeClusterClient(t *testing.T) *client.GRPCClusterClient {
	t.Helper()

	var listenConfig net.ListenConfig

	lis, err := listenConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	protogen.RegisterClusterServiceServer(srv, fakeClusterServer{})

	go func() { _ = srv.Serve(lis) }()

	t.Cleanup(srv.Stop)

	return client.NewClusterClient(lis.Addr().String())
}

func newTestService(t *testing.T, opts ...wp.WorkerPoolOpt) *WorkerService {
	t.Helper()

	// НЕ t.Context(): он отменяется прямо перед вызовом Cleanup-функций (по
	// документации testing), а Stop() ниже сам вызывается из Cleanup и должен
	// успеть догрести очередь, пока ctx ещё жив — иначе ShellTask.Do получает
	// уже отменённый ctx и Stop() виснет, ожидая опустошения очереди.
	ctx, cancel := context.WithCancel(context.Background())

	pool := wp.NewWorkerPool(opts...)
	ws := NewWorkerService(pool, executor.NewExecutors(), newFakeClusterClient(t), logger.New(logger.ERROR, false))

	require.NoError(t, ws.Start(ctx, &domain.WorkerNode{ID: "test-worker"}))

	t.Cleanup(func() {
		pool.Stop()
		cancel()
	})

	return ws
}

func TestWorkerService_DispatchTask_ExecutesShellTask(t *testing.T) {
	ws := newTestService(t)

	req := &protogen.DispatchRequest{
		TaskId:         "task-1",
		Type:           "shell",
		TimeoutSeconds: 5,
		Payload:        map[string]string{"command": "echo"},
	}

	err := ws.DispatchTask(context.Background(), req)
	assert.NoError(t, err)
}

func TestWorkerService_DispatchTask_UnknownExecutorType(t *testing.T) {
	ws := newTestService(t)

	req := &protogen.DispatchRequest{
		TaskId: "task-2",
		Type:   "unknown",
	}

	err := ws.DispatchTask(context.Background(), req)
	assert.Error(t, err)
}

func TestWorkerService_Start_FailsWhenRegistrationRejected(t *testing.T) {
	pool := wp.NewWorkerPool()
	ws := NewWorkerService(pool, executor.NewExecutors(), client.NewClusterClient("127.0.0.1:0"), logger.New(logger.ERROR, false))

	err := ws.Start(context.Background(), &domain.WorkerNode{ID: "w1"})
	assert.Error(t, err, "недостижимый control-plane — Start обязан вернуть ошибку, а не тихо поднять пул")
}

func TestWorkerService_DispatchTask_SurvivesRequestContextCancel(t *testing.T) {
	var buf bytes.Buffer

	log := &logger.Logger{Logger: slog.New(slog.NewJSONHandler(&buf, nil))}

	poolCtx := t.Context()

	pool := wp.NewWorkerPool(wp.WithWorkerCount(1), wp.WithCapacity(2))
	pool.Start(poolCtx)

	// проверяем только выживание задачи при отмене request ctx — регистрация
	// тут не при чём, поэтому WorkerService собираем напрямую, минуя Start
	ws := &WorkerService{
		workerPool:    pool,
		executors:     executor.NewExecutors(),
		clusterClient: client.NewClusterClient("127.0.0.1:0"),
		log:           log,
		inFlight:      make(map[string]struct{}),
	}

	// занимает единственного воркера, чтобы target исполнился уже после отмены reqCtx
	blocker := &protogen.DispatchRequest{
		TaskId: "blocker", Type: "shell", TimeoutSeconds: 5,
		Payload: map[string]string{"command": "sleep", "args": "0.3"},
	}
	target := &protogen.DispatchRequest{
		TaskId: "target", Type: "shell", TimeoutSeconds: 5,
		Payload: map[string]string{"command": "echo"},
	}

	reqCtx, cancelReq := context.WithCancel(context.Background())

	assert.NoError(t, ws.DispatchTask(context.Background(), blocker))
	assert.NoError(t, ws.DispatchTask(reqCtx, target))

	// request ctx умирает сразу, как это бывает после завершения gRPC-запроса —
	// target к этому моменту ещё сидит в очереди и исполнится позже
	cancelReq()

	// Stop() ждёт завершения воркеров (wg.Wait) — это и есть точка синхронизации
	// с записью в buf, без неё чтение buf ниже было бы гонкой
	pool.Stop()

	assert.Contains(t, buf.String(), `"task_id":"target"`)
	assert.NotContains(t, buf.String(), "task execution failed")
}

func TestWorkerService_DispatchTask_DuplicateInFlight_Ignored(t *testing.T) {
	ws := newTestService(t, wp.WithWorkerCount(1), wp.WithCapacity(2))

	req := &protogen.DispatchRequest{
		WorkflowId: "wf-1", TaskId: "task-1", Type: "shell", TimeoutSeconds: 5,
		Payload: map[string]string{"command": "sleep", "args": "0.3"},
	}

	assert.NoError(t, ws.DispatchTask(context.Background(), req))
	time.Sleep(20 * time.Millisecond) // даём воркеру забрать job из канала

	// дубликат той же пары (workflowID, taskID), пока первая ещё выполняется —
	// должен быть тихо проигнорирован, а не считаться ошибкой
	assert.NoError(t, ws.DispatchTask(context.Background(), req))

	// в пуле реально только одна задача, а не две
	assert.EqualValues(t, 1, ws.workerPool.BusyCount())

	time.Sleep(400 * time.Millisecond)
}

func TestWorkerService_DispatchTask_DifferentWorkflow_NotDeduped(t *testing.T) {
	ws := newTestService(t, wp.WithWorkerCount(2), wp.WithCapacity(2))

	slowTask := func(workflowID string) *protogen.DispatchRequest {
		return &protogen.DispatchRequest{
			WorkflowId: workflowID, TaskId: "build", Type: "shell", TimeoutSeconds: 5,
			Payload: map[string]string{"command": "sleep", "args": "0.2"},
		}
	}

	// одинаковый TaskId, разные WorkflowId — ключ дедупа включает оба, коллизии быть не должно
	assert.NoError(t, ws.DispatchTask(context.Background(), slowTask("wf-1")))
	assert.NoError(t, ws.DispatchTask(context.Background(), slowTask("wf-2")))

	time.Sleep(20 * time.Millisecond)
	assert.EqualValues(t, 2, ws.workerPool.BusyCount())

	time.Sleep(300 * time.Millisecond)
}

func TestWorkerService_DispatchTask_AfterSuccess_AcceptsRedispatch(t *testing.T) {
	ws := newTestService(t)

	req := &protogen.DispatchRequest{
		WorkflowId: "wf-1", TaskId: "task-1", Type: "shell", TimeoutSeconds: 5,
		Payload: map[string]string{"command": "echo"},
	}

	assert.NoError(t, ws.DispatchTask(context.Background(), req))

	// ждём, пока задача реально доисполнится успехом и ключ снимется
	assert.Eventually(t, func() bool {
		ws.mu.Lock()
		defer ws.mu.Unlock()

		_, stillInFlight := ws.inFlight[inFlightKey("wf-1", "task-1")]

		return !stillInFlight
	}, time.Second, 10*time.Millisecond)

	// повторный диспатч той же пары уже после успеха — не дубликат, обычная задача
	assert.NoError(t, ws.DispatchTask(context.Background(), req))
}

func TestWorkerService_DispatchTask_PoolFull(t *testing.T) {
	ws := newTestService(t, wp.WithWorkerCount(1), wp.WithCapacity(1))

	slowTask := func(id string) *protogen.DispatchRequest {
		return &protogen.DispatchRequest{
			TaskId: id, Type: "shell", TimeoutSeconds: 5,
			Payload: map[string]string{"command": "sleep", "args": "0.3"},
		}
	}

	// первая занимает единственного воркера
	assert.NoError(t, ws.DispatchTask(context.Background(), slowTask("task-1")))
	time.Sleep(20 * time.Millisecond) // даём воркеру время забрать job из канала

	// вторая занимает единственное место в очереди jobChan
	assert.NoError(t, ws.DispatchTask(context.Background(), slowTask("task-2")))

	// третьей уже некуда деться
	err := ws.DispatchTask(context.Background(), slowTask("task-3"))
	assert.Error(t, err)

	time.Sleep(400 * time.Millisecond)
}
