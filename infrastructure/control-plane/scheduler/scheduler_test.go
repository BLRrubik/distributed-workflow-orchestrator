package scheduler

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/client"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/orchestration"
)

func newTestScheduler(t *testing.T) (*Scheduler, *orchestration.WorkerRegistry) {
	t.Helper()

	log := logger.New(logger.ERROR, false)
	registry := orchestration.NewWorkerRegistry(log)
	workerClient := client.NewWorkerClient(registry)
	s := New(registry, workerClient, log)

	return s, registry
}

type fakeWorkerServer struct {
	protogen.UnimplementedWorkerServiceServer
}

func (fakeWorkerServer) Dispatch(context.Context, *protogen.DispatchRequest) (*protogen.DispatchResponse, error) {
	return &protogen.DispatchResponse{Accepted: true}, nil
}

// newFakeWorkerAddr поднимает настоящий TCP-листенер с fake WorkerService —
// GRPCWorkerClient дайлит по обычному host:port, без кастомного dialer'а под тест не залезть.
func newFakeWorkerAddr(t *testing.T) string {
	t.Helper()

	var listenConfig net.ListenConfig

	lis, err := listenConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	protogen.RegisterWorkerServiceServer(srv, fakeWorkerServer{})

	go func() { _ = srv.Serve(lis) }()

	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

func TestScheduler_SelectWorker_EmptyWorkers(t *testing.T) {
	s, _ := newTestScheduler(t)

	_, err := s.selectWorker(nil)
	assert.Error(t, err)
}

func TestScheduler_SelectWorker_FreshWorkerNoPanic(t *testing.T) {
	s, _ := newTestScheduler(t)

	// RunningTasks == 0 у обоих — раньше падало паникой на делении на ноль
	workers := []domain.WorkerNode{
		{ID: "w1", Capacity: 5},
		{ID: "w2", Capacity: 10},
	}

	id, err := s.selectWorker(workers)
	assert.NoError(t, err)
	assert.Equal(t, "w2", id) // больше свободной капасити
}

func TestScheduler_SelectWorker_PicksLeastLoaded(t *testing.T) {
	s, _ := newTestScheduler(t)

	workers := []domain.WorkerNode{
		{ID: "busy", Capacity: 10, RunningTasks: 9},
		{ID: "free", Capacity: 10, RunningTasks: 1},
	}

	id, err := s.selectWorker(workers)
	assert.NoError(t, err)
	assert.Equal(t, "free", id)
}

func TestScheduler_PushTask(t *testing.T) {
	s, _ := newTestScheduler(t)

	s.PushTask(Job{ID: "task-1"})

	batch := s.queue.PopBatch(10)
	assert.Len(t, batch, 1)
	assert.Equal(t, "task-1", batch[0].ID)
}

func TestScheduler_AssignOnce_NoReadyTasks(t *testing.T) {
	s, registry := newTestScheduler(t)

	assert.NoError(t, registry.Register(domain.WorkerNode{ID: "w1", Capacity: 5}))

	s.assignOnce(context.Background())

	workers := registry.AliveWorkers()
	assert.Equal(t, 0, workers[0].RunningTasks) // никого не назначили
}

func TestScheduler_AssignOnce_NoWorkers_RequeuesTask(t *testing.T) {
	s, _ := newTestScheduler(t)

	s.PushTask(Job{ID: "task-1"})

	s.assignOnce(context.Background())

	// воркеров нет — задача должна вернуться в очередь, а не потеряться
	batch := s.queue.PopBatch(10)
	assert.Len(t, batch, 1)
	assert.Equal(t, "task-1", batch[0].ID)
}

func TestScheduler_AssignOnce_AssignsToWorker(t *testing.T) {
	s, registry := newTestScheduler(t)

	assert.NoError(t, registry.Register(domain.WorkerNode{ID: "w1", Address: newFakeWorkerAddr(t), Capacity: 5}))

	var dispatchedTo string

	s.PushTask(Job{
		ID:      "task-1",
		Request: &protogen.DispatchRequest{TaskId: "task-1"},
		OnDispatched: func(_ context.Context, workerID string) {
			dispatchedTo = workerID
		},
	})

	s.assignOnce(context.Background())

	// очередь опустела — задачу забрали
	assert.Empty(t, s.queue.PopBatch(10))

	// занятость воркера учтена локально, не дожидаясь heartbeat
	workers := registry.AliveWorkers()
	assert.Equal(t, 1, workers[0].RunningTasks)

	// OnDispatched реально вызван после успешного Dispatch
	assert.Equal(t, "w1", dispatchedTo)
}

func TestScheduler_AssignOnce_SkipsDeadWorkers(t *testing.T) {
	s, registry := newTestScheduler(t)

	dead := domain.WorkerNode{ID: "dead", Capacity: 5}
	dead.SetDead()
	assert.NoError(t, registry.Register(dead))

	s.PushTask(Job{ID: "task-1"})

	s.assignOnce(context.Background())

	// мёртвый воркер не в AliveWorkers — задача должна вернуться в очередь
	batch := s.queue.PopBatch(10)
	assert.Len(t, batch, 1)
}

func TestScheduler_Run_AssignsOnTick(t *testing.T) {
	s, registry := newTestScheduler(t)

	assert.NoError(t, registry.Register(domain.WorkerNode{ID: "w1", Address: newFakeWorkerAddr(t), Capacity: 5}))

	s.PushTask(Job{
		ID:           "task-1",
		Request:      &protogen.DispatchRequest{TaskId: "task-1"},
		OnDispatched: func(context.Context, string) {},
	})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		s.Run(ctx)
		close(done)
	}()

	assert.Eventually(t, func() bool {
		return registry.AliveWorkers()[0].RunningTasks == 1
	}, 2*time.Second, 20*time.Millisecond)

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}
