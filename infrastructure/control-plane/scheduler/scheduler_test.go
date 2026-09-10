package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/orchestration"
)

func newTestScheduler(t *testing.T) (*Scheduler, *orchestration.WorkerRegistry) {
	t.Helper()

	registry := orchestration.NewWorkerRegistry(logger.New(logger.ERROR, false))
	s := New(registry, logger.New(logger.ERROR, false))

	return s, registry
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

	s.PushTask(&domain.Task{ID: "task-1"})

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

	s.PushTask(&domain.Task{ID: "task-1"})

	s.assignOnce(context.Background())

	// воркеров нет — задача должна вернуться в очередь, а не потеряться
	batch := s.queue.PopBatch(10)
	assert.Len(t, batch, 1)
	assert.Equal(t, "task-1", batch[0].ID)
}

func TestScheduler_AssignOnce_AssignsToWorker(t *testing.T) {
	s, registry := newTestScheduler(t)

	assert.NoError(t, registry.Register(domain.WorkerNode{ID: "w1", Capacity: 5}))

	s.PushTask(&domain.Task{ID: "task-1"})

	s.assignOnce(context.Background())

	// очередь опустела — задачу забрали
	assert.Empty(t, s.queue.PopBatch(10))

	// занятость воркера учтена локально, не дожидаясь heartbeat
	workers := registry.AliveWorkers()
	assert.Equal(t, 1, workers[0].RunningTasks)
}

func TestScheduler_AssignOnce_SkipsDeadWorkers(t *testing.T) {
	s, registry := newTestScheduler(t)

	dead := domain.WorkerNode{ID: "dead", Capacity: 5}
	dead.SetDead()
	assert.NoError(t, registry.Register(dead))

	s.PushTask(&domain.Task{ID: "task-1"})

	s.assignOnce(context.Background())

	// мёртвый воркер не в AliveWorkers — задача должна вернуться в очередь
	batch := s.queue.PopBatch(10)
	assert.Len(t, batch, 1)
}

func TestScheduler_Run_AssignsOnTick(t *testing.T) {
	s, registry := newTestScheduler(t)

	assert.NoError(t, registry.Register(domain.WorkerNode{ID: "w1", Capacity: 5}))

	s.PushTask(&domain.Task{ID: "task-1"})

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
