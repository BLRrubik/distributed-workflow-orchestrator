package orchestration

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
)

func TestWorkerRegistry_Register(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	err := r.Register(domain.WorkerNode{ID: "w1"})
	assert.NoError(t, err)

	err = r.Register(domain.WorkerNode{ID: "w1"})
	assert.Error(t, err)
}

func TestWorkerRegistry_Heartbeat(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	err := r.Heartbeat("unknown", 3)
	assert.Error(t, err)

	assert.NoError(t, r.Register(domain.WorkerNode{ID: "w1"}))

	assert.NoError(t, r.Heartbeat("w1", 5))

	workers := r.AliveWorkers()
	assert.Len(t, workers, 1)
	assert.Equal(t, 5, workers[0].RunningTasks)
}

func TestWorkerRegistry_AliveWorkers(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	assert.Empty(t, r.AliveWorkers())

	alive := domain.WorkerNode{ID: "alive"}

	suspect := domain.WorkerNode{ID: "suspect"}
	suspect.SetSuspect()

	dead := domain.WorkerNode{ID: "dead"}
	dead.SetDead()

	assert.NoError(t, r.Register(alive))
	assert.NoError(t, r.Register(suspect))
	assert.NoError(t, r.Register(dead))

	workers := r.AliveWorkers()
	assert.Len(t, workers, 1)
	assert.Equal(t, "alive", workers[0].ID)
}

func TestWorkerRegistry_IncrementRunning(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	// неизвестный воркер — молча ничего не делает, не паникует
	r.IncrementRunning("unknown")

	assert.NoError(t, r.Register(domain.WorkerNode{ID: "w1"}))

	r.IncrementRunning("w1")
	r.IncrementRunning("w1")

	workers := r.AliveWorkers()
	assert.Len(t, workers, 1)
	assert.Equal(t, 2, workers[0].RunningTasks)
}

func TestWorkerRegistry_ConcurrentAccess(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	assert.NoError(t, r.Register(domain.WorkerNode{ID: "w1"}))

	var wg sync.WaitGroup

	wg.Add(3)

	go func() {
		defer wg.Done()

		for range 100 {
			r.IncrementRunning("w1")
		}
	}()

	go func() {
		defer wg.Done()

		for range 100 {
			_ = r.Heartbeat("w1", 1)
		}
	}()

	go func() {
		defer wg.Done()

		for range 100 {
			r.AliveWorkers()
		}
	}()

	wg.Wait()
}
