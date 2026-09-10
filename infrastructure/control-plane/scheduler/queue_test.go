package scheduler

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
)

func TestReadyQueue_PopBatch_Empty(t *testing.T) {
	q := NewReadyQueue()

	batch := q.PopBatch(10)
	assert.Empty(t, batch)
}

func TestReadyQueue_PushPop_FIFO(t *testing.T) {
	q := NewReadyQueue()

	q.Push(&domain.Task{ID: "task-1"})
	q.Push(&domain.Task{ID: "task-2"})
	q.Push(&domain.Task{ID: "task-3"})

	batch := q.PopBatch(10)

	assert.Len(t, batch, 3)
	assert.Equal(t, "task-1", batch[0].ID)
	assert.Equal(t, "task-2", batch[1].ID)
	assert.Equal(t, "task-3", batch[2].ID)
}

func TestReadyQueue_PopBatch_Partial(t *testing.T) {
	q := NewReadyQueue()

	q.Push(&domain.Task{ID: "task-1"})
	q.Push(&domain.Task{ID: "task-2"})
	q.Push(&domain.Task{ID: "task-3"})

	first := q.PopBatch(2)
	assert.Len(t, first, 2)
	assert.Equal(t, "task-1", first[0].ID)
	assert.Equal(t, "task-2", first[1].ID)

	second := q.PopBatch(2)
	assert.Len(t, second, 1)
	assert.Equal(t, "task-3", second[0].ID)

	assert.Empty(t, q.PopBatch(2))
}

func TestReadyQueue_PopBatch_MoreThanAvailable(t *testing.T) {
	q := NewReadyQueue()

	q.Push(&domain.Task{ID: "task-1"})

	batch := q.PopBatch(100)
	assert.Len(t, batch, 1)
}

func TestReadyQueue_PopBatch_Zero(t *testing.T) {
	q := NewReadyQueue()

	q.Push(&domain.Task{ID: "task-1"})

	batch := q.PopBatch(0)
	assert.Empty(t, batch)

	// задача никуда не делась
	assert.Len(t, q.PopBatch(10), 1)
}

func TestReadyQueue_RequeueAfterPop(t *testing.T) {
	q := NewReadyQueue()

	q.Push(&domain.Task{ID: "task-1"})

	batch := q.PopBatch(1)
	assert.Len(t, batch, 1)

	// как в Scheduler.assignOnce: не смогли назначить — вернули в очередь
	q.Push(batch[0])

	requeued := q.PopBatch(10)
	assert.Len(t, requeued, 1)
	assert.Equal(t, "task-1", requeued[0].ID)
}

func TestReadyQueue_ConcurrentAccess(t *testing.T) {
	q := NewReadyQueue()

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for range 200 {
			q.Push(&domain.Task{ID: "task"})
		}
	}()

	go func() {
		defer wg.Done()

		for range 200 {
			q.PopBatch(5)
		}
	}()

	wg.Wait()

	// добираем то, что осталось — не должно паниковать и не должно терять счёт
	drained := q.PopBatch(1000)
	assert.LessOrEqual(t, len(drained), 200)
}
