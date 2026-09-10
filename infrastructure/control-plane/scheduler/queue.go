package scheduler

import (
	"sync"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
)

type ReadyQueue struct {
	mu    sync.Mutex
	items []*domain.Task // Task.ID, в порядке появления READY-статуса
}

func NewReadyQueue() *ReadyQueue {
	return &ReadyQueue{
		items: make([]*domain.Task, 0),
	}
}

func (q *ReadyQueue) Push(task *domain.Task) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.items = append(q.items, task)
}

func (q *ReadyQueue) PopBatch(n int) []*domain.Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	if n > len(q.items) {
		n = len(q.items)
	}

	batch := q.items[:n]
	q.items = q.items[n:]

	return batch
}
