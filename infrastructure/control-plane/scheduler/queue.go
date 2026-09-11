package scheduler

import "sync"

type ReadyQueue struct {
	mu    sync.Mutex
	items []Job
}

func NewReadyQueue() *ReadyQueue {
	return &ReadyQueue{
		items: make([]Job, 0),
	}
}

func (q *ReadyQueue) Push(job Job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.items = append(q.items, job)
}

func (q *ReadyQueue) PopBatch(n int) []Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	if n > len(q.items) {
		n = len(q.items)
	}

	batch := q.items[:n]
	q.items = q.items[n:]

	return batch
}
