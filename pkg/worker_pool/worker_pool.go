package wp

import (
	"context"
	"sync"
	"time"
)

type Job[T any] struct {
	event      T
	handler    func(T) error
	waitToTime int64
}

type WorkerPoolOpt[T any] func(*WorkerPool[T])

func WithWorkerCount[T any](count int) WorkerPoolOpt[T] {
	return func(wp *WorkerPool[T]) {
		wp.workerCount = count
	}
}

func WithCapacity[T any](capacity int) WorkerPoolOpt[T] {
	return func(wp *WorkerPool[T]) {
		wp.capacity = capacity
	}
}

type WorkerPool[T any] struct {
	workerCount int
	capacity    int
	jobsQ       LinkedQueue[Job[T]]
	retryQ      PriorityQueue[Job[T]]
	jobChan     chan Job[T]
	wg          sync.WaitGroup
}

func NewWorkerPool[T any](opts ...WorkerPoolOpt[T]) *WorkerPool[T] {
	wp := &WorkerPool[T]{}

	for _, opt := range opts {
		opt(wp)
	}

	if wp.workerCount == 0 {
		wp.workerCount = 5
	}

	if wp.capacity == 0 {
		wp.capacity = 128
	}

	wp.jobChan = make(chan Job[T], wp.capacity)

	return wp
}

func (wp *WorkerPool[T]) Start(ctx context.Context) {
	for range wp.workerCount {
		wp.wg.Add(1)
		go wp.worker(ctx)
	}

	go wp.jobsLoop(ctx)
	go wp.retryLoop(ctx)
}

func (wp *WorkerPool[T]) Stop() {
	close(wp.jobChan)
	wp.wg.Wait()
}

func (wp *WorkerPool[T]) Submit(event T, handler func(T) error) {
	wp.jobChan <- Job[T]{event: event, handler: handler}
}

func (wp *WorkerPool[T]) TrySubmit(value T, handler func(T) error) bool {
	select {
	case wp.jobChan <- Job[T]{event: value, handler: handler}:
		return true
	default:
		return false
	}
}

func (wp *WorkerPool[T]) worker(ctx context.Context) {
	defer wp.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-wp.jobChan:
			if !ok {
				return
			}

			if err := job.handler(job.event); err != nil {
				wp.moveToRetry(job)
			}
		}
	}
}

func (wp *WorkerPool[T]) jobsLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			job, ok := wp.jobsQ.Dequeue()
			if !ok {
				time.Sleep(time.Millisecond * 200)

				continue
			}

			wp.jobChan <- job
		}
	}
}

func (wp *WorkerPool[T]) retryLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			for {
				item := wp.retryQ.Top()
				if item == nil {
					break
				}

				itemPQ, ok := item.(*ItemPQ[Job[T]])
				if !ok {
					break
				}

				if itemPQ.waitToTime > time.Now().Unix() {
					break
				}

				job, ok := wp.retryQ.Dequeue()
				if !ok {
					break
				}

				wp.jobsQ.Queue(job)
			}

			time.Sleep(time.Millisecond * 200)
		}
	}
}

func (wp *WorkerPool[T]) moveToRetry(job Job[T]) {
	wp.retryQ.Enqueue(&ItemPQ[Job[T]]{
		Value:      job,
		waitToTime: job.waitToTime,
	})
}
