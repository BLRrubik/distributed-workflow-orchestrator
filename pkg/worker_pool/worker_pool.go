package wp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Task — единица работы, которую исполняет пул.
type Task interface {
	Do(ctx context.Context) error
	GetWaitDuration() time.Duration
}

type Job struct {
	task       Task
	waitToTime int64
}

type WorkerPoolOpt func(*WorkerPool)

func WithWorkerCount(count int) WorkerPoolOpt {
	return func(wp *WorkerPool) {
		wp.workerCount = count
	}
}

func WithCapacity(capacity int) WorkerPoolOpt {
	return func(wp *WorkerPool) {
		wp.capacity = capacity
	}
}

type WorkerPool struct {
	workerCount int
	capacity    int
	jobsQ       LinkedQueue
	retryQ      PriorityQueue
	jobChan     chan Job
	stopChan    chan struct{}
	busyCount   atomic.Int32
	wg          sync.WaitGroup
}

func NewWorkerPool(opts ...WorkerPoolOpt) *WorkerPool {
	wp := &WorkerPool{}

	for _, opt := range opts {
		opt(wp)
	}

	if wp.workerCount == 0 {
		wp.workerCount = 5
	}

	if wp.capacity == 0 {
		wp.capacity = 128
	}

	wp.jobChan = make(chan Job, wp.capacity)
	wp.stopChan = make(chan struct{})

	return wp
}

func (wp *WorkerPool) Start(ctx context.Context) {
	for range wp.workerCount {
		wp.wg.Add(1)
		go wp.worker(ctx)
	}

	go wp.jobsLoop(ctx)
	go wp.retryLoop(ctx)
}

// Stop блокируется, пока jobsQ и retryQ не опустеют и все воркеры не станут
// свободны — иначе задачи, ждущие ретрая, потерялись бы при остановке.
func (wp *WorkerPool) Stop() {
	for wp.jobsQ.Size() != 0 || wp.retryQ.Size() != 0 || len(wp.jobChan) != 0 || wp.busyCount.Load() != 0 {
		time.Sleep(50 * time.Millisecond)
	}

	close(wp.stopChan)
	close(wp.jobChan)
	wp.wg.Wait()
}

func (wp *WorkerPool) Submit(task Task) {
	wp.jobChan <- Job{task: task}
}

func (wp *WorkerPool) TrySubmit(task Task) bool {
	select {
	case wp.jobChan <- Job{task: task}:
		return true
	default:
		return false
	}
}

// BusyCount возвращает число задач, исполняемых прямо сейчас — для heartbeat.
func (wp *WorkerPool) BusyCount() int32 {
	return wp.busyCount.Load()
}

func (wp *WorkerPool) worker(ctx context.Context) {
	defer wp.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-wp.jobChan:
			if !ok {
				return
			}

			wp.busyCount.Add(1)

			if err := job.task.Do(ctx); err != nil {
				wp.moveToRetry(job)
			}

			wp.busyCount.Add(-1)
		}
	}
}

func (wp *WorkerPool) jobsLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-wp.stopChan:
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

func (wp *WorkerPool) retryLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-wp.stopChan:
			return
		default:
			for {
				item := wp.retryQ.Top()
				if item == nil {
					break
				}

				itemPQ, ok := item.(*ItemPQ)
				if !ok {
					break
				}

				if itemPQ.waitToTime > time.Now().UnixNano() {
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

func (wp *WorkerPool) moveToRetry(job Job) {
	job.waitToTime = time.Now().Add(job.task.GetWaitDuration()).UnixNano()

	wp.retryQ.Enqueue(&ItemPQ{
		Value:      job,
		waitToTime: job.waitToTime,
	})
}
