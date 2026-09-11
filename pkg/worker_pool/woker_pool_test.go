package wp

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// funcTask — Task на замыкании, для тестов. GetWaitDuration маленький по
// умолчанию, чтобы ретраи в тестах не тормозили.
type funcTask struct {
	do   func(context.Context) error
	wait time.Duration
}

func (f *funcTask) Do(ctx context.Context) error {
	return f.do(ctx)
}

func (f *funcTask) GetWaitDuration() time.Duration {
	if f.wait == 0 {
		return 5 * time.Millisecond
	}

	return f.wait
}

func TestWorkerPool_ExecutesJobs(t *testing.T) {
	wp := NewWorkerPool()

	wp.Start(t.Context())
	defer wp.Stop()

	var mu sync.Mutex

	results := make([]int, 0)

	var wg sync.WaitGroup

	wg.Add(5)

	for i := range 5 {
		v := i

		wp.Submit(&funcTask{do: func(context.Context) error {
			defer wg.Done()

			mu.Lock()

			results = append(results, v)
			mu.Unlock()

			return nil
		}})
	}

	waitOrFail(t, &wg)

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
}

func TestWorkerPool_ParallelExecution(t *testing.T) {
	wp := NewWorkerPool(WithWorkerCount(4), WithCapacity(10))

	wp.Start(t.Context())
	defer wp.Stop()

	start := time.Now()

	var wg sync.WaitGroup

	wg.Add(4)

	for range 4 {
		wp.Submit(&funcTask{do: func(context.Context) error {
			defer wg.Done()

			time.Sleep(100 * time.Millisecond)

			return nil
		}})
	}

	waitOrFail(t, &wg)

	elapsed := time.Since(start)

	if elapsed > 200*time.Millisecond {
		t.Fatalf("jobs did not run in parallel, took %v", elapsed)
	}
}

func TestWorkerPool_StopWaitsForJobs(t *testing.T) {
	wp := NewWorkerPool()
	wp.Start(t.Context())

	var wg sync.WaitGroup

	wg.Add(1)

	wp.Submit(&funcTask{do: func(context.Context) error {
		defer wg.Done()

		time.Sleep(50 * time.Millisecond)

		return nil
	}})

	wp.Stop()

	select {
	case <-time.After(10 * time.Millisecond):
	default:
	}

	if waitTimeout(&wg, time.Millisecond) {
		t.Fatal("Stop() did not wait for job completion")
	}
}

func TestWorkerPool_TrySubmit(t *testing.T) {
	wp := NewWorkerPool(WithWorkerCount(1), WithCapacity(1))
	wp.Start(t.Context())

	blocker := make(chan struct{})
	started := make(chan struct{})

	// занимает единственного воркера — дожидаемся, чтобы буфер гарантированно опустел
	ok := wp.TrySubmit(&funcTask{do: func(context.Context) error {
		close(started)
		<-blocker

		return nil
	}})
	if !ok {
		t.Fatal("expected first TrySubmit to succeed")
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker never picked up the first job")
	}

	noop := &funcTask{do: func(context.Context) error { return nil }}

	// заполняет единственное свободное место в буфере
	ok = wp.TrySubmit(noop)
	if !ok {
		t.Fatal("expected second TrySubmit to fill the buffer")
	}

	// и воркер занят, и буфер полон — третьей уже некуда деться
	ok = wp.TrySubmit(noop)
	if ok {
		t.Fatal("expected TrySubmit to fail when queue full")
	}

	close(blocker)
	wp.Stop()
}

func TestWorkerPool_Stress10kJobs(t *testing.T) {
	const (
		workers = 8
		jobs    = 10_000
	)

	wp := NewWorkerPool(WithWorkerCount(workers), WithCapacity(jobs))

	wp.Start(t.Context())
	defer wp.Stop()

	var counter atomic.Int64

	var wg sync.WaitGroup

	wg.Add(jobs)

	start := time.Now()

	for range jobs {
		wp.Submit(&funcTask{do: func(context.Context) error {
			counter.Add(1)
			wg.Done()

			return nil
		}})
	}

	waitOrFail(t, &wg)

	elapsed := time.Since(start)

	if counter.Load() != jobs {
		t.Fatalf("expected %d jobs executed, got %d",
			jobs, counter.Load(),
		)
	}

	t.Logf(
		"Processed %d jobs with %d workers in %v",
		jobs, workers, elapsed,
	)
}

func TestWorkerPool_Stress10kJobs_SmallQueue(t *testing.T) {
	const (
		workers = 8
		jobs    = 10_000
		queue   = 64
	)

	wp := NewWorkerPool(WithWorkerCount(workers), WithCapacity(queue))

	wp.Start(t.Context())
	defer wp.Stop()

	var counter atomic.Int64

	var wg sync.WaitGroup

	wg.Add(jobs)

	start := time.Now()

	for range jobs {
		wp.Submit(&funcTask{do: func(context.Context) error {
			counter.Add(1)
			wg.Done()

			return nil
		}})
	}

	waitOrFail(t, &wg)

	elapsed := time.Since(start)

	if counter.Load() != jobs {
		t.Fatalf("expected %d jobs executed, got %d",
			jobs, counter.Load(),
		)
	}

	t.Logf(
		"Processed %d jobs (queue=%d) in %v",
		jobs, queue, elapsed,
	)
}

func TestWorkerPool_Stress10kJobs_RandomLatency(t *testing.T) {
	const jobs = 10_000

	wp := NewWorkerPool(WithWorkerCount(8), WithCapacity(128))

	wp.Start(t.Context())
	defer wp.Stop()

	var counter atomic.Int64

	var wg sync.WaitGroup

	wg.Add(jobs)

	for range jobs {
		wp.Submit(&funcTask{do: func(context.Context) error {
			time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond) //nolint:gosec
			counter.Add(1)
			wg.Done()

			return nil
		}})
	}

	waitOrFail(t, &wg)

	if counter.Load() != jobs {
		t.Fatalf("lost jobs: expected %d got %d",
			jobs, counter.Load(),
		)
	}
}

func TestWorkerPool_RetriesFailedJob(t *testing.T) {
	wp := NewWorkerPool(WithWorkerCount(1), WithCapacity(4))

	wp.Start(t.Context())
	defer wp.Stop()

	var attempts atomic.Int32

	done := make(chan struct{})

	wp.Submit(&funcTask{do: func(context.Context) error {
		if attempts.Add(1) == 1 {
			return errors.New("fail once")
		}

		close(done)

		return nil
	}})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("job was never retried, attempts=%d", attempts.Load())
	}

	if attempts.Load() != 2 {
		t.Fatalf("expected exactly 2 attempts, got %d", attempts.Load())
	}
}

func TestWorkerPool_Stop_WaitsForRetryQueueToDrain(t *testing.T) {
	pool := NewWorkerPool(WithWorkerCount(1), WithCapacity(4))
	pool.Start(t.Context())

	var attempts atomic.Int32

	proceed := make(chan struct{}) // держит попытки под полным контролем теста

	pool.Submit(&funcTask{do: func(context.Context) error {
		<-proceed

		if attempts.Add(1) < 3 {
			return errors.New("fail")
		}

		return nil
	}})

	stopped := make(chan struct{})

	go func() {
		pool.Stop()
		close(stopped)
	}()

	// пока ни одной попытки не разрешено — Stop не может завершиться
	select {
	case <-stopped:
		t.Fatal("Stop returned before any attempt ran")
	case <-time.After(150 * time.Millisecond):
	}

	release := func() {
		select {
		case proceed <- struct{}{}:
		case <-time.After(10 * time.Second):
			t.Fatal("worker never picked up the retried job")
		}
	}

	release() // попытка 1: fail
	release() // попытка 2: fail

	select {
	case <-stopped:
		t.Fatal("Stop returned before job finished retrying")
	default:
	}

	release() // попытка 3: success

	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop never returned")
	}

	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}
}

func waitOrFail(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for WaitGroup")
	}
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return false
	case <-time.After(timeout):
		return true
	}
}
