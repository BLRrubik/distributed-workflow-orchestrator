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

func TestWorkerPool_ExecutesJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := NewWorkerPool[int]()
	wp.Start(ctx)
	defer wp.Stop()

	var mu sync.Mutex
	results := make([]int, 0)

	var wg sync.WaitGroup
	wg.Add(5)

	for i := range 5 {
		v := i

		wp.Submit(v, func(n int) error {
			defer wg.Done()

			mu.Lock()
			results = append(results, n)
			mu.Unlock()

			return nil
		})
	}

	waitOrFail(t, &wg)

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
}

func TestWorkerPool_ParallelExecution(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := NewWorkerPool[int](WithWorkerCount[int](4), WithCapacity[int](10))
	wp.Start(ctx)
	defer wp.Stop()

	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(4)

	for i := range 4 {
		wp.Submit(i, func(n int) error {
			defer wg.Done()
			time.Sleep(100 * time.Millisecond)

			return nil
		})
	}

	waitOrFail(t, &wg)

	elapsed := time.Since(start)

	if elapsed > 200*time.Millisecond {
		t.Fatalf("jobs did not run in parallel, took %v", elapsed)
	}
}

func TestWorkerPool_StopWaitsForJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := NewWorkerPool[int]()
	wp.Start(ctx)

	var wg sync.WaitGroup
	wg.Add(1)

	wp.Submit(1, func(n int) error {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)

		return nil
	})

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := NewWorkerPool[int](WithWorkerCount[int](1), WithCapacity[int](1))
	wp.Start(ctx)
	defer wp.Stop()

	blocker := make(chan struct{})

	// заполняем очередь
	ok := wp.TrySubmit(1, func(n int) error {
		<-blocker
		return nil
	})
	if !ok {
		t.Fatal("expected first TrySubmit to succeed")
	}

	// очередь full
	ok = wp.TrySubmit(2, func(n int) error { return nil })
	if ok {
		t.Fatal("expected TrySubmit to fail when queue full")
	}

	close(blocker)
}

func TestWorkerPool_Stress10kJobs(t *testing.T) {
	const (
		workers = 8
		jobs    = 10_000
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := NewWorkerPool[int](WithWorkerCount[int](workers), WithCapacity[int](jobs))
	wp.Start(ctx)
	defer wp.Stop()

	var counter atomic.Int64
	var wg sync.WaitGroup
	wg.Add(jobs)

	start := time.Now()

	for i := range jobs {
		v := i

		wp.Submit(v, func(n int) error {
			counter.Add(1)
			wg.Done()

			return nil
		})
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := NewWorkerPool[int](WithWorkerCount[int](workers), WithCapacity[int](queue))
	wp.Start(ctx)
	defer wp.Stop()

	var counter atomic.Int64
	var wg sync.WaitGroup
	wg.Add(jobs)

	start := time.Now()

	for i := range jobs {
		v := i

		wp.Submit(v, func(n int) error {
			counter.Add(1)
			wg.Done()

			return nil
		})
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := NewWorkerPool[int](WithWorkerCount[int](8), WithCapacity[int](128))
	wp.Start(ctx)
	defer wp.Stop()

	var counter atomic.Int64
	var wg sync.WaitGroup
	wg.Add(jobs)

	for i := range jobs {
		wp.Submit(i, func(n int) error {
			time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond) //nolint:gosec
			counter.Add(1)
			wg.Done()

			return nil
		})
	}

	waitOrFail(t, &wg)

	if counter.Load() != jobs {
		t.Fatalf("lost jobs: expected %d got %d",
			jobs, counter.Load(),
		)
	}
}

func TestWorkerPool_RetriesFailedJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := NewWorkerPool[int](WithWorkerCount[int](1), WithCapacity[int](4))
	wp.Start(ctx)
	defer wp.Stop()

	var attempts atomic.Int32
	done := make(chan struct{})

	wp.Submit(1, func(n int) error {
		if attempts.Add(1) == 1 {
			return errors.New("fail once")
		}

		close(done)

		return nil
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("job was never retried, attempts=%d", attempts.Load())
	}

	if attempts.Load() != 2 {
		t.Fatalf("expected exactly 2 attempts, got %d", attempts.Load())
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
