package event_bus

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testEvent struct{ typ string }

func (e testEvent) GetType() string { return e.typ }

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestPublishDeliversToSubscriber(t *testing.T) {
	b := NewBus(4)
	defer b.Shutdown()

	var got atomic.Value
	done := make(chan struct{}, 1)

	err := b.Subscribe("sub1", func(e Event) {
		got.Store(e)
		done <- struct{}{}
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	b.Publish(testEvent{typ: "created"})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event not delivered within timeout")
	}

	e, ok := got.Load().(testEvent)
	if !ok || e.GetType() != "created" {
		t.Fatalf("unexpected event delivered: %v", got.Load())
	}
}

func TestPublishFansOutToMultipleSubscribers(t *testing.T) {
	b := NewBus(4)
	defer b.Shutdown()

	const n = 5
	var wg sync.WaitGroup
	wg.Add(n)
	var count atomic.Int32

	for i := 0; i < n; i++ {
		id := string(rune('a' + i))
		if err := b.Subscribe(id, func(e Event) {
			count.Add(1)
			wg.Done()
		}); err != nil {
			t.Fatalf("Subscribe(%s) failed: %v", id, err)
		}
	}

	b.Publish(testEvent{typ: "broadcast"})

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatalf("not all subscribers received event, got %d/%d", count.Load(), n)
	}
}

func TestSubscribeSameIDReplacesOldSubscriber(t *testing.T) {
	b := NewBus(4)
	defer b.Shutdown()

	var oldCalls, newCalls atomic.Int32

	if err := b.Subscribe("dup", func(e Event) { oldCalls.Add(1) }); err != nil {
		t.Fatalf("first Subscribe failed: %v", err)
	}
	if err := b.Subscribe("dup", func(e Event) { newCalls.Add(1) }); err != nil {
		t.Fatalf("second Subscribe failed: %v", err)
	}

	b.Publish(testEvent{typ: "x"})

	waitFor(t, time.Second, func() bool { return newCalls.Load() == 1 })

	if oldCalls.Load() != 0 {
		t.Fatalf("old subscriber handler still firing: %d calls", oldCalls.Load())
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	b := NewBus(4)
	defer b.Shutdown()

	var calls atomic.Int32
	if err := b.Subscribe("sub1", func(e Event) { calls.Add(1) }); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if err := b.Unsubscribe("sub1"); err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}

	b.Publish(testEvent{typ: "after-unsub"})

	time.Sleep(100 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("unsubscribed handler still called: %d calls", calls.Load())
	}
}

func TestPublishAfterShutdownIsNoop(t *testing.T) {
	b := NewBus(4)

	var calls atomic.Int32
	if err := b.Subscribe("sub1", func(e Event) { calls.Add(1) }); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	b.Shutdown()
	b.Publish(testEvent{typ: "ignored"})

	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("handler called after shutdown: %d calls", calls.Load())
	}
}

func TestSubscribeAfterShutdownReturnsError(t *testing.T) {
	b := NewBus(4)
	b.Shutdown()

	if err := b.Subscribe("sub1", func(e Event) {}); err == nil {
		t.Fatal("expected error subscribing to a stopped bus, got nil")
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	b := NewBus(4)
	if err := b.Subscribe("sub1", func(e Event) {}); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	done := make(chan struct{})
	go func() {
		b.Shutdown()
		b.Shutdown()
		b.Shutdown()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Shutdown hung or deadlocked on repeated calls")
	}
}

// Stresses Publish/Subscribe/Shutdown concurrently to catch locking
// regressions (RLock/RUnlock mismatch, WaitGroup misuse) under -race.
func TestConcurrentPublishAndShutdownNoDeadlock(t *testing.T) {
	b := NewBus(8)

	for i := 0; i < 10; i++ {
		id := string(rune('a' + i))
		if err := b.Subscribe(id, func(e Event) {}); err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}
	}

	publishersDone := make(chan struct{})
	go func() {
		for i := 0; i < 2000; i++ {
			b.Publish(testEvent{typ: "spam"})
		}
		close(publishersDone)
	}()

	shutdownDone := make(chan struct{})
	go func() {
		time.Sleep(time.Millisecond)
		b.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not complete - possible deadlock")
	}
	<-publishersDone

	// If RLock leaked in dispatch(), any further Lock-based call hangs forever.
	postShutdownDone := make(chan struct{})
	go func() {
		_ = b.Unsubscribe("a")
		b.Publish(testEvent{typ: "post-shutdown"})
		close(postShutdownDone)
	}()

	select {
	case <-postShutdownDone:
	case <-time.After(time.Second):
		t.Fatal("deadlock after shutdown - mutex leaked")
	}
}
