package event_bus

import (
	"fmt"
	"sync"
)

type Bus struct {
	subscribers []*Subscriber

	mu   sync.RWMutex
	done chan struct{}
	ch   chan Event
	wg   sync.WaitGroup

	stopped bool
}

func NewBus(capacity int) *Bus {
	if capacity == 0 {
		capacity = 128
	}

	b := &Bus{
		subscribers: make([]*Subscriber, 0, capacity),
		ch:          make(chan Event, capacity),
		done:        make(chan struct{}),
		wg:          sync.WaitGroup{},
		stopped:     false,
	}

	go b.dispatch()

	return b
}

func (b *Bus) Subscribe(id string, handler Handler) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stopped {
		return fmt.Errorf("bus is stopped")
	}

	for i, sub := range b.subscribers {
		if sub.id == id {
			close(sub.close)

			b.subscribers = append(b.subscribers[:i], b.subscribers[i+1:]...)

			break
		}
	}

	sub := NewSubscriber(id, handler)

	b.subscribers = append(b.subscribers, sub)
	b.wg.Add(1)
	go b.startSubscriber(sub)

	return nil
}

func (b *Bus) Unsubscribe(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range b.subscribers {
		if sub.id == id {
			close(sub.close)
			b.subscribers = append(b.subscribers[:i], b.subscribers[i+1:]...)

			break
		}
	}

	return nil
}

func (b *Bus) Publish(event Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stopped {
		return
	}

	select {
	case b.ch <- event:
	default:
	}
}

func (b *Bus) Shutdown() {
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()

		return
	}
	b.stopped = true
	b.mu.Unlock()

	close(b.done)
	b.wg.Wait()
}

func (b *Bus) dispatch() {
	for {
		select {
		case <-b.done:
			return
		case event, ok := <-b.ch:
			if !ok {
				return
			}
			b.mu.RLock()
			for _, sub := range b.subscribers {
				select {
				case sub.queue <- event:
				default:
				}
			}
			b.mu.RUnlock()
		}
	}
}

func (b *Bus) startSubscriber(sub *Subscriber) {
	defer b.wg.Done()
	for {
		select {
		case e := <-sub.queue:
			sub.handler(e)
		case <-sub.close:
			return
		case <-b.done:
			return
		}
	}
}
