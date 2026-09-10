package retry_queue

import (
	"context"
	"sync"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
)

type node struct {
	value RetryTrigger
	next  *node
}

type RetryQueue struct {
	len      int
	stopChan chan bool
	head     *node
	tail     *node
	active   map[string]int64
	log      *logger.Logger
	mu       sync.Mutex
}

func NewRetryQueue() *RetryQueue {
	return &RetryQueue{
		active:   make(map[string]int64),
		stopChan: make(chan bool),
	}
}

func (q *RetryQueue) Push(trigger RetryTrigger) {
	q.mu.Lock()
	defer q.mu.Unlock()
	key := trigger.GetKey()
	if _, ok := q.active[key]; !ok {
		q.active[key] = time.Now().Add(trigger.GetTimeout()).Unix()
		q.enqueue(trigger)
	}
}

func (q *RetryQueue) Cancel(key string) {
	q.mu.Lock()
	delete(q.active, key)
	q.mu.Unlock()
}

func (q *RetryQueue) StopLoop() {
	select {
	case <-q.stopChan:
	default:
		q.stopChan <- true
		close(q.stopChan)
	}
}

func (q *RetryQueue) RunLoop(ctx context.Context) {
	var (
		trigger RetryTrigger
		ok      bool
		t       int64
		key     string
	)
	for {
		select {
		case <-q.stopChan:
			return
		default:
			time.Sleep(100 * time.Microsecond)
			ok = false
			q.mu.Lock()
			trigger = q.dequeue()
			if trigger != nil {
				key = trigger.GetKey()
				t, ok = q.active[key]
			}
			q.mu.Unlock()
			if !ok {
				continue
			}
			if time.Now().Unix() >= t {
				trigger.Log(ctx)
				if trigger.Do(ctx) {
					t = time.Now().Add(trigger.GetTimeout()).Unix()
				} else {
					q.Cancel(key)
				}
			}
			q.mu.Lock()
			if _, ok = q.active[key]; ok {
				q.active[key] = t
				q.enqueue(trigger)
			}
			q.mu.Unlock()
		}
	}
}

func (q *RetryQueue) Len() uint {
	q.mu.Lock()
	defer q.mu.Unlock()

	return uint(q.len)
}

func (q *RetryQueue) ActiveLen() uint {
	q.mu.Lock()
	defer q.mu.Unlock()

	return uint(len(q.active))
}

// ActiveKeys возвращает массив ключей (идентификаторов) первых 100 активных задач в очереди ретраев.
func (q *RetryQueue) ActiveKeys() []string {
	q.mu.Lock()
	defer q.mu.Unlock()

	const top = 100
	s := make([]string, 0, min(len(q.active), top))
	for trigger := q.head; trigger != nil && len(s) < top; trigger = trigger.next {
		key := trigger.value.GetKey()
		if _, ok := q.active[key]; ok {
			s = append(s, key)
		}
	}

	return s
}

// ActiveExists возвращает true, если задача с указанным ключем существует и активна.
func (q *RetryQueue) ActiveExists(key string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	var ok bool
	_, ok = q.active[key]

	return ok
}

func (q *RetryQueue) enqueue(trigger RetryTrigger) {
	n := &node{
		value: trigger,
	}
	if q.head == nil {
		q.head = n
		q.tail = n
	} else {
		q.tail.next = n
		q.tail = n
	}
	q.len++
}

func (q *RetryQueue) dequeue() RetryTrigger {
	if q.head == nil {
		return nil
	}
	tmp := q.head
	q.head = q.head.next
	if q.head == nil {
		q.tail = nil
	}
	tmp.next = nil
	q.len--

	return tmp.value
}
