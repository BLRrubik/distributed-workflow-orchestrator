package wp

import "sync"

type Item struct {
	value Job
	next  *Item
}

type LinkedQueue struct {
	head *Item
	tail *Item
	size int
	mu   sync.Mutex
}

func NewLinkedQueue() *LinkedQueue {
	return &LinkedQueue{
		head: nil,
		tail: nil,
		size: 0,
	}
}

func (q *LinkedQueue) Queue(value Job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	item := &Item{
		value: value,
	}

	if q.head == nil {
		q.head = item
		q.tail = item
	} else {
		q.tail.next = item
		q.tail = item
	}

	q.size++
}

func (q *LinkedQueue) Dequeue() (Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head == nil {
		var zero Job

		return zero, false
	}

	head := q.head
	q.head = q.head.next

	if q.head == nil {
		q.tail = nil
	}

	head.next = nil
	q.size--

	return head.value, true
}

func (q *LinkedQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.size
}
