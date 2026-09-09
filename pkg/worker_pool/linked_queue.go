package wp

import "sync"

type Item[T any] struct {
	value T
	next  *Item[T]
}

type LinkedQueue[T any] struct {
	head *Item[T]
	tail *Item[T]
	size int
	mu   sync.Mutex
}

func NewLinkedQueue[T any]() *LinkedQueue[T] {
	return &LinkedQueue[T]{
		head: nil,
		tail: nil,
		size: 0,
	}
}

func (q *LinkedQueue[T]) Queue(value T) {
	q.mu.Lock()
	defer q.mu.Unlock()

	item := &Item[T]{
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

func (q *LinkedQueue[T]) Dequeue() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head == nil {
		var zero T

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

func (q *LinkedQueue[T]) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.size
}
