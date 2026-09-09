package wp

import (
	"container/heap"
	"sync"
)

type ItemPQ[T any] struct {
	Value      T
	index      int
	waitToTime int64
}

type PriorityQueue[T any] struct {
	items []*ItemPQ[T]
	seq   uint64
	mux   sync.Mutex
}

func NewPriorityQueue[T any]() *PriorityQueue[T] {
	pq := &PriorityQueue[T]{}
	pq.items = make([]*ItemPQ[T], 0)
	heap.Init(pq)

	return pq
}

func (pq *PriorityQueue[T]) Len() int {
	return len(pq.items)
}

func (pq *PriorityQueue[T]) Less(i, j int) bool {
	return pq.items[i].waitToTime < pq.items[j].waitToTime
}

func (pq *PriorityQueue[T]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *PriorityQueue[T]) Push(x any) {
	item, ok := x.(*ItemPQ[T])
	if !ok {
		return
	}

	item.index = len(pq.items)
	pq.items = append(pq.items, item)
}

func (pq *PriorityQueue[T]) Pop() any {
	n := len(pq.items)
	item := pq.items[n-1]
	item.index = -1
	pq.items[n-1] = nil
	pq.items = pq.items[0 : n-1]

	return item
}

func (pq *PriorityQueue[T]) Top() any {
	pq.mux.Lock()
	defer pq.mux.Unlock()

	n := len(pq.items)
	if n == 0 {
		return nil
	}

	return pq.items[0]
}

func (pq *PriorityQueue[T]) Update(item *ItemPQ[T], value T) {
	item.Value = value
	heap.Fix(pq, item.index)
}

func (pq *PriorityQueue[T]) Enqueue(item *ItemPQ[T]) {
	pq.mux.Lock()
	defer pq.mux.Unlock()

	pq.seq++

	heap.Push(pq, item)
}
func (pq *PriorityQueue[T]) Dequeue() (T, bool) {
	pq.mux.Lock()
	defer pq.mux.Unlock()

	if len(pq.items) == 0 {
		var zero T
		return zero, false
	}

	item, _ := heap.Pop(pq).(*ItemPQ[T])

	return item.Value, true
}

func (pq *PriorityQueue[T]) Peek() (T, bool) {
	pq.mux.Lock()
	defer pq.mux.Unlock()

	if len(pq.items) == 0 {
		var zero T
		return zero, false
	}

	return pq.items[0].Value, true
}

func (pq *PriorityQueue[T]) Size() int {
	pq.mux.Lock()
	defer pq.mux.Unlock()

	return len(pq.items)
}
