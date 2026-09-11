package wp

import (
	"container/heap"
	"sync"
)

type ItemPQ struct {
	Value      Job
	index      int
	waitToTime int64
}

type PriorityQueue struct {
	items []*ItemPQ
	seq   uint64
	mux   sync.Mutex
}

func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{}
	pq.items = make([]*ItemPQ, 0)
	heap.Init(pq)

	return pq
}

func (pq *PriorityQueue) Len() int {
	return len(pq.items)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	return pq.items[i].waitToTime < pq.items[j].waitToTime
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	item, ok := x.(*ItemPQ)
	if !ok {
		return
	}

	item.index = len(pq.items)
	pq.items = append(pq.items, item)
}

func (pq *PriorityQueue) Pop() any {
	n := len(pq.items)
	item := pq.items[n-1]
	item.index = -1
	pq.items[n-1] = nil
	pq.items = pq.items[0 : n-1]

	return item
}

func (pq *PriorityQueue) Top() any {
	pq.mux.Lock()
	defer pq.mux.Unlock()

	n := len(pq.items)
	if n == 0 {
		return nil
	}

	return pq.items[0]
}

func (pq *PriorityQueue) Update(item *ItemPQ, value Job) {
	item.Value = value
	heap.Fix(pq, item.index)
}

func (pq *PriorityQueue) Enqueue(item *ItemPQ) {
	pq.mux.Lock()
	defer pq.mux.Unlock()

	pq.seq++

	heap.Push(pq, item)
}

func (pq *PriorityQueue) Dequeue() (Job, bool) {
	pq.mux.Lock()
	defer pq.mux.Unlock()

	if len(pq.items) == 0 {
		var zero Job

		return zero, false
	}

	item, _ := heap.Pop(pq).(*ItemPQ)

	return item.Value, true
}

func (pq *PriorityQueue) Peek() (Job, bool) {
	pq.mux.Lock()
	defer pq.mux.Unlock()

	if len(pq.items) == 0 {
		var zero Job

		return zero, false
	}

	return pq.items[0].Value, true
}

func (pq *PriorityQueue) Size() int {
	pq.mux.Lock()
	defer pq.mux.Unlock()

	return len(pq.items)
}
