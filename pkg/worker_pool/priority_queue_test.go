package wp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPriorityQueue(t *testing.T) {
	pq := NewPriorityQueue[int]()

	assert.Equal(t, 0, pq.Size())
	item, ok := pq.Peek()
	assert.False(t, ok)
	assert.Equal(t, 0, item)

	pq.Enqueue(&ItemPQ[int]{Value: 1, waitToTime: 1})

	assert.Equal(t, 1, pq.Size())

	pq.Enqueue(&ItemPQ[int]{Value: 2, waitToTime: 2})
	assert.Equal(t, 2, pq.Size())

	item, ok = pq.Peek()
	assert.True(t, ok)
	assert.Equal(t, 1, item)

	item, ok = pq.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, 1, item)
	assert.Equal(t, 1, pq.Size())

	item, ok = pq.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, 2, item)
	assert.Equal(t, 0, pq.Size())
}
