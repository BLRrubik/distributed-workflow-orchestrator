package wp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPriorityQueue(t *testing.T) {
	pq := NewPriorityQueue()

	assert.Equal(t, 0, pq.Size())
	item, ok := pq.Peek()
	assert.False(t, ok)
	assert.Equal(t, Job{}, item)

	job1 := Job{waitToTime: 1}
	job2 := Job{waitToTime: 2}

	pq.Enqueue(&ItemPQ{Value: job1, waitToTime: 1})

	assert.Equal(t, 1, pq.Size())

	pq.Enqueue(&ItemPQ{Value: job2, waitToTime: 2})
	assert.Equal(t, 2, pq.Size())

	item, ok = pq.Peek()
	assert.True(t, ok)
	assert.Equal(t, job1, item)

	item, ok = pq.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, job1, item)
	assert.Equal(t, 1, pq.Size())

	item, ok = pq.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, job2, item)
	assert.Equal(t, 0, pq.Size())
}
