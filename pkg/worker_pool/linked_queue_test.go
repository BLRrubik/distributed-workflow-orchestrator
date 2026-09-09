package wp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLinkedQueue(t *testing.T) {
	queue := NewLinkedQueue()

	assert.Equal(t, 0, queue.Size())

	job1 := Job{waitToTime: 1}
	job2 := Job{waitToTime: 2}

	queue.Queue(job1)
	assert.Equal(t, 1, queue.Size())

	queue.Queue(job2)
	assert.Equal(t, 2, queue.Size())

	val, ok := queue.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, job1, val)
	assert.Equal(t, 1, queue.Size())

	val, ok = queue.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, job2, val)
	assert.Equal(t, 0, queue.Size())

	_, ok = queue.Dequeue()
	assert.False(t, ok)
}
