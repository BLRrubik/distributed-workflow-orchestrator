package wp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLinkedQueue(t *testing.T) {
	queue := NewLinkedQueue[int]()

	assert.Equal(t, 0, queue.Size())

	queue.Queue(1)
	assert.Equal(t, 1, queue.Size())

	queue.Queue(2)
	assert.Equal(t, 2, queue.Size())

	val, ok := queue.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, 1, val)
	assert.Equal(t, 1, queue.Size())

	val, ok = queue.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, 2, val)
	assert.Equal(t, 0, queue.Size())

	_, ok = queue.Dequeue()
	assert.False(t, ok)
}
