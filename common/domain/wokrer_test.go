package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWorkerNode_StatusTransitions(t *testing.T) {
	n := &WorkerNode{}

	assert.Equal(t, WorkerAlive, n.GetStatus())
	assert.True(t, n.IsReady())

	n.SetSuspect()
	assert.Equal(t, WorkerSuspect, n.GetStatus())
	assert.False(t, n.IsReady())

	n.SetDead()
	assert.Equal(t, WorkerDead, n.GetStatus())
	assert.False(t, n.IsReady())

	n.SetAlive()
	assert.Equal(t, WorkerAlive, n.GetStatus())
	assert.True(t, n.IsReady())
}

func TestWorkerStatus_String(t *testing.T) {
	tests := []struct {
		name   string
		status WorkerStatus
		want   string
	}{
		{name: "alive", status: WorkerAlive, want: "ALIVE"},
		{name: "suspect", status: WorkerSuspect, want: "SUSPECT"},
		{name: "dead", status: WorkerDead, want: "DEAD"},
		{name: "unknown", status: WorkerStatus(255), want: "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.status.String())
		})
	}
}
