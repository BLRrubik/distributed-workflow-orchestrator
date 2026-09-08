package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTaskStatus_String(t *testing.T) {
	tests := []struct {
		name string
		t    TaskStatus
		want string
	}{
		{name: "PENDING", t: TaskPending, want: "PENDING"},
		{name: "DISPATCHED", t: TaskDispatched, want: "DISPATCHED"},
		{name: "READY", t: TaskReady, want: "READY"},
		{name: "RUNNING", t: TaskRunning, want: "RUNNING"},
		{name: "SUCCEEDED", t: TaskSucceeded, want: "SUCCEEDED"},
		{name: "FAILED", t: TaskFailed, want: "FAILED"},
		{name: "RETRYING", t: TaskRetrying, want: "RETRYING"},
		{name: "CANCELLED", t: TaskCancelled, want: "CANCELLED"},
		{name: "UNKNOWN", t: TaskStatus(99), want: "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.t.String(); got != tt.want {
				t.Errorf("TaskStatus.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTask_UpdateStatus(t *testing.T) {
	task := &Task{ID: "task1", Name: "task1"}
	assert.Equal(t, TaskPending, task.GetStatus())

	assert.NoError(t, task.UpdateStatus(TaskReady))
	assert.Equal(t, TaskReady, task.GetStatus())

	assert.Error(t, task.UpdateStatus(TaskSucceeded))
	assert.Equal(t, TaskReady, task.GetStatus(), "status must not change on rejected transition")

	assert.Error(t, task.UpdateStatus(TaskReady))
	assert.Equal(t, TaskReady, task.GetStatus(), "transition to same status must be rejected")
}

func TestTask_GetSetResult(t *testing.T) {
	task := &Task{ID: "task1", Name: "task1"}

	_, ok := task.GetResult()
	assert.False(t, ok)

	task.SetResult(&TaskResult{ExitCode: 0, Stdout: "hello"})

	result, ok := task.GetResult()
	assert.True(t, ok)
	assert.Equal(t, "hello", result.Stdout)
}
