package domain

import "testing"

func TestTaskStatus_String(t *testing.T) {
	tests := []struct {
		name string
		t    TaskStatus
		want string
	}{
		{name: "PENDING", t: TaskPending, want: "PENDING"},
		{name: "DISPATCHED", t: TaskDispatched, want: "DISPATCHED"},
		{name: "RUNNING", t: TaskRunning, want: "RUNNING"},
		{name: "SUCCEEDED", t: TaskSucceeded, want: "SUCCEEDED"},
		{name: "FAILED", t: TaskFailed, want: "FAILED"},
		{name: "RETRYING", t: TaskRetrying, want: "RETRYING"},
		{name: "CANCELLED", t: TaskCancelled, want: "CANCELLED"},
		{name: "UNKNOWN", t: "unknown", want: "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.t.String(); got != tt.want {
				t.Errorf("TaskStatus.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
