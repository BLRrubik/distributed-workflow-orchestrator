package domain

import "testing"

func TestWorkflowStatus_String(t *testing.T) {
	tests := []struct {
		name string
		wt   WorkflowStatus
		want string
	}{
		{name: "PENDING", wt: WorkflowPending, want: "PENDING"},
		{name: "RUNNING", wt: WorkflowRunning, want: "RUNNING"},
		{name: "SUCCEEDED", wt: WorkflowSucceeded, want: "SUCCEEDED"},
		{name: "FAILED", wt: WorkflowFailed, want: "FAILED"},
		{name: "CANCELLED", wt: WorkflowCancelled, want: "CANCELLED"},
		{name: "UNKNOWN", wt: "unknown", want: "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.wt.String(); got != tt.want {
				t.Errorf("WorkflowStatus.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
