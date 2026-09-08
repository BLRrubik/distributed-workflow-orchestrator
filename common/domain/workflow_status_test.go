package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWorkflowStatus_CanTransitTo(t *testing.T) {
	tests := []struct {
		name    string
		from    WorkflowStatus
		to      WorkflowStatus
		wantErr error
	}{
		{name: "pending to running", from: WorkflowPending, to: WorkflowRunning},
		{name: "pending to cancelled", from: WorkflowPending, to: WorkflowCancelled},
		{name: "running to succeeded", from: WorkflowRunning, to: WorkflowSucceeded},
		{name: "running to failed", from: WorkflowRunning, to: WorkflowFailed},
		{name: "running to cancelled", from: WorkflowRunning, to: WorkflowCancelled},

		{name: "same status", from: WorkflowPending, to: WorkflowPending, wantErr: ErrTransitToSameStatus},
		{name: "pending to succeeded not allowed", from: WorkflowPending, to: WorkflowSucceeded, wantErr: ErrTransitNotAllowed},
		{name: "succeeded is terminal", from: WorkflowSucceeded, to: WorkflowRunning, wantErr: ErrTransitNotAllowed},
		{name: "failed is terminal", from: WorkflowFailed, to: WorkflowRunning, wantErr: ErrTransitNotAllowed},
		{name: "cancelled is terminal", from: WorkflowCancelled, to: WorkflowRunning, wantErr: ErrTransitNotAllowed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.from.CanTransitTo(tt.to)

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			assert.NoError(t, err)
		})
	}
}
