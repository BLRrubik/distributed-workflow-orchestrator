package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTaskStatus_CanTransitTo(t *testing.T) {
	tests := []struct {
		name    string
		from    TaskStatus
		to      TaskStatus
		wantErr error
	}{
		{name: "pending to ready", from: TaskPending, to: TaskReady},
		{name: "pending to cancelled", from: TaskPending, to: TaskCancelled},
		{name: "ready to dispatched", from: TaskReady, to: TaskDispatched},
		{name: "dispatched to running", from: TaskDispatched, to: TaskRunning},
		{name: "running to succeeded", from: TaskRunning, to: TaskSucceeded},
		{name: "running to failed", from: TaskRunning, to: TaskFailed},
		{name: "failed to retrying", from: TaskFailed, to: TaskRetrying},
		{name: "retrying to dispatched", from: TaskRetrying, to: TaskDispatched},

		{name: "same status", from: TaskPending, to: TaskPending, wantErr: ErrTransitToSameStatus},
		{name: "pending to succeeded not allowed", from: TaskPending, to: TaskSucceeded, wantErr: ErrTransitNotAllowed},
		{name: "ready to succeeded not allowed", from: TaskReady, to: TaskSucceeded, wantErr: ErrTransitNotAllowed},
		{name: "succeeded is terminal", from: TaskSucceeded, to: TaskRetrying, wantErr: ErrTransitNotAllowed},
		{name: "cancelled is terminal", from: TaskCancelled, to: TaskReady, wantErr: ErrTransitNotAllowed},
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
