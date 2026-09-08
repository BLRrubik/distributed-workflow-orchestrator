package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
		{name: "UNKNOWN", wt: WorkflowStatus(99), want: "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.wt.String(); got != tt.want {
				t.Errorf("WorkflowStatus.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_ValidateDependencies(t *testing.T) {
	validDependencies := map[string]*Task{
		"task1": {
			ID:        "task1",
			Name:      "task1",
			DependsOn: []string{},
		},
		"task2": {
			ID:   "task2",
			Name: "task2",
			DependsOn: []string{
				"task1",
			},
		},
		"task3": {
			ID:   "task3",
			Name: "task3",
			DependsOn: []string{
				"task1",
				"task2",
			},
		},
	}

	invalidDependencies := map[string]*Task{
		"task1": {
			ID:        "task1",
			Name:      "task1",
			DependsOn: []string{},
		},
		"task3": {
			ID:   "task3",
			Name: "task3",
			DependsOn: []string{
				"task1",
				"task2",
			},
		},
	}

	assert.NoError(t, validateDependencies(validDependencies))
	assert.Error(t, validateDependencies(invalidDependencies))
}

func TestNewWorkflow(t *testing.T) {
	tests := []struct {
		name    string
		tasks   []Task
		wantErr string
	}{
		{
			name: "success",
			tasks: []Task{
				{ID: "task1", Name: "task1", DependsOn: []string{}},
				{ID: "task2", Name: "task2", DependsOn: []string{"task1"}},
			},
		},
		{
			name:  "empty tasks",
			tasks: []Task{},
		},
		{
			name: "duplicate task name",
			tasks: []Task{
				{ID: "task1", Name: "dup", DependsOn: []string{}},
				{ID: "task2", Name: "dup", DependsOn: []string{}},
			},
			wantErr: "duplicate task name: dup",
		},
		{
			name: "invalid dependency",
			tasks: []Task{
				{ID: "task1", Name: "task1", DependsOn: []string{"missing"}},
			},
			wantErr: "invalid tasks: invalid dependency: missing",
		},
		{
			name: "cycle detected",
			tasks: []Task{
				{ID: "task1", Name: "task1", DependsOn: []string{"task1"}},
			},
			wantErr: "tasks cycle failed: cycle detected in task: task1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wf, err := NewWorkflow("tenant1", "wf1", tt.tasks)

			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				assert.Nil(t, wf)

				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, wf)
			assert.NotEmpty(t, wf.ID)
			assert.Equal(t, "tenant1", wf.TenantID)
			assert.Equal(t, "wf1", wf.Name)
			assert.Equal(t, WorkflowPending, wf.GetStatus())
			assert.Len(t, wf.Tasks, len(tt.tasks))
		})
	}
}

func TestWorkflow_UpdateStatus(t *testing.T) {
	ctx := newTestCtx()

	wf, err := NewWorkflow("tenant1", "wf1", []Task{
		{ID: "task1", Name: "task1", DependsOn: []string{}},
	})
	assert.NoError(t, err)
	assert.Equal(t, WorkflowPending, wf.GetStatus())

	assert.True(t, wf.UpdateStatus(ctx, WorkflowRunning))
	assert.Equal(t, WorkflowRunning, wf.GetStatus())

	assert.False(t, wf.UpdateStatus(ctx, WorkflowPending), "backwards transition must be rejected")
	assert.Equal(t, WorkflowRunning, wf.GetStatus())

	assert.True(t, wf.UpdateStatus(ctx, WorkflowSucceeded))
	assert.Equal(t, WorkflowSucceeded, wf.GetStatus())

	assert.False(t, wf.UpdateStatus(ctx, WorkflowFailed), "terminal status must be rejected")
}

func TestWorkflow_IsFinished(t *testing.T) {
	ctx := newTestCtx()

	wf, err := NewWorkflow("tenant1", "wf1", []Task{
		{ID: "task1", Name: "task1", DependsOn: []string{}},
	})
	assert.NoError(t, err)
	assert.False(t, wf.IsFinished())

	assert.True(t, wf.UpdateStatus(ctx, WorkflowRunning))
	assert.False(t, wf.IsFinished())

	assert.True(t, wf.UpdateStatus(ctx, WorkflowSucceeded))
	assert.True(t, wf.IsFinished())
}

func TestWorkflow_AllTasksFinished(t *testing.T) {
	ctx := newTestCtx()

	wf, err := NewWorkflow("tenant1", "wf1", []Task{
		{ID: "task1", Name: "task1", DependsOn: []string{}},
		{ID: "task2", Name: "task2", DependsOn: []string{}},
	})
	assert.NoError(t, err)
	assert.False(t, wf.AllTasksFinished())

	assert.True(t, wf.Tasks["task1"].UpdateStatus(ctx, TaskReady))
	assert.True(t, wf.Tasks["task1"].UpdateStatus(ctx, TaskDispatched))
	assert.True(t, wf.Tasks["task1"].UpdateStatus(ctx, TaskRunning))
	assert.True(t, wf.Tasks["task1"].UpdateStatus(ctx, TaskSucceeded))
	assert.False(t, wf.AllTasksFinished(), "task2 still pending")

	assert.True(t, wf.Tasks["task2"].UpdateStatus(ctx, TaskCancelled))
	assert.True(t, wf.AllTasksFinished())
}

func TestWorkflow_HasFailedTask(t *testing.T) {
	ctx := newTestCtx()

	wf, err := NewWorkflow("tenant1", "wf1", []Task{
		{ID: "task1", Name: "task1", DependsOn: []string{}},
		{ID: "task2", Name: "task2", DependsOn: []string{}},
	})
	assert.NoError(t, err)
	assert.False(t, wf.HasFailedTask())

	assert.True(t, wf.Tasks["task1"].UpdateStatus(ctx, TaskReady))
	assert.True(t, wf.Tasks["task1"].UpdateStatus(ctx, TaskDispatched))
	assert.True(t, wf.Tasks["task1"].UpdateStatus(ctx, TaskRunning))
	assert.True(t, wf.Tasks["task1"].UpdateStatus(ctx, TaskFailed))
	assert.True(t, wf.HasFailedTask())
}

func Test_ValidateTasksCycle(t *testing.T) {
	validDependencies := map[string]*Task{
		"task1": {
			ID:        "task1",
			Name:      "task1",
			DependsOn: []string{},
		},
		"task2": {
			ID:   "task2",
			Name: "task2",
			DependsOn: []string{
				"task1",
			},
		},
		"task3": {
			ID:   "task3",
			Name: "task3",
			DependsOn: []string{
				"task1",
				"task2",
			},
		},
	}

	invalidDependencies := map[string]*Task{
		"task1": {
			ID:        "task1",
			Name:      "task1",
			DependsOn: []string{},
		},
		"task2": {
			ID:   "task2",
			Name: "task2",
			DependsOn: []string{
				"task3",
			},
		},
		"task3": {
			ID:   "task3",
			Name: "task3",
			DependsOn: []string{
				"task4",
			},
		},
		"task4": {
			ID:   "task3",
			Name: "task3",
			DependsOn: []string{
				"task2",
			},
		},
	}

	assert.NoError(t, validateTasksCycle(validDependencies))
	assert.Error(t, validateTasksCycle(invalidDependencies))
}
