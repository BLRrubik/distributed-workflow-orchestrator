package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func Test_ResolveDependencies(t *testing.T) {
	validDependencies := map[string]*Task{
		"task1": {
			ID:        "id-1",
			Name:      "task1",
			DependsOn: []string{},
		},
		"task2": {
			ID:   "id-2",
			Name: "task2",
			DependsOn: []string{
				"task1",
			},
		},
		"task3": {
			ID:   "id-3",
			Name: "task3",
			DependsOn: []string{
				"task1",
				"task2",
			},
		},
	}

	invalidDependencies := map[string]*Task{
		"task1": {
			ID:        "id-1",
			Name:      "task1",
			DependsOn: []string{},
		},
		"task3": {
			ID:   "id-3",
			Name: "task3",
			DependsOn: []string{
				"task1",
				"task2",
			},
		},
	}

	assert.NoError(t, resolveDependencies(validDependencies))
	// имена в DependsOn резолвятся в ID зависимых задач
	assert.Equal(t, []string{"id-1"}, validDependencies["task2"].DependsOn)
	assert.Equal(t, []string{"id-1", "id-2"}, validDependencies["task3"].DependsOn)

	assert.Error(t, resolveDependencies(invalidDependencies))
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
			name: "duplicate task id",
			tasks: []Task{
				{ID: "dup-id", Name: "task1", DependsOn: []string{}},
				{ID: "dup-id", Name: "task2", DependsOn: []string{}},
			},
			wantErr: "duplicate task id: dup-id",
		},
		{
			name: "depends_on by name with pre-generated ids",
			tasks: []Task{
				{ID: "uuid-1", Name: "fetch", DependsOn: []string{}},
				{ID: "uuid-2", Name: "build", DependsOn: []string{"fetch"}},
			},
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

// TestNewWorkflow_DependsOnByName_ResolvedToID проверяет ключевой контракт:
// task_id приходит уже сгенерённым, а depends_on в запросе — по имени задачи;
// NewWorkflow обязан резолвить имена в ID и хранить wf.Tasks по ID.
func TestNewWorkflow_DependsOnByName_ResolvedToID(t *testing.T) {
	wf, err := NewWorkflow("tenant1", "wf1", []Task{
		{ID: "uuid-1", Name: "fetch", DependsOn: []string{}},
		{ID: "uuid-2", Name: "build", DependsOn: []string{"fetch"}},
	})
	require.NoError(t, err)

	buildTask, ok := wf.Tasks["uuid-2"]
	require.True(t, ok, "wf.Tasks должен быть проиндексирован по ID")
	assert.Equal(t, []string{"uuid-1"}, buildTask.DependsOn, "имя зависимости должно резолвиться в её ID")

	assert.False(t, buildTask.AllDepsSucceeded(wf), "fetch ещё не завершён успехом")

	require.NoError(t, wf.Tasks["uuid-1"].UpdateStatus(TaskReady))
	require.NoError(t, wf.Tasks["uuid-1"].UpdateStatus(TaskDispatched))
	require.NoError(t, wf.Tasks["uuid-1"].UpdateStatus(TaskRunning))
	require.NoError(t, wf.Tasks["uuid-1"].UpdateStatus(TaskSucceeded))

	assert.True(t, buildTask.AllDepsSucceeded(wf))
}

func TestWorkflow_UpdateStatus(t *testing.T) {
	wf, err := NewWorkflow("tenant1", "wf1", []Task{
		{ID: "task1", Name: "task1", DependsOn: []string{}},
	})
	assert.NoError(t, err)
	assert.Equal(t, WorkflowPending, wf.GetStatus())

	assert.NoError(t, wf.UpdateStatus(WorkflowRunning))
	assert.Equal(t, WorkflowRunning, wf.GetStatus())

	assert.Error(t, wf.UpdateStatus(WorkflowPending), "backwards transition must be rejected")
	assert.Equal(t, WorkflowRunning, wf.GetStatus())

	assert.NoError(t, wf.UpdateStatus(WorkflowSucceeded))
	assert.Equal(t, WorkflowSucceeded, wf.GetStatus())

	assert.Error(t, wf.UpdateStatus(WorkflowFailed), "terminal status must be rejected")
}

func TestWorkflow_IsFinished(t *testing.T) {
	wf, err := NewWorkflow("tenant1", "wf1", []Task{
		{ID: "task1", Name: "task1", DependsOn: []string{}},
	})
	assert.NoError(t, err)
	assert.False(t, wf.IsFinished())

	assert.NoError(t, wf.UpdateStatus(WorkflowRunning))
	assert.False(t, wf.IsFinished())

	assert.NoError(t, wf.UpdateStatus(WorkflowSucceeded))
	assert.True(t, wf.IsFinished())
}

func TestWorkflow_AllTasksFinished(t *testing.T) {
	wf, err := NewWorkflow("tenant1", "wf1", []Task{
		{ID: "task1", Name: "task1", DependsOn: []string{}},
		{ID: "task2", Name: "task2", DependsOn: []string{}},
	})
	assert.NoError(t, err)
	assert.False(t, wf.AllTasksFinished())

	assert.NoError(t, wf.Tasks["task1"].UpdateStatus(TaskReady))
	assert.NoError(t, wf.Tasks["task1"].UpdateStatus(TaskDispatched))
	assert.NoError(t, wf.Tasks["task1"].UpdateStatus(TaskRunning))
	assert.NoError(t, wf.Tasks["task1"].UpdateStatus(TaskSucceeded))
	assert.False(t, wf.AllTasksFinished(), "task2 still pending")

	assert.NoError(t, wf.Tasks["task2"].UpdateStatus(TaskCancelled))
	assert.True(t, wf.AllTasksFinished())
}

func TestWorkflow_HasFailedTask(t *testing.T) {
	wf, err := NewWorkflow("tenant1", "wf1", []Task{
		{ID: "task1", Name: "task1", DependsOn: []string{}},
		{ID: "task2", Name: "task2", DependsOn: []string{}},
	})
	assert.NoError(t, err)
	assert.False(t, wf.HasFailedTask())

	assert.NoError(t, wf.Tasks["task1"].UpdateStatus(TaskReady))
	assert.NoError(t, wf.Tasks["task1"].UpdateStatus(TaskDispatched))
	assert.NoError(t, wf.Tasks["task1"].UpdateStatus(TaskRunning))
	assert.NoError(t, wf.Tasks["task1"].UpdateStatus(TaskFailed))
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
