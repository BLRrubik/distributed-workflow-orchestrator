package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/client"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/orchestration"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/scheduler"
)

func buildTestDeployTasks() []domain.Task {
	return []domain.Task{
		{ID: "build", Name: "build", DependsOn: []string{}},
		{ID: "test", Name: "test", DependsOn: []string{"build"}},
		{ID: "deploy", Name: "deploy", DependsOn: []string{"test"}},
	}
}

func newTestEngine(t *testing.T) *WorkflowEngine {
	t.Helper()

	log := logger.New(logger.INFO, true)
	registry := orchestration.NewWorkerRegistry(log)
	workerClient := client.NewWorkerClient(registry)
	sched := scheduler.New(registry, workerClient, log)

	return NewWorkflowEngine(log, sched)
}

func TestSubmitWorkflow_MarksInitialTasksReady(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	wf, err := domain.NewWorkflow("tenant1", "wf1", buildTestDeployTasks())
	assert.NoError(t, err)

	id, err := e.SubmitWorkflow(ctx, wf)
	assert.NoError(t, err)
	assert.Equal(t, wf.ID, id)

	assert.Equal(t, domain.TaskReady, wf.Tasks["build"].GetStatus())
	assert.Equal(t, domain.TaskPending, wf.Tasks["test"].GetStatus())
	assert.Equal(t, domain.TaskPending, wf.Tasks["deploy"].GetStatus())
}

func TestOnTaskResponse_PropagatesReadyToDependents(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	wf, err := domain.NewWorkflow("tenant1", "wf1", buildTestDeployTasks())
	assert.NoError(t, err)

	_, err = e.SubmitWorkflow(ctx, wf)
	assert.NoError(t, err)

	// OnTaskResponse умеет ставить только Running/Succeeded/Failed — переход в
	// Dispatched в реальном флоу должен делать scheduler.commitAssignment, но
	// сейчас он этого не делает, так что эмулируем диспатч вручную.
	e.UpdateTaskStatus(ctx, wf.Tasks["build"], domain.TaskDispatched)

	err = e.OnTaskResponse(ctx, wf.ID, "build", domain.TaskResult{Status: domain.TaskSucceeded, ExitCode: 0})
	assert.NoError(t, err)

	assert.Equal(t, domain.TaskSucceeded, wf.Tasks["build"].GetStatus())
	assert.Equal(t, domain.TaskReady, wf.Tasks["test"].GetStatus())
	assert.Equal(t, domain.TaskPending, wf.Tasks["deploy"].GetStatus())

	e.UpdateTaskStatus(ctx, wf.Tasks["test"], domain.TaskDispatched)

	err = e.OnTaskResponse(ctx, wf.ID, "test", domain.TaskResult{Status: domain.TaskSucceeded, ExitCode: 0})
	assert.NoError(t, err)

	assert.Equal(t, domain.TaskSucceeded, wf.Tasks["test"].GetStatus())
	assert.Equal(t, domain.TaskReady, wf.Tasks["deploy"].GetStatus())

	e.UpdateTaskStatus(ctx, wf.Tasks["deploy"], domain.TaskDispatched)

	err = e.OnTaskResponse(ctx, wf.ID, "deploy", domain.TaskResult{Status: domain.TaskSucceeded, ExitCode: 0})
	assert.NoError(t, err)

	assert.Equal(t, domain.TaskSucceeded, wf.Tasks["deploy"].GetStatus())
}

func TestSubmitWorkflow_MarksWorkflowRunning(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	wf, err := domain.NewWorkflow("tenant1", "wf1", buildTestDeployTasks())
	assert.NoError(t, err)
	assert.Equal(t, domain.WorkflowPending, wf.GetStatus())

	_, err = e.SubmitWorkflow(ctx, wf)
	assert.NoError(t, err)
	assert.Equal(t, domain.WorkflowRunning, wf.GetStatus())
}

func TestOnTaskResponse_FinalizesWorkflowAsSucceeded(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	wf, err := domain.NewWorkflow("tenant1", "wf1", buildTestDeployTasks())
	assert.NoError(t, err)

	_, err = e.SubmitWorkflow(ctx, wf)
	assert.NoError(t, err)

	for _, taskID := range []string{"build", "test"} {
		e.UpdateTaskStatus(ctx, wf.Tasks[taskID], domain.TaskDispatched)
		assert.NoError(t, e.OnTaskResponse(ctx, wf.ID, taskID, domain.TaskResult{Status: domain.TaskSucceeded, ExitCode: 0}))
		assert.Equal(t, domain.WorkflowRunning, wf.GetStatus(), "workflow must stay RUNNING while tasks remain")
	}

	e.UpdateTaskStatus(ctx, wf.Tasks["deploy"], domain.TaskDispatched)
	assert.NoError(t, e.OnTaskResponse(ctx, wf.ID, "deploy", domain.TaskResult{Status: domain.TaskSucceeded, ExitCode: 0}))
	assert.Equal(t, domain.WorkflowSucceeded, wf.GetStatus())
	assert.True(t, wf.IsFinished())
}

func TestOnTaskResponse_WorkflowNotFound(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	err := e.OnTaskResponse(ctx, "missing-wf", "build", domain.TaskResult{})
	assert.Error(t, err)
}

func TestOnTaskResponse_TaskNotFound(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	wf, err := domain.NewWorkflow("tenant1", "wf1", buildTestDeployTasks())
	assert.NoError(t, err)

	_, err = e.SubmitWorkflow(ctx, wf)
	assert.NoError(t, err)

	err = e.OnTaskResponse(ctx, wf.ID, "missing-task", domain.TaskResult{})
	assert.Error(t, err)
}
