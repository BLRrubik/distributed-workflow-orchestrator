package engine

import (
	"context"
	"testing"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/stretchr/testify/assert"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
)

func buildTestDeployTasks() []domain.Task {
	return []domain.Task{
		{ID: "build", Name: "build", DependsOn: []string{}},
		{ID: "test", Name: "test", DependsOn: []string{"build"}},
		{ID: "deploy", Name: "deploy", DependsOn: []string{"test"}},
	}
}

func TestSubmitWorkflow_MarksInitialTasksReady(t *testing.T) {
	log := logger.New(logger.INFO, true)
	e := NewWorkflowEngine(log)
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

func TestOnTaskCompleted_PropagatesReadyToDependents(t *testing.T) {
	log := logger.New(logger.INFO, true)
	e := NewWorkflowEngine(log)
	ctx := context.Background()

	wf, err := domain.NewWorkflow("tenant1", "wf1", buildTestDeployTasks())
	assert.NoError(t, err)

	_, err = e.SubmitWorkflow(ctx, wf)
	assert.NoError(t, err)

	err = e.OnTaskCompleted(ctx, wf.ID, "build", domain.TaskResult{ExitCode: 0})
	assert.NoError(t, err)

	assert.Equal(t, domain.TaskSucceeded, wf.Tasks["build"].GetStatus())
	assert.Equal(t, domain.TaskReady, wf.Tasks["test"].GetStatus())
	assert.Equal(t, domain.TaskPending, wf.Tasks["deploy"].GetStatus())

	err = e.OnTaskCompleted(ctx, wf.ID, "test", domain.TaskResult{ExitCode: 0})
	assert.NoError(t, err)

	assert.Equal(t, domain.TaskSucceeded, wf.Tasks["test"].GetStatus())
	assert.Equal(t, domain.TaskReady, wf.Tasks["deploy"].GetStatus())

	err = e.OnTaskCompleted(ctx, wf.ID, "deploy", domain.TaskResult{ExitCode: 0})
	assert.NoError(t, err)

	assert.Equal(t, domain.TaskSucceeded, wf.Tasks["deploy"].GetStatus())
}

func TestSubmitWorkflow_MarksWorkflowRunning(t *testing.T) {
	log := logger.New(logger.INFO, true)
	e := NewWorkflowEngine(log)
	ctx := context.Background()

	wf, err := domain.NewWorkflow("tenant1", "wf1", buildTestDeployTasks())
	assert.NoError(t, err)
	assert.Equal(t, domain.WorkflowPending, wf.GetStatus())

	_, err = e.SubmitWorkflow(ctx, wf)
	assert.NoError(t, err)
	assert.Equal(t, domain.WorkflowRunning, wf.GetStatus())
}

func TestOnTaskCompleted_FinalizesWorkflowAsSucceeded(t *testing.T) {
	log := logger.New(logger.INFO, true)
	e := NewWorkflowEngine(log)
	ctx := context.Background()

	wf, err := domain.NewWorkflow("tenant1", "wf1", buildTestDeployTasks())
	assert.NoError(t, err)

	_, err = e.SubmitWorkflow(ctx, wf)
	assert.NoError(t, err)

	for _, taskID := range []string{"build", "test"} {
		assert.NoError(t, e.OnTaskCompleted(ctx, wf.ID, taskID, domain.TaskResult{ExitCode: 0}))
		assert.Equal(t, domain.WorkflowRunning, wf.GetStatus(), "workflow must stay RUNNING while tasks remain")
	}

	assert.NoError(t, e.OnTaskCompleted(ctx, wf.ID, "deploy", domain.TaskResult{ExitCode: 0}))
	assert.Equal(t, domain.WorkflowSucceeded, wf.GetStatus())
	assert.True(t, wf.IsFinished())
}

func TestOnTaskCompleted_WorkflowNotFound(t *testing.T) {
	log := logger.New(logger.INFO, true)
	e := NewWorkflowEngine(log)
	ctx := context.Background()

	err := e.OnTaskCompleted(ctx, "missing-wf", "build", domain.TaskResult{})
	assert.Error(t, err)
}

func TestOnTaskCompleted_TaskNotFound(t *testing.T) {
	log := logger.New(logger.INFO, true)
	e := NewWorkflowEngine(log)
	ctx := context.Background()

	wf, err := domain.NewWorkflow("tenant1", "wf1", buildTestDeployTasks())
	assert.NoError(t, err)

	_, err = e.SubmitWorkflow(ctx, wf)
	assert.NoError(t, err)

	err = e.OnTaskCompleted(ctx, wf.ID, "missing-task", domain.TaskResult{})
	assert.Error(t, err)
}
