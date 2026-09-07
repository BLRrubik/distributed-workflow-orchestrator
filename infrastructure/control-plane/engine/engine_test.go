package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	appctx "github.com/blrrubik/distributed-workflow-orchestrator/common/context"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
)

func newTestCtx() appctx.AppContext {
	return appctx.NewAppContext(context.Background(), logger.New(logger.ERROR, true))
}

func buildTestDeployTasks() []domain.Task {
	return []domain.Task{
		{ID: "build", Name: "build", Status: domain.TaskPending, DependsOn: []string{}},
		{ID: "test", Name: "test", Status: domain.TaskPending, DependsOn: []string{"build"}},
		{ID: "deploy", Name: "deploy", Status: domain.TaskPending, DependsOn: []string{"test"}},
	}
}

func TestSubmitWorkflow_MarksInitialTasksReady(t *testing.T) {
	e := NewWorkflowEngine()
	ctx := newTestCtx()

	wf, err := domain.NewWorkflow("tenant1", "wf1", buildTestDeployTasks())
	assert.NoError(t, err)

	id, err := e.SubmitWorkflow(ctx, wf)
	assert.NoError(t, err)
	assert.Equal(t, wf.ID, id)

	assert.Equal(t, domain.TaskReady, wf.Tasks["build"].Status)
	assert.Equal(t, domain.TaskPending, wf.Tasks["test"].Status)
	assert.Equal(t, domain.TaskPending, wf.Tasks["deploy"].Status)
}

func TestOnTaskCompleted_PropagatesReadyToDependents(t *testing.T) {
	e := NewWorkflowEngine()
	ctx := newTestCtx()

	wf, err := domain.NewWorkflow("tenant1", "wf1", buildTestDeployTasks())
	assert.NoError(t, err)

	_, err = e.SubmitWorkflow(ctx, wf)
	assert.NoError(t, err)

	err = e.OnTaskCompleted(ctx, wf.ID, "build", domain.TaskResult{ExitCode: 0})
	assert.NoError(t, err)

	assert.Equal(t, domain.TaskSucceeded, wf.Tasks["build"].Status)
	assert.Equal(t, domain.TaskReady, wf.Tasks["test"].Status)
	assert.Equal(t, domain.TaskPending, wf.Tasks["deploy"].Status)

	err = e.OnTaskCompleted(ctx, wf.ID, "test", domain.TaskResult{ExitCode: 0})
	assert.NoError(t, err)

	assert.Equal(t, domain.TaskSucceeded, wf.Tasks["test"].Status)
	assert.Equal(t, domain.TaskReady, wf.Tasks["deploy"].Status)

	err = e.OnTaskCompleted(ctx, wf.ID, "deploy", domain.TaskResult{ExitCode: 0})
	assert.NoError(t, err)

	assert.Equal(t, domain.TaskSucceeded, wf.Tasks["deploy"].Status)
}

func TestOnTaskCompleted_WorkflowNotFound(t *testing.T) {
	e := NewWorkflowEngine()
	ctx := newTestCtx()

	err := e.OnTaskCompleted(ctx, "missing-wf", "build", domain.TaskResult{})
	assert.Error(t, err)
}

func TestOnTaskCompleted_TaskNotFound(t *testing.T) {
	e := NewWorkflowEngine()
	ctx := newTestCtx()

	wf, err := domain.NewWorkflow("tenant1", "wf1", buildTestDeployTasks())
	assert.NoError(t, err)

	_, err = e.SubmitWorkflow(ctx, wf)
	assert.NoError(t, err)

	err = e.OnTaskCompleted(ctx, wf.ID, "missing-task", domain.TaskResult{})
	assert.Error(t, err)
}
