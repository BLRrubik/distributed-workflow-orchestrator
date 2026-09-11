package shell_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor/shell"
)

func TestExecutor_Execute_Success(t *testing.T) {
	e := &shell.Executor{}

	result, err := e.Execute(context.Background(), domain.TaskSpec{
		Payload: map[string]string{"command": "echo", "args": "hello"},
	}, time.Second)

	assert.NoError(t, err)
	assert.Equal(t, 0, result.ExitCode)
	assert.Equal(t, "hello\n", result.Stdout)
	assert.Empty(t, result.Error)
}

func TestExecutor_Execute_NonZeroExitCode(t *testing.T) {
	e := &shell.Executor{}

	result, err := e.Execute(context.Background(), domain.TaskSpec{
		Payload: map[string]string{"command": "false"},
	}, time.Second)

	assert.NoError(t, err)
	assert.Equal(t, 1, result.ExitCode)
}

func TestExecutor_Execute_CapturesStderr(t *testing.T) {
	e := &shell.Executor{}

	result, err := e.Execute(context.Background(), domain.TaskSpec{
		Payload: map[string]string{"command": "ls", "args": "/no-such-path-xyz"},
	}, time.Second)

	assert.NoError(t, err)
	assert.NotEqual(t, 0, result.ExitCode)
	assert.NotEmpty(t, result.Stderr)
}

func TestExecutor_Execute_MissingCommand(t *testing.T) {
	e := &shell.Executor{}

	result, err := e.Execute(context.Background(), domain.TaskSpec{
		Payload: map[string]string{},
	}, time.Second)

	assert.Error(t, err)
	assert.Equal(t, "command not found in task spec", result.Error)
}

func TestExecutor_Execute_BinaryNotFound(t *testing.T) {
	e := &shell.Executor{}

	_, err := e.Execute(context.Background(), domain.TaskSpec{
		Payload: map[string]string{"command": "no-such-binary-xyz"},
	}, time.Second)

	assert.Error(t, err)
}

func TestExecutor_Execute_Timeout(t *testing.T) {
	e := &shell.Executor{}

	result, err := e.Execute(context.Background(), domain.TaskSpec{
		Payload: map[string]string{"command": "sleep", "args": "2"},
	}, 50*time.Millisecond)

	assert.NoError(t, err)
	assert.Equal(t, -1, result.ExitCode)
	assert.Equal(t, "task timed out", result.Error)
}

func TestExecutor_Execute_MeasuresDuration(t *testing.T) {
	e := &shell.Executor{}

	result, err := e.Execute(context.Background(), domain.TaskSpec{
		Payload: map[string]string{"command": "echo", "args": "hi"},
	}, time.Second)

	assert.NoError(t, err)
	assert.GreaterOrEqual(t, result.Duration, time.Duration(0))
}
