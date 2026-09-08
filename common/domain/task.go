package domain

import (
	"time"

	appcontext "github.com/blrrubik/distributed-workflow-orchestrator/common/context"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
)

// Task — узел графа выполнения (DAG node).
type Task struct {
	ID           string
	WorkflowID   string
	Name         string
	DependsOn    []string // рёбра графа: этот таск ждёт завершения перечисленных
	Command      TaskSpec // что именно выполнять
	MaxRetries   int
	RetryBackoff time.Duration
	Timeout      time.Duration
	status       TaskStatus
	AssignedTo   string
	Attempt      int
	result       *TaskResult
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (t *Task) IsFinished() bool {
	return t.status == TaskSucceeded || t.status == TaskFailed || t.status == TaskCancelled
}

func (t *Task) AllDepsSucceeded(wf *Workflow) bool {
	for _, dep := range t.DependsOn {
		task, ok := wf.Tasks[dep]
		if !ok {
			continue
		}

		if !task.IsFinished() {
			return false
		}
	}

	return true
}

func (t *Task) UpdateStatus(ctx appcontext.AppContext, status TaskStatus) bool {
	if err := t.status.CanTransitTo(status); err != nil {
		ctx.GetLogger().Error(
			"update status failed",
			logger.String("task_id", t.ID),
			logger.String("from", string(t.status)),
			logger.String("to", string(status)),
			logger.Error(err),
		)

		return false
	}

	t.status = status

	return true
}

func (t *Task) GetStatus() TaskStatus {
	return t.status
}

func (t *Task) GetResult() (TaskResult, bool) {
	if t.result == nil {
		return TaskResult{}, false
	}

	return *t.result, true
}

func (t *Task) SetResult(result *TaskResult) {
	t.result = result
}

// TaskSpec — абстракция типа задачи.
type TaskSpec struct {
	Type    string            // "shell" | "http" | "webhook"
	Payload map[string]string // например {"cmd": "echo hello"} или {"url": "...", "method": "POST"}
}

type TaskResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Error    string
	Duration time.Duration
}
