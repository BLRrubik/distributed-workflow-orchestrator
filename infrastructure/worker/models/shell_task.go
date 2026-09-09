package models

import (
	"context"
	"fmt"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor"
)

const retryBackoff = 2 * time.Second

type ShellTask struct {
	task *TaskInfo
	exec executor.Executor
	log  *logger.Logger
}

func NewShellTask(task *TaskInfo, exec executor.Executor, log *logger.Logger) *ShellTask {
	return &ShellTask{
		task: task,
		exec: exec,
		log:  log,
	}
}

// Do использует ctx жизненного цикла пула (а не запроса) — задача может
// исполниться спустя минуты после диспатча, когда исходный gRPC-запрос уже завершится.
func (t *ShellTask) Do(ctx context.Context) error {
	timeout := time.Duration(t.task.TimeoutInSeconds) * time.Second

	result, err := t.exec.Execute(ctx, t.task.Spec, timeout)
	if err != nil {
		t.log.Error("task execution failed",
			logger.String("task_id", t.task.ID),
			logger.Error(err),
		)

		return fmt.Errorf("execute task %s: %w", t.task.ID, err)
	}

	t.log.Info("task executed",
		logger.String("task_id", t.task.ID),
		logger.Any("result", result),
	)

	return nil
}

func (t *ShellTask) GetWaitDuration() time.Duration {
	return retryBackoff
}
