package models

import (
	"context"
	"fmt"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/client"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor"
)

const retryBackoff = 2 * time.Second

type ShellTask struct {
	task          *TaskInfo
	exec          executor.Executor
	clusterClient *client.GRPCClusterClient
	log           *logger.Logger
}

func NewShellTask(
	task *TaskInfo,
	exec executor.Executor,
	clusterClient *client.GRPCClusterClient,
	log *logger.Logger,
) *ShellTask {
	return &ShellTask{
		task:          task,
		exec:          exec,
		log:           log,
		clusterClient: clusterClient,
	}
}

// Do использует ctx жизненного цикла пула (а не запроса) — задача может
// исполниться спустя минуты после диспатча, когда исходный gRPC-запрос уже завершится.
func (t *ShellTask) Do(ctx context.Context) error {
	t.sendStatus(ctx, protogen.TaskReportStatus_TASK_REPORT_RUNNING, nil)

	timeout := time.Duration(t.task.TimeoutInSeconds) * time.Second

	result, err := t.exec.Execute(ctx, t.task.Spec, timeout)
	if err != nil {
		t.log.Error("task execution failed",
			logger.String("task_id", t.task.ID),
			logger.Error(err),
		)

		return fmt.Errorf("execute task %s: %w", t.task.ID, err)
	}

	status := protogen.TaskReportStatus_TASK_REPORT_SUCCEEDED
	if result.ExitCode != 0 || len(result.Error) != 0 {
		status = protogen.TaskReportStatus_TASK_REPORT_FAILED
	}

	t.sendStatus(ctx, status, &result)

	t.log.Info("task executed",
		logger.String("task_id", t.task.ID),
		logger.String("workflow", t.task.WorkflowID),
	)

	return nil
}

func (t *ShellTask) GetWaitDuration() time.Duration {
	return retryBackoff
}

func (t *ShellTask) sendStatus(ctx context.Context, status protogen.TaskReportStatus, taskResult *domain.TaskResult) {
	result := &protogen.ResultRequest{
		Status:     status,
		WorkflowId: t.task.WorkflowID,
		TaskId:     t.task.ID,
	}

	if taskResult != nil {
		result.ExitCode = int32(taskResult.ExitCode)
		result.Error = taskResult.Error
		result.Stdout = taskResult.Stdout
		result.Stderr = taskResult.Stderr
	}

	if _, err := t.clusterClient.ReportResult(ctx, result); err != nil {
		t.log.Error("report result failed",
			logger.String("task_id", t.task.ID),
			logger.String("workflow_id", t.task.WorkflowID),
			logger.Error(err),
		)
	}
}
