package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
)

type WorkflowEngine struct {
	workflows map[string]*domain.Workflow
	log       *logger.Logger

	mu sync.RWMutex
}

func NewWorkflowEngine(log *logger.Logger) *WorkflowEngine {
	return &WorkflowEngine{
		workflows: make(map[string]*domain.Workflow),
		log:       log,
	}
}

// SubmitWorkflow вызывается из API. Валидирует DAG и сохраняет workflow.
func (e *WorkflowEngine) SubmitWorkflow(ctx context.Context, wf *domain.Workflow) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.log.Info("submitting workflow", logger.String("workflow_id", wf.ID))
	e.workflows[wf.ID] = wf

	e.markReadyTasks(ctx, wf)

	return wf.ID, nil
}

// OnTaskCompleted вызывается, когда Scheduler получил результат от воркера.
// Пересчитывает READY-множество для зависимых задач.
func (e *WorkflowEngine) OnTaskCompleted(ctx context.Context, workflowID, taskID string, result domain.TaskResult) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	wf, ok := e.workflows[workflowID]
	if !ok {
		return fmt.Errorf("workflow not found %s", workflowID)
	}

	task, ok := wf.Tasks[taskID]
	if !ok {
		return fmt.Errorf("task not found %s", taskID)
	}

	e.UpdateTaskStatus(ctx, task, domain.TaskDispatched)
	e.UpdateTaskStatus(ctx, task, domain.TaskRunning)

	if e.UpdateTaskStatus(ctx, task, domain.TaskSucceeded) {
		task.SetResult(&result)
		e.log.Info("task succeeded", logger.String("task", task.ID))
	}

	e.markReadyTasks(ctx, wf)
	e.finalizeWorkflowIfDone(ctx, wf)

	return nil
}

func (e *WorkflowEngine) UpdateTaskStatus(ctx context.Context, task *domain.Task, newStatus domain.TaskStatus) bool {
	// metrics there

	if err := task.UpdateStatus(newStatus); err != nil {
		e.log.Info(
			"workflow status was not changed",
			logger.String("task_id", task.ID),
			logger.String("status", newStatus.String()),
			logger.Error(err),
		)

		return true
	}

	return false
}

func (e *WorkflowEngine) UpdateWorkflowStatus(ctx context.Context, wf *domain.Workflow, newStatus domain.WorkflowStatus) bool {
	// metrics there
	if err := wf.UpdateStatus(newStatus); err != nil {
		e.log.Info(
			"workflow was not changed",
			logger.String("workflow_id", wf.ID),
			logger.String("status", newStatus.String()),
			logger.Error(err),
		)

		return true
	}

	return false
}

// markReadyTasks переводит задачи с выполненными зависимостями в статус READY и логирует переход.
func (e *WorkflowEngine) markReadyTasks(ctx context.Context, wf *domain.Workflow) {
	readyTasks := e.recomputeReadyTasks(wf)

	if len(readyTasks) > 0 && wf.GetStatus() == domain.WorkflowPending {
		e.UpdateWorkflowStatus(ctx, wf, domain.WorkflowRunning)
	}

	for _, readyTaskID := range readyTasks {
		readyTask, ok := wf.Tasks[readyTaskID]
		if !ok {
			e.log.Error("task not found by ready task", logger.String("task", readyTaskID))

			continue
		}

		if readyTask.GetStatus() == domain.TaskReady {
			continue
		}

		if e.UpdateTaskStatus(ctx, readyTask, domain.TaskReady) {
			e.log.Info("task ready", logger.String("task", readyTask.ID))
		}
	}
}

// recomputeReadyTasks — приватная функция: топологический пересчёт готовых к запуску задач.
func (e *WorkflowEngine) recomputeReadyTasks(wf *domain.Workflow) []string {
	readyTasks := make([]string, 0, len(wf.Tasks))

	for _, task := range wf.Tasks {
		if task.IsFinished() || !task.AllDepsSucceeded(wf) {
			continue
		}

		readyTasks = append(readyTasks, task.ID)
	}

	return readyTasks
}

// finalizeWorkflowIfDone проверяет, завершены ли все задачи графа, и переводит workflow
// в терминальный статус (SUCCEEDED, если ни одна задача не провалилась, иначе FAILED).
func (e *WorkflowEngine) finalizeWorkflowIfDone(ctx context.Context, wf *domain.Workflow) {
	if wf.IsFinished() || !wf.AllTasksFinished() {
		return
	}

	target := domain.WorkflowSucceeded
	if wf.HasFailedTask() {
		target = domain.WorkflowFailed
	}

	e.UpdateWorkflowStatus(ctx, wf, target)
}
