package engine

import (
	"fmt"
	"sync"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/context"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
)

type WorkflowEngine struct {
	workflows map[string]*domain.Workflow

	mu sync.RWMutex
}

func NewWorkflowEngine() *WorkflowEngine {
	return &WorkflowEngine{
		workflows: make(map[string]*domain.Workflow),
	}
}

// SubmitWorkflow вызывается из API. Валидирует DAG и сохраняет workflow.
func (e *WorkflowEngine) SubmitWorkflow(ctx context.AppContext, wf *domain.Workflow) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	ctx.GetLogger().Info("submitting workflow", logger.String("workflow_id", wf.ID))
	e.workflows[wf.ID] = wf

	e.markReadyTasks(ctx, wf)

	return wf.ID, nil
}

// OnTaskCompleted вызывается, когда Scheduler получил результат от воркера.
// Пересчитывает READY-множество для зависимых задач.
func (e *WorkflowEngine) OnTaskCompleted(ctx context.AppContext, workflowID, taskID string, result domain.TaskResult) error {
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

	task.Status = domain.TaskSucceeded
	task.Result = &result

	ctx.GetLogger().Info("task succeeded", logger.String("task", task.ID))

	e.markReadyTasks(ctx, wf)

	return nil
}

// markReadyTasks переводит задачи с выполненными зависимостями в статус READY и логирует переход.
func (e *WorkflowEngine) markReadyTasks(ctx context.AppContext, wf *domain.Workflow) {
	for _, readyTaskID := range e.recomputeReadyTasks(wf) {
		readyTask, ok := wf.Tasks[readyTaskID]
		if !ok {
			ctx.GetLogger().Error("task not found by ready task", logger.String("task", readyTaskID))

			continue
		}

		if readyTask.Status == domain.TaskReady {
			continue
		}

		readyTask.Status = domain.TaskReady

		ctx.GetLogger().Info("task ready", logger.String("task", readyTask.ID))
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
