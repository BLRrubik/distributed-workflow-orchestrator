package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/scheduler"
)

type WorkflowEngine struct {
	workflows map[string]*domain.Workflow
	scheduler *scheduler.Scheduler
	log       *logger.Logger

	mu sync.RWMutex
}

func NewWorkflowEngine(log *logger.Logger, scheduler *scheduler.Scheduler) *WorkflowEngine {
	return &WorkflowEngine{
		workflows: make(map[string]*domain.Workflow),
		scheduler: scheduler,
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

// OnTaskResponse обрабатывает хук о статусе задачи от воркера.
func (e *WorkflowEngine) OnTaskResponse(ctx context.Context, workflowID, taskID string, result domain.TaskResult) error {
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

	switch result.Status {
	case domain.TaskFailed, domain.TaskSucceeded:
		if e.UpdateTaskStatus(ctx, task, result.Status) {
			task.SetResult(&result)
			e.log.Info("task status updated",
				logger.String("status", result.Status.String()),
				logger.String("task", task.ID),
			)
		}
	case domain.TaskRunning:
		if e.UpdateTaskStatus(ctx, task, domain.TaskRunning) {
			e.log.Info("task running", logger.String("task", task.ID))
		}
	}

	e.markReadyTasks(ctx, wf)
	e.finalizeWorkflowIfDone(ctx, wf)

	return nil
}

// OnTaskDispatched — колбэк scheduler'а: вызывается после того, как задача
// успешно ушла воркеру по gRPC. Берёт e.mu сам — вызывается из горутины
// scheduler.Run, а не из-под уже захваченного замка engine.
func (e *WorkflowEngine) OnTaskDispatched(ctx context.Context, task *domain.Task, workerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	task.AssignedTo = workerID

	if e.UpdateTaskStatus(ctx, task, domain.TaskDispatched) {
		e.log.Info("task dispatched",
			logger.String("task", task.ID),
			logger.String("worker", workerID),
		)
	}
}

func (e *WorkflowEngine) UpdateTaskStatus(ctx context.Context, task *domain.Task, newStatus domain.TaskStatus) bool {
	// metrics there
	if err := task.UpdateStatus(newStatus); err != nil {
		e.log.Info(
			"task status was not changed",
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

		e.scheduler.PushTask(scheduler.Job{
			ID:         readyTask.ID,
			WorkflowID: readyTask.WorkflowID,
			Request: &protogen.DispatchRequest{
				TaskId:         readyTask.ID,
				WorkflowId:     readyTask.WorkflowID,
				Type:           readyTask.Spec.Type,
				Payload:        readyTask.Spec.Payload,
				TimeoutSeconds: int64(readyTask.Timeout.Seconds()),
			},
			OnDispatched: func(ctx context.Context, workerID string) {
				e.OnTaskDispatched(ctx, readyTask, workerID)
			},
		})
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
