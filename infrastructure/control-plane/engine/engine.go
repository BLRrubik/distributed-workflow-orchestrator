package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/scheduler"
)

// WorkerNotifier — то немногое, что engine нужно от сети, чтобы остановить
// реально исполняющиеся задачи на воркере при отмене. *client.GRPCWorkerClient
// удовлетворяет этому интерфейсу неявно.
type WorkerNotifier interface {
	CancelTasks(ctx context.Context, workerID string, taskIDs []string) (*protogen.CancelTasksResponse, error)
}

type WorkflowEngine struct {
	workflows      map[string]*domain.Workflow
	scheduler      *scheduler.Scheduler
	workerNotifier WorkerNotifier
	log            *logger.Logger

	mu sync.RWMutex
}

func NewWorkflowEngine(log *logger.Logger, scheduler *scheduler.Scheduler, workerNotifier WorkerNotifier) *WorkflowEngine {
	return &WorkflowEngine{
		workflows:      make(map[string]*domain.Workflow),
		scheduler:      scheduler,
		workerNotifier: workerNotifier,
		log:            log,
	}
}

func (e *WorkflowEngine) GetWorkflow(workflowID string) (*domain.Workflow, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	workflow, ok := e.workflows[workflowID]
	if !ok {
		return nil, fmt.Errorf("workflow %s not found", workflowID)
	}

	return workflow, nil
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

			if result.Status == domain.TaskFailed {
				e.cancelDownstream(ctx, wf, task.ID)
			}
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

		return false
	}

	return true
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

		return false
	}

	return true
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

		if e.UpdateTaskStatus(ctx, readyTask, domain.TaskReady) {
			e.log.Info("task ready", logger.String("task", readyTask.ID))
		}

		e.pushJob(readyTask)
	}
}

func (e *WorkflowEngine) pushJob(task *domain.Task) {
	e.scheduler.PushTask(scheduler.Job{
		ID:         task.ID,
		WorkflowID: task.WorkflowID,
		Request: &protogen.DispatchRequest{
			TaskId:         task.ID,
			WorkflowId:     task.WorkflowID,
			Type:           task.Spec.Type,
			Payload:        task.Spec.Payload,
			TimeoutSeconds: int64(task.Timeout.Seconds()),
		},
		OnDispatched: func(ctx context.Context, workerID string) {
			e.OnTaskDispatched(ctx, task, workerID)
		},
	})
}

// RunningTaskRef — пара (задача, воркер), достаточная, чтобы разослать CancelTask.
type RunningTaskRef struct {
	TaskID   string
	WorkerID string
}

// CancelWorkflow — см. §4.1 docs/about.md. Помечает CANCELLED все нетерминальные
// задачи workflow синхронно (источник истины), затем best-effort уведомляет
// воркеров, у кого реально исполнялись (DISPATCHED/RUNNING) задачи этого
// workflow — сгруппировав по воркеру и отправив каждому один батч-запрос,
// а не по вызову на задачу. Уведомление не блокирует ответ этого метода.
func (e *WorkflowEngine) CancelWorkflow(ctx context.Context, workflowID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	wf, ok := e.workflows[workflowID]
	if !ok {
		return fmt.Errorf("workflow not found %s", workflowID)
	}

	var running []RunningTaskRef
	for _, task := range wf.Tasks {
		if task.IsFinished() {
			continue
		}

		if status := task.GetStatus(); status == domain.TaskDispatched || status == domain.TaskRunning {
			running = append(running, RunningTaskRef{TaskID: task.ID, WorkerID: task.AssignedTo})
		}

		e.UpdateTaskStatus(ctx, task, domain.TaskCancelled)
	}

	e.UpdateWorkflowStatus(ctx, wf, domain.WorkflowCancelled)

	e.notifyWorkersOfCancellation(running)

	return nil
}

// CancelTask — см. §4.4 docs/about.md. Отменяет ОДНУ задачу (и каскадом — всё,
// что от неё зависит), не трогая независимые ветки того же workflow.
func (e *WorkflowEngine) CancelTask(ctx context.Context, taskID string) error {
	e.mu.Lock()

	wf, task, ok := e.findTask(taskID)
	if !ok {
		e.mu.Unlock()

		return fmt.Errorf("task not found %s", taskID)
	}

	if task.IsFinished() {
		e.mu.Unlock()

		return fmt.Errorf("task %s already terminal: %s", taskID, task.GetStatus())
	}

	var running []RunningTaskRef
	if status := task.GetStatus(); status == domain.TaskDispatched || status == domain.TaskRunning {
		running = append(running, RunningTaskRef{TaskID: task.ID, WorkerID: task.AssignedTo})
	}

	e.UpdateTaskStatus(ctx, task, domain.TaskCancelled)
	running = append(running, e.cancelDownstream(ctx, wf, task.ID)...)
	e.finalizeWorkflowIfDone(ctx, wf)

	e.mu.Unlock()

	e.notifyWorkersOfCancellation(running)

	return nil
}

// findTask ищет задачу по ID среди всех workflow. O(число workflow * задач в
// нём) — на этом этапе (до Раздела 5, до FSM) engine не держит глобальный
// индекс taskID -> workflow, а число активных workflow невелико.
func (e *WorkflowEngine) findTask(taskID string) (*domain.Workflow, *domain.Task, bool) {
	for _, wf := range e.workflows {
		if task, ok := wf.Tasks[taskID]; ok {
			return wf, task, true
		}
	}

	return nil, nil, false
}

// notifyWorkersOfCancellation — общий best-effort рассыльщик WorkerRPC.CancelTasks,
// сгруппированный по воркеру: каждому воркеру уходит один батч-запрос со всеми
// его задачами, а не по вызову на задачу.
func (e *WorkflowEngine) notifyWorkersOfCancellation(refs []RunningTaskRef) {
	if len(refs) == 0 || e.workerNotifier == nil {
		return
	}

	byWorker := make(map[string][]string)
	for _, ref := range refs {
		byWorker[ref.WorkerID] = append(byWorker[ref.WorkerID], ref.TaskID)
	}

	for workerID, taskIDs := range byWorker {
		go func(workerID string, taskIDs []string) {
			cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if _, err := e.workerNotifier.CancelTasks(cancelCtx, workerID, taskIDs); err != nil {
				e.log.Warn("failed to notify worker about cancellation",
					logger.String("worker", workerID),
					logger.Error(err),
				)
			}
		}(workerID, taskIDs)
	}
}

// ReassignDeadWorkerTasks — хук на WorkerRegistry.OnWorkerDead: задачи, которые
// висели на мёртвом воркере в DISPATCHED/RUNNING, возвращаются в READY и уходят
// в scheduler заново. Не трогает Attempt/MaxRetries — воркер умер не по вине
// задачи, а её собственный retry-бюджет остаётся нетронутым для реальных фейлов
// исполнения. At-least-once: если воркер на самом деле жив (false positive по
// heartbeat) и всё ещё выполняет задачу, возможен параллельный дубль-запуск —
// осознанно принято, без fencing token.
func (e *WorkflowEngine) ReassignDeadWorkerTasks(workerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, wf := range e.workflows {
		for _, task := range wf.Tasks {
			if task.AssignedTo != workerID {
				continue
			}

			status := task.GetStatus()
			if status != domain.TaskDispatched && status != domain.TaskRunning {
				continue
			}

			task.AssignedTo = ""

			if !e.UpdateTaskStatus(context.Background(), task, domain.TaskReady) {
				continue
			}

			e.log.Warn("task reassigned due to dead worker",
				logger.String("task", task.ID),
				logger.String("worker", workerID),
			)

			e.pushJob(task)
		}
	}
}

// cancelDownstream рекурсивно отменяет ещё не запущенные задачи, зависящие
// (прямо или транзитивно) от упавшей rootID — без этого их AllDepsSucceeded
// никогда не станет true, а значит workflow никогда не дойдёт до AllTasksFinished
// и зависнет в RUNNING навсегда. Возвращает те из отменённых, что были
// DISPATCHED/RUNNING — вызывающая сторона уведомит их воркеров.
func (e *WorkflowEngine) cancelDownstream(ctx context.Context, wf *domain.Workflow, rootID string) []RunningTaskRef {
	var running []RunningTaskRef
	queue := []string{rootID}

	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]

		for _, t := range wf.Tasks {
			if t.IsFinished() {
				continue
			}

			for _, dep := range t.DependsOn {
				if dep != id {
					continue
				}

				if status := t.GetStatus(); status == domain.TaskDispatched || status == domain.TaskRunning {
					running = append(running, RunningTaskRef{TaskID: t.ID, WorkerID: t.AssignedTo})
				}

				if e.UpdateTaskStatus(ctx, t, domain.TaskCancelled) {
					e.log.Info("task cancelled due to failed dependency",
						logger.String("task", t.ID),
						logger.String("failed_dependency", id),
					)

					queue = append(queue, t.ID)
				}

				break
			}
		}
	}

	return running
}

// recomputeReadyTasks — приватная функция: топологический пересчёт готовых к запуску задач.
func (e *WorkflowEngine) recomputeReadyTasks(wf *domain.Workflow) []string {
	readyTasks := make([]string, 0, len(wf.Tasks))

	for _, task := range wf.Tasks {
		if !task.IsReady() || !task.AllDepsSucceeded(wf) {
			continue
		}

		readyTasks = append(readyTasks, task.ID)
	}

	return readyTasks
}

// finalizeWorkflowIfDone проверяет, завершены ли все задачи графа, и переводит workflow
// в терминальный статус: FAILED, если реально провалилась хоть одна задача (приоритет
// выше отмены — см. §4.2 about.md), иначе CANCELLED, если хоть одна отменена, иначе SUCCEEDED.
func (e *WorkflowEngine) finalizeWorkflowIfDone(ctx context.Context, wf *domain.Workflow) {
	if wf.IsFinished() || !wf.AllTasksFinished() {
		return
	}

	target := domain.WorkflowSucceeded

	switch {
	case wf.HasFailedTask():
		target = domain.WorkflowFailed
	case wf.HasCancelledTask():
		target = domain.WorkflowCancelled
	}

	e.UpdateWorkflowStatus(ctx, wf, target)
}
