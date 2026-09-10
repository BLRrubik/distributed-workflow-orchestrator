package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/client"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/models"
	wp "github.com/blrrubik/distributed-workflow-orchestrator/pkg/worker_pool"
)

type WorkerService struct {
	workerPool    *wp.WorkerPool
	executors     *executor.Executors
	clusterClient *client.GRPCClusterClient
	log           *logger.Logger

	mu       sync.Mutex
	inFlight map[string]struct{} // ключ workflowID+"/"+taskID — защита от повторного Dispatch, пока задача не завершилась успехом
}

func NewWorkerService(
	pool *wp.WorkerPool,
	executors *executor.Executors,
	clusterClient *client.GRPCClusterClient,
	log *logger.Logger,
) *WorkerService {
	return &WorkerService{
		workerPool:    pool,
		log:           log,
		executors:     executors,
		clusterClient: clusterClient,
		inFlight:      make(map[string]struct{}),
	}
}

// Start регистрирует воркера в control-plane и только при успехе поднимает
// пул исполнителей и heartbeat-цикл. Незарегистрированный воркер не должен
// молча крутиться и принимать Dispatch — control-plane про него всё равно
// ничего не знает, задачи слать ему некому.
func (ws *WorkerService) Start(ctx context.Context, node *domain.WorkerNode) error {
	resp, err := ws.clusterClient.Register(ctx, &protogen.RegisterRequest{
		WorkerId: node.ID,
		Address:  node.Address,
		Labels:   node.Labels,
		Capacity: int32(node.Capacity),
	})
	if err != nil {
		return fmt.Errorf("register worker %s: %w", node.ID, err)
	}

	if !resp.GetAccepted() {
		return fmt.Errorf("registration rejected: %s", resp.GetReason())
	}

	ws.workerPool.Start(ctx)

	go ws.heartbeatLoop(ctx, node.ID)

	return nil
}

// DispatchTask идемпотентен относительно (workflowID, taskID): повторный Dispatch
// той же пары, пока задача ещё не доисполнилась успехом, — no-op (nil, не ошибка).
// Это важно: если вернуть ошибку, scheduler интерпретирует её как провал и может
// передиспатчить задачу на ДРУГОЙ воркер — тогда она реально выполнится дважды.
func (ws *WorkerService) DispatchTask(ctx context.Context, req *protogen.DispatchRequest) error {
	key := inFlightKey(req.GetWorkflowId(), req.GetTaskId())

	if !ws.markInFlight(key) {
		ws.log.Info("task already in flight, skipping duplicate dispatch",
			logger.String("task_id", req.GetTaskId()),
			logger.String("workflow_id", req.GetWorkflowId()),
		)

		return nil
	}

	task, err := ws.getExecutionTask(req)
	if err != nil {
		ws.clearInFlight(key)

		return fmt.Errorf("could not find executor for task type %s", req.GetType())
	}

	if ok := ws.workerPool.TrySubmit(&dedupTask{inner: task, key: key, clear: ws.clearInFlight}); !ok {
		ws.clearInFlight(key)

		return errors.New("worker pool is full")
	}

	return nil
}

func inFlightKey(workflowID, taskID string) string {
	return workflowID + "/" + taskID
}

// markInFlight возвращает false, если ключ уже занят (дубликат).
func (ws *WorkerService) markInFlight(key string) bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if _, ok := ws.inFlight[key]; ok {
		return false
	}

	ws.inFlight[key] = struct{}{}

	return true
}

func (ws *WorkerService) clearInFlight(key string) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	delete(ws.inFlight, key)
}

// dedupTask снимает ключ из inFlight только при успехе. WorkerPool на ошибке
// перекладывает ТОТ ЖЕ Job в retry (см. moveToRetry) — снимать ключ на каждой
// попытке нельзя: в окне между провалом и подхватом ретраем туда мог бы
// проскочить дубликат-диспатч и создать вторую параллельную копию задачи.
type dedupTask struct {
	inner wp.Task
	key   string
	clear func(string)
}

func (d *dedupTask) Do(ctx context.Context) error {
	err := d.inner.Do(ctx)
	if err == nil {
		d.clear(d.key)
	}

	return err //nolint:wrapcheck // прозрачная обёртка: инкапсулированная задача уже сама оборачивает свои ошибки
}

func (d *dedupTask) GetWaitDuration() time.Duration {
	return d.inner.GetWaitDuration()
}

func (ws *WorkerService) heartbeatLoop(ctx context.Context, workerID string) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, err := ws.clusterClient.Heartbeat(ctx, &protogen.HeartbeatRequest{
				WorkerId:     workerID,
				RunningTasks: ws.workerPool.BusyCount(),
			})
			if err != nil {
				ws.log.Error("heartbeat failed",
					logger.String("worker_id", workerID),
					logger.Error(err),
				)
			}
		}
	}
}

func (ws *WorkerService) getExecutionTask(req *protogen.DispatchRequest) (wp.Task, error) {
	taskInfo := &models.TaskInfo{
		ID:         req.GetTaskId(),
		WorkflowID: req.GetWorkflowId(),
		Spec: domain.TaskSpec{
			Payload: req.GetPayload(),
		},
		TimeoutInSeconds: req.GetTimeoutSeconds(),
	}

	switch req.GetType() {
	case "shell":
		return models.NewShellTask(
			taskInfo,
			ws.executors.Shell,
			ws.clusterClient,
			ws.log,
		), nil
	default:
		return nil, errors.New("executor type not supported")
	}
}
