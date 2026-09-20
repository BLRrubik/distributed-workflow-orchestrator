package service

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/client"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/models"
	er "github.com/blrrubik/distributed-workflow-orchestrator/pkg/executror_registry"
	wp "github.com/blrrubik/distributed-workflow-orchestrator/pkg/worker_pool"
)

type WorkerService struct {
	workerPool        *wp.WorkerPool
	executionRegistry *er.Registry
	clusterClient     *client.GRPCClusterClient
	unique            *unique
	log               *logger.Logger
	node              *domain.WorkerNode
	suspended         atomic.Bool // выставляется при провале heartbeat, снимается успешной перерегистрацией
}

func NewWorkerService(
	node *domain.WorkerNode,
	pool *wp.WorkerPool,
	executionRegistry *er.Registry,
	clusterClient *client.GRPCClusterClient,
	log *logger.Logger,
) *WorkerService {
	return &WorkerService{
		node:              node,
		workerPool:        pool,
		log:               log,
		executionRegistry: executionRegistry,
		clusterClient:     clusterClient,
		unique:            newUnique(),
	}
}

// Start регистрирует воркера в control-plane и только при успехе поднимает
// пул исполнителей и heartbeat-цикл. Незарегистрированный воркер не должен
// молча крутиться и принимать Dispatch — control-plane про него всё равно
// ничего не знает, задачи слать ему некому.
func (ws *WorkerService) Start(ctx context.Context) error {
	if err := ws.register(ctx); err != nil {
		return err
	}

	ws.workerPool.Start(ctx)

	go ws.heartbeatLoop(ctx)

	return nil
}

func (ws *WorkerService) register(ctx context.Context) error {
	resp, err := ws.clusterClient.Register(ctx, &protogen.RegisterRequest{
		WorkerId:     ws.node.ID,
		Address:      ws.node.Address,
		Labels:       ws.node.Labels,
		Capabilities: ws.node.Capabilities,
		Capacity:     int32(ws.node.Capacity),
	})
	if err != nil {
		return fmt.Errorf("register worker %s: %w", ws.node.ID, err)
	}

	if !resp.GetAccepted() {
		return fmt.Errorf("registration rejected: %s", resp.GetReason())
	}

	return nil
}

// DispatchTask идемпотентен относительно (workflowID, taskID): повторный Dispatch
// той же пары, пока задача ещё не доисполнилась успехом, — no-op (nil, не ошибка).
// Это важно: если вернуть ошибку, scheduler интерпретирует её как провал и может
// передиспатчить задачу на ДРУГОЙ воркер — тогда она реально выполнится дважды.
func (ws *WorkerService) DispatchTask(ctx context.Context, req *protogen.DispatchRequest) error {
	if ws.suspended.Load() {
		return errors.New("worker suspended: lost connection to control-plane")
	}

	key := req.GetTaskId() // TaskID генерируется control-plane (uuid), глобально уникален

	if !ws.unique.Add(key) {
		ws.log.Info("task already in flight, skipping duplicate dispatch",
			logger.String("task_id", req.GetTaskId()),
			logger.String("workflow_id", req.GetWorkflowId()),
		)

		return nil
	}

	executor, err := ws.executionRegistry.Get(req.GetType())
	if err != nil {
		return fmt.Errorf("get executor error: %w", err)
	}

	taskInfo := &models.TaskInfo{
		ID:         req.GetTaskId(),
		WorkflowID: req.GetWorkflowId(),
		Spec: domain.TaskSpec{
			Payload: req.GetPayload(),
		},
		TimeoutInSeconds: req.GetTimeoutSeconds(),
	}

	task := models.NewShellTask(
		taskInfo,
		executor,
		ws.clusterClient,
		ws.log,
	)

	tracked := &trackedTask{
		inner: task,
		setCancel: func(cancel context.CancelFunc) {
			ws.unique.SetCancel(key, cancel)
		},
		onDone: func(err error) {
			if err == nil {
				ws.unique.Remove(key)
			}
		},
	}

	if ok := ws.workerPool.TrySubmit(tracked); !ok {
		ws.unique.Remove(key)

		return errors.New("worker pool is full")
	}

	return nil
}

// CancelTask — обработчик WorkerRPC.CancelTask (best-effort остановка). false —
// не ошибка, а нормальный исход гонки: задача уже успела завершиться сама
// или никогда не исполнялась на этом воркере.
func (ws *WorkerService) CancelTask(taskID string) bool {
	return ws.unique.Cancel(taskID)
}

func (ws *WorkerService) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ws.sendHeartbeat(ctx)
		}
	}
}

// sendHeartbeat пингует control-plane. Провал пинга подвешивает воркера —
// DispatchTask начинает отклонять новые задачи — и тут же пробует
// перерегистрироваться: control-plane мог рестартовать и забыть про воркера,
// обычный heartbeat такому уже не поможет, нужен новый Register. Успешная
// перерегистрация сразу снимает suspend, не дожидаясь следующего тика.
func (ws *WorkerService) sendHeartbeat(ctx context.Context) {
	_, err := ws.clusterClient.Heartbeat(ctx, &protogen.HeartbeatRequest{
		WorkerId:     ws.node.ID,
		RunningTasks: ws.workerPool.BusyCount(),
	})
	if err == nil {
		return
	}

	ws.log.Error("heartbeat failed, suspending worker",
		logger.String("worker_id", ws.node.ID),
		logger.Error(err),
	)

	ws.suspended.Store(true)

	if regErr := ws.register(ctx); regErr != nil {
		ws.log.Error("re-registration failed",
			logger.String("worker_id", ws.node.ID),
			logger.Error(regErr),
		)

		return
	}

	ws.suspended.Store(false)

	ws.log.Info("worker re-registered successfully", logger.String("worker_id", ws.node.ID))
}
