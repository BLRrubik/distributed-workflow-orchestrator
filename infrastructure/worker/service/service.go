package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/models"
	wp "github.com/blrrubik/distributed-workflow-orchestrator/pkg/worker_pool"
)

type WorkerService struct {
	workerPool *wp.WorkerPool
	executors  *executor.Executors
	log        *logger.Logger
}

func NewWorkerService(
	ctx context.Context,
	pool *wp.WorkerPool,
	executors *executor.Executors,
	log *logger.Logger,
) *WorkerService {
	ws := &WorkerService{
		workerPool: pool,
		log:        log,
		executors:  executors,
	}

	ws.workerPool.Start(ctx)

	return ws
}

func (ws *WorkerService) DispatchTask(ctx context.Context, req *protogen.DispatchRequest) error {
	task, err := ws.getExecutionTask(req)
	if err != nil {
		return fmt.Errorf("could not find executor for task type %s", req.GetType())
	}

	if ok := ws.workerPool.TrySubmit(task); !ok {
		return errors.New("worker pool is full")
	}

	return nil
}

func (ws *WorkerService) getExecutionTask(req *protogen.DispatchRequest) (wp.Task, error) {
	taskInfo := &models.TaskInfo{
		ID: req.GetTaskId(),
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
			ws.log,
		), nil
	default:
		return nil, errors.New("executor type not supported")
	}
}
