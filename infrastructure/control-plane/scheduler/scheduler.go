package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/client"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/orchestration"
)

// Job — единица работы для Scheduler'а: готовый к отправке запрос и то, что
// сделать после успешного Dispatch. Scheduler ничего не знает про domain.Task —
// только про транспортный контракт (protogen) и голые функции; вся мутация
// доменной модели (статус, метрики) остаётся на стороне вызывающего (engine).
type Job struct {
	ID           string
	WorkflowID   string
	Request      *protogen.DispatchRequest
	OnDispatched func(ctx context.Context, workerID string)
}

type Scheduler struct {
	workerRegistry *orchestration.WorkerRegistry
	workerClient   *client.GRPCWorkerClient
	queue          *ReadyQueue
	log            *logger.Logger
}

func New(
	workerRegistry *orchestration.WorkerRegistry,
	workerClient *client.GRPCWorkerClient,
	log *logger.Logger,
) *Scheduler {
	return &Scheduler{
		workerRegistry: workerRegistry,
		queue:          NewReadyQueue(),
		workerClient:   workerClient,
		log:            log,
	}
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.assignOnce(ctx)
		}
	}
}

func (s *Scheduler) PushTask(job Job) {
	s.queue.Push(job)
}

// selectWorker — алгоритм подбора. Начните с простого, усложняйте по мере роста требований.
func (s *Scheduler) selectWorker(workers []domain.WorkerNode) (string, error) {
	if len(workers) == 0 {
		return "", errors.New("empty workers")
	}

	sort.Slice(workers, func(i, j int) bool {
		w1 := workers[i]
		w2 := workers[j]

		// +1 в знаменателе — иначе свежий воркер с RunningTasks == 0 роняет
		// планировщик паникой на делении на ноль
		return w1.Capacity/(w1.RunningTasks+1) >= w2.Capacity/(w2.RunningTasks+1)
	})

	return workers[0].ID, nil
}

func (s *Scheduler) assignOnce(ctx context.Context) {
	readyJobs := s.queue.PopBatch(50) // не пытаемся раздать всю очередь за один присест
	if len(readyJobs) == 0 {
		return
	}

	workers := s.workerRegistry.AliveWorkers() // снимок текущих живых воркеров

	for _, job := range readyJobs {
		workerID, err := s.selectWorker(workers)
		if err != nil {
			// нет подходящего воркера прямо сейчас (все заняты / нет с нужным label)
			s.queue.Push(job) // вернуть в очередь, попробуем на следующем тике
			continue
		}

		if err = s.commitAssignment(ctx, job, workerID); err != nil {
			s.queue.Push(job) // Raft-команда не прошла (например, потеряли лидерство прямо сейчас) — вернуть
			continue
		}

		// ЛОКАЛЬНО учитываем занятость воркера СРАЗУ, не дожидаясь его heartbeat —
		// иначе следующая задача в этом же батче может уйти туда же (см. §4 ниже)
		s.workerRegistry.IncrementRunning(workerID)
	}
}

func (s *Scheduler) commitAssignment(ctx context.Context, job Job, workerID string) error {
	s.log.Info("committing assignment for task",
		logger.String("task", job.ID),
		logger.String("worker", workerID),
	)

	if _, err := s.workerClient.Dispatch(ctx, workerID, job.Request); err != nil {
		return fmt.Errorf("failed to commit assignment for task %s: %w", job.ID, err)
	}

	if job.OnDispatched == nil {
		s.log.Error("no dispatch function for task",
			logger.String("task", job.ID),
			logger.String("workflow", job.WorkflowID),
		)

		return fmt.Errorf("task %s has no OnDispatched", job.ID)
	}

	job.OnDispatched(ctx, workerID)

	s.log.Info("assigned task",
		logger.String("task", job.ID),
		logger.String("worker", workerID),
	)

	return nil
}
