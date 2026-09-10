package scheduler

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/orchestration"
)

type Scheduler struct {
	workerRegistry *orchestration.WorkerRegistry
	queue          *ReadyQueue
	log            *logger.Logger
}

func New(workerRegistry *orchestration.WorkerRegistry, log *logger.Logger) *Scheduler {
	return &Scheduler{
		workerRegistry: workerRegistry,
		queue:          NewReadyQueue(),
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

func (s *Scheduler) PushTask(task *domain.Task) {
	s.queue.Push(task)
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
	readyTasks := s.queue.PopBatch(50) // не пытаемся раздать всю очередь за один присест
	if len(readyTasks) == 0 {
		return
	}

	workers := s.workerRegistry.AliveWorkers() // снимок текущих живых воркеров

	for _, task := range readyTasks {
		workerID, err := s.selectWorker(workers)
		if err != nil {
			// нет подходящего воркера прямо сейчас (все заняты / нет с нужным label)
			s.queue.Push(task) // вернуть в очередь, попробуем на следующем тике
			continue
		}

		if err = s.commitAssignment(ctx, task, workerID); err != nil {
			s.queue.Push(task) // Raft-команда не прошла (например, потеряли лидерство прямо сейчас) — вернуть
			continue
		}

		// ЛОКАЛЬНО учитываем занятость воркера СРАЗУ, не дожидаясь его heartbeat —
		// иначе следующая задача в этом же батче может уйти туда же (см. §4 ниже)
		s.workerRegistry.IncrementRunning(workerID)
	}
}

func (s *Scheduler) commitAssignment(ctx context.Context, task *domain.Task, workerID string) error {
	s.log.Info("Committing assignment for task",
		logger.String("task", task.ID),
		logger.String("worker", workerID),
	)

	return nil
}
