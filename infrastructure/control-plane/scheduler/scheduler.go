package scheduler

import (
	"context"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
)

type Scheduler struct {
}

// Assign — вызывается по таймеру/событию. Забирает задачи из очереди READY
// и подбирает воркер под каждую.
func (s *Scheduler) Assign(ctx context.Context) error {
	return nil
}

// SelectWorker — алгоритм подбора. Начните с простого, усложняйте по мере роста требований.
func (s *Scheduler) SelectWorker(task domain.Task) (string, error) {
	return "", nil
}
