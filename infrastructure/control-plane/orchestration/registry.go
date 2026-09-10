package orchestration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	rq "github.com/blrrubik/distributed-workflow-orchestrator/pkg/retry_queue"
)

type WorkerRegistry struct {
	mu  sync.RWMutex
	log *logger.Logger

	workers     map[string]*domain.WorkerNode // ключ — WorkerNode.ID
	retryQueue  *rq.RetryQueue                // pkg/retryqueue — health-check по dead man's switch
	onDeadHooks []func(workerID string)
}

func NewWorkerRegistry(log *logger.Logger) *WorkerRegistry {
	return &WorkerRegistry{
		workers:    make(map[string]*domain.WorkerNode),
		retryQueue: rq.NewRetryQueue(),
		log:        log,
	}
}

// Register — обработчик ClusterService.Register (§6.2).
func (r *WorkerRegistry) Register(w domain.WorkerNode) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.workers[w.ID]; ok {
		return fmt.Errorf("worker %s already exists", w.ID)
	}

	r.workers[w.ID] = &w

	r.retryQueue.Push(rq.NewCommonRetryTrigger(
		w.ID,
		15*time.Second, // heartbeatGracePeriod
		func(ctx context.Context) bool {
			worker := r.workers[w.ID]
			if time.Since(worker.LastHeartbeat) > 15*time.Second {
				r.markDead(w.ID)

				return false
			}

			return true
		},
		func(ctx context.Context) {
			r.log.Info("checking worker heartbeat %s",
				logger.String("worker", w.ID),
			)
		},
	))

	return nil
}

// OnWorkerDead — регистрация колбэка.
func (r *WorkerRegistry) OnWorkerDead(hook func(workerID string)) {
	r.onDeadHooks = append(r.onDeadHooks, hook)
}

// Heartbeat — обработчик ClusterService.Heartbeat (§6.2).
func (r *WorkerRegistry) Heartbeat(workerID string, runningTasks int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	wnode, ok := r.workers[workerID]
	if !ok {
		return fmt.Errorf("worker %s not found", workerID)
	}

	wnode.RunningTasks = runningTasks
	wnode.LastHeartbeat = time.Now()

	return nil
}

func (r *WorkerRegistry) AliveWorkers() []domain.WorkerNode {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var workers []domain.WorkerNode
	for _, w := range r.workers {
		if !w.IsReady() {
			continue
		}

		workers = append(workers, *w)
	}

	return workers
}

func (r *WorkerRegistry) IncrementRunning(workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	wnode, ok := r.workers[workerID]
	if !ok {
		return
	}

	wnode.RunningTasks++
}

func (r *WorkerRegistry) markDead(workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	wnode, ok := r.workers[workerID]
	if !ok {
		return
	}

	wnode.SetDead()

	for _, hook := range r.onDeadHooks {
		hook(workerID)
	}
}

func (r *WorkerRegistry) AddressOf(workerID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	wnode, ok := r.workers[workerID]
	if !ok {
		return ""
	}

	return wnode.Address
}
