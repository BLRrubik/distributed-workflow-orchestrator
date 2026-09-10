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

const heartbeatGracePeriod = 15 * time.Second

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

// Run запускает dead man's switch (health-check по heartbeat). Без вызова этого
// метода триггеры, поставленные в Register, никогда не исполняются.
func (r *WorkerRegistry) Run(ctx context.Context) {
	r.retryQueue.RunLoop(ctx)
}

// Register — обработчик ClusterService.Register (§6.2). Идемпотентен: повторная
// регистрация уже известного ID (например, воркер передеплоился и переподнялся
// с тем же ID) не ошибка — обновляем данные и возвращаем воркера в ALIVE.
// Иначе воркер после рестарта никогда не смог бы зарегистрироваться заново.
func (r *WorkerRegistry) Register(w domain.WorkerNode) error {
	r.mu.Lock()

	if existing, ok := r.workers[w.ID]; ok {
		existing.Address = w.Address
		existing.Labels = w.Labels
		existing.Capacity = w.Capacity
		existing.LastHeartbeat = time.Now()
		existing.SetAlive()
	} else {
		w.LastHeartbeat = time.Now()
		r.workers[w.ID] = &w
	}

	r.mu.Unlock()

	r.log.Info("worker was registered successfully",
		logger.String("id", w.ID),
		logger.String("address", w.Address),
	)

	// retryQueue.Push идемпотентен по ключу (worker ID) — при повторной
	// регистрации не заведёт второй параллельный триггер поверх уже активного.
	r.retryQueue.Push(rq.NewCommonRetryTrigger(
		w.ID,
		heartbeatGracePeriod,
		func(ctx context.Context) bool {
			return r.checkHeartbeat(w.ID)
		},
		func(ctx context.Context) {
			r.log.Debug("checking worker heartbeat", logger.String("worker", w.ID))
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
	// живой heartbeat — снимаем SUSPECT немедленно, не дожидаясь следующего
	// опроса dead man's switch (до heartbeatGracePeriod)
	wnode.SetAlive()

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

func (r *WorkerRegistry) AddressOf(workerID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	wnode, ok := r.workers[workerID]
	if !ok {
		return ""
	}

	return wnode.Address
}

// checkHeartbeat — dead man's switch: ALIVE -> SUSPECT -> DEAD за два пропущенных
// heartbeatGracePeriod подряд, ALIVE восстанавливается сам, как только heartbeat
// снова свежий. Возвращает false, когда триггер больше не нужно перезапускать
// (воркер помечен мёртвым или пропал из реестра).
func (r *WorkerRegistry) checkHeartbeat(workerID string) bool {
	r.mu.RLock()
	worker, ok := r.workers[workerID]
	r.mu.RUnlock()

	if !ok || worker.GetStatus() == domain.WorkerDead {
		return false
	}

	if time.Since(worker.LastHeartbeat) > heartbeatGracePeriod {
		if worker.GetStatus() == domain.WorkerAlive {
			r.updateWorkerStatus(workerID, domain.WorkerSuspect)

			return true
		}

		r.updateWorkerStatus(workerID, domain.WorkerDead)

		return false
	}

	r.updateWorkerStatus(workerID, domain.WorkerAlive)

	return true
}

func (r *WorkerRegistry) updateWorkerStatus(workerID string, status domain.WorkerStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()

	wnode, ok := r.workers[workerID]
	if !ok {
		return
	}

	switch status {
	case domain.WorkerAlive:
		wnode.SetAlive()
	case domain.WorkerSuspect:
		r.log.Warn("marking worker suspect", logger.String("worker", workerID))
		wnode.SetSuspect()
	case domain.WorkerDead:
		r.log.Warn("marking worker dead", logger.String("worker", workerID))
		wnode.SetDead()

		for _, hook := range r.onDeadHooks {
			hook(workerID)
		}
	}
}
