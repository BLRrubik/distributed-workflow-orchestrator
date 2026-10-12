package service

import (
	"context"
	"time"

	wp "github.com/blrrubik/distributed-workflow-orchestrator/pkg/worker_pool"
)

// trackedTask даёт задаче отменяемый контекст (для CancelTask, не только
// context.WithTimeout) и на OnDone (см. wp.Task) чистит её ключ из unique —
// но только при успехе. WorkerPool на ошибке перекладывает ТОТ ЖЕ Job в retry
// (см. moveToRetry) — снимать ключ на каждой попытке нельзя: в окне между
// провалом и подхватом ретраем туда мог бы проскочить дубликат-диспатч и
// создать вторую параллельную копию задачи. Берёт только узкие замыкания, а
// не весь *unique — trackedTask не обязан знать про его внутреннее устройство.
type trackedTask struct {
	inner     wp.Task
	setCancel func(cancel context.CancelFunc)
	onDone    func(err error)
}

func (t *trackedTask) Do(ctx context.Context) error {
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	t.setCancel(cancel)

	return t.inner.Do(taskCtx) //nolint:wrapcheck // прозрачная обёртка: инкапсулированная задача уже сама оборачивает свои ошибки
}

func (t *trackedTask) OnDone(err error) {
	if t.onDone != nil {
		t.onDone(err)
	}
}

func (t *trackedTask) GetWaitDuration() time.Duration {
	return t.inner.GetWaitDuration()
}
