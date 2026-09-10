package orchestration

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
)

func TestWorkerRegistry_Register(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	err := r.Register(domain.WorkerNode{ID: "w1"})
	assert.NoError(t, err)

	// повторная регистрация того же ID — не ошибка (передеплой воркера)
	err = r.Register(domain.WorkerNode{ID: "w1"})
	assert.NoError(t, err)
}

func TestWorkerRegistry_Register_Redeploy_UpdatesAndRevives(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	assert.NoError(t, r.Register(domain.WorkerNode{ID: "w1", Address: "old:8080", Capacity: 5}))

	// воркер умер (например, пропустил heartbeat) — типичное состояние перед передеплоем
	r.workers["w1"].SetDead()
	assert.Empty(t, r.AliveWorkers())

	// передеплой: тот же ID, новый адрес (новый под) — должен обновить данные и ожить
	assert.NoError(t, r.Register(domain.WorkerNode{ID: "w1", Address: "new:9090", Capacity: 10}))

	workers := r.AliveWorkers()
	assert.Len(t, workers, 1)
	assert.Equal(t, "new:9090", workers[0].Address)
	assert.Equal(t, 10, workers[0].Capacity)
}

func TestWorkerRegistry_Heartbeat(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	err := r.Heartbeat("unknown", 3)
	assert.Error(t, err)

	assert.NoError(t, r.Register(domain.WorkerNode{ID: "w1"}))

	assert.NoError(t, r.Heartbeat("w1", 5))

	workers := r.AliveWorkers()
	assert.Len(t, workers, 1)
	assert.Equal(t, 5, workers[0].RunningTasks)
}

func TestWorkerRegistry_AliveWorkers(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	assert.Empty(t, r.AliveWorkers())

	alive := domain.WorkerNode{ID: "alive"}

	suspect := domain.WorkerNode{ID: "suspect"}
	suspect.SetSuspect()

	dead := domain.WorkerNode{ID: "dead"}
	dead.SetDead()

	assert.NoError(t, r.Register(alive))
	assert.NoError(t, r.Register(suspect))
	assert.NoError(t, r.Register(dead))

	workers := r.AliveWorkers()
	assert.Len(t, workers, 1)
	assert.Equal(t, "alive", workers[0].ID)
}

func TestWorkerRegistry_IncrementRunning(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	// неизвестный воркер — молча ничего не делает, не паникует
	r.IncrementRunning("unknown")

	assert.NoError(t, r.Register(domain.WorkerNode{ID: "w1"}))

	r.IncrementRunning("w1")
	r.IncrementRunning("w1")

	workers := r.AliveWorkers()
	assert.Len(t, workers, 1)
	assert.Equal(t, 2, workers[0].RunningTasks)
}

func TestWorkerRegistry_CheckHeartbeat_SuspectThenDead(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	assert.NoError(t, r.Register(domain.WorkerNode{ID: "w1"}))
	r.workers["w1"].LastHeartbeat = time.Now().Add(-heartbeatGracePeriod - time.Second)

	// первый пропуск — ещё не мёртв, только SUSPECT, триггер просит перезапустить
	assert.True(t, r.checkHeartbeat("w1"))
	assert.Equal(t, domain.WorkerSuspect, r.workers["w1"].GetStatus())
	assert.Empty(t, r.AliveWorkers(), "SUSPECT не должен попадать в AliveWorkers")

	// heartbeat так и не пришёл — второй пропуск подряд добивает до DEAD
	r.workers["w1"].LastHeartbeat = time.Now().Add(-heartbeatGracePeriod - time.Second)
	assert.False(t, r.checkHeartbeat("w1"), "DEAD — триггер больше не нужно перезапускать")
	assert.Equal(t, domain.WorkerDead, r.workers["w1"].GetStatus())
}

func TestWorkerRegistry_CheckHeartbeat_RevivesToAlive(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	assert.NoError(t, r.Register(domain.WorkerNode{ID: "w1"}))
	r.workers["w1"].SetSuspect()

	// heartbeat снова свежий — очередной опрос должен вернуть воркера в ALIVE
	r.workers["w1"].LastHeartbeat = time.Now()
	assert.True(t, r.checkHeartbeat("w1"))
	assert.Equal(t, domain.WorkerAlive, r.workers["w1"].GetStatus())
}

func TestWorkerRegistry_Heartbeat_RevivesSuspectImmediately(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	assert.NoError(t, r.Register(domain.WorkerNode{ID: "w1"}))
	r.workers["w1"].SetSuspect()
	assert.Empty(t, r.AliveWorkers())

	// живой Heartbeat — прямое доказательство жизни, не ждём следующего опроса dead man's switch
	assert.NoError(t, r.Heartbeat("w1", 0))
	assert.Len(t, r.AliveWorkers(), 1)
}

func TestWorkerRegistry_CheckHeartbeat_UnknownWorker(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	assert.False(t, r.checkHeartbeat("ghost"))
}

func TestWorkerRegistry_ConcurrentAccess(t *testing.T) {
	r := NewWorkerRegistry(logger.New(logger.ERROR, false))

	assert.NoError(t, r.Register(domain.WorkerNode{ID: "w1"}))

	var wg sync.WaitGroup

	wg.Add(3)

	go func() {
		defer wg.Done()

		for range 100 {
			r.IncrementRunning("w1")
		}
	}()

	go func() {
		defer wg.Done()

		for range 100 {
			_ = r.Heartbeat("w1", 1)
		}
	}()

	go func() {
		defer wg.Done()

		for range 100 {
			r.AliveWorkers()
		}
	}()

	wg.Wait()
}
