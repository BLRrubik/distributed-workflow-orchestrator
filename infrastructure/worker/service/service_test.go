package service

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor"
	wp "github.com/blrrubik/distributed-workflow-orchestrator/pkg/worker_pool"
)

func newTestService(t *testing.T, opts ...wp.WorkerPoolOpt) *WorkerService {
	t.Helper()

	pool := wp.NewWorkerPool(opts...)
	ws := NewWorkerService(t.Context(), pool, executor.NewExecutors(), logger.New(logger.ERROR, false))

	// Stop() должен успеть догрести очередь, пока ctx ещё жив — ShellTask.Do
	// использует ctx пула напрямую, а t.Context() отменится только после этого
	// cleanup (регистрируется раньше — сработает позже, LIFO)
	t.Cleanup(pool.Stop)

	return ws
}

func TestWorkerService_DispatchTask_ExecutesShellTask(t *testing.T) {
	ws := newTestService(t)

	req := &protogen.DispatchRequest{
		TaskId:         "task-1",
		Type:           "shell",
		TimeoutSeconds: 5,
		Payload:        map[string]string{"command": "echo"},
	}

	err := ws.DispatchTask(context.Background(), req)
	assert.NoError(t, err)
}

func TestWorkerService_DispatchTask_UnknownExecutorType(t *testing.T) {
	ws := newTestService(t)

	req := &protogen.DispatchRequest{
		TaskId: "task-2",
		Type:   "unknown",
	}

	err := ws.DispatchTask(context.Background(), req)
	assert.Error(t, err)
}

func TestWorkerService_DispatchTask_SurvivesRequestContextCancel(t *testing.T) {
	var buf bytes.Buffer

	log := &logger.Logger{Logger: slog.New(slog.NewJSONHandler(&buf, nil))}

	poolCtx := t.Context()

	pool := wp.NewWorkerPool(wp.WithWorkerCount(1), wp.WithCapacity(2))
	ws := NewWorkerService(poolCtx, pool, executor.NewExecutors(), log)

	// занимает единственного воркера, чтобы target исполнился уже после отмены reqCtx
	blocker := &protogen.DispatchRequest{
		TaskId: "blocker", Type: "shell", TimeoutSeconds: 5,
		Payload: map[string]string{"command": "sleep", "args": "0.3"},
	}
	target := &protogen.DispatchRequest{
		TaskId: "target", Type: "shell", TimeoutSeconds: 5,
		Payload: map[string]string{"command": "echo"},
	}

	reqCtx, cancelReq := context.WithCancel(context.Background())

	assert.NoError(t, ws.DispatchTask(context.Background(), blocker))
	assert.NoError(t, ws.DispatchTask(reqCtx, target))

	// request ctx умирает сразу, как это бывает после завершения gRPC-запроса —
	// target к этому моменту ещё сидит в очереди и исполнится позже
	cancelReq()

	// Stop() ждёт завершения воркеров (wg.Wait) — это и есть точка синхронизации
	// с записью в buf, без неё чтение buf ниже было бы гонкой
	pool.Stop()

	assert.Contains(t, buf.String(), `"task_id":"target"`)
	assert.NotContains(t, buf.String(), "task execution failed")
}

func TestWorkerService_DispatchTask_PoolFull(t *testing.T) {
	ws := newTestService(t, wp.WithWorkerCount(1), wp.WithCapacity(1))

	slowTask := func(id string) *protogen.DispatchRequest {
		return &protogen.DispatchRequest{
			TaskId: id, Type: "shell", TimeoutSeconds: 5,
			Payload: map[string]string{"command": "sleep", "args": "0.3"},
		}
	}

	// первая занимает единственного воркера
	assert.NoError(t, ws.DispatchTask(context.Background(), slowTask("task-1")))
	time.Sleep(20 * time.Millisecond) // даём воркеру время забрать job из канала

	// вторая занимает единственное место в очереди jobChan
	assert.NoError(t, ws.DispatchTask(context.Background(), slowTask("task-2")))

	// третьей уже некуда деться
	err := ws.DispatchTask(context.Background(), slowTask("task-3"))
	assert.Error(t, err)

	time.Sleep(400 * time.Millisecond)
}
