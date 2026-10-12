package executrorregistry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
)

type Executor interface {
	Execute(ctx context.Context, spec domain.TaskSpec, timeout time.Duration) (domain.TaskResult, error)
}

type Registry struct {
	mu        sync.RWMutex
	executors map[string]Executor
}

func New() *Registry {
	return &Registry{executors: make(map[string]Executor)}
}

func (r *Registry) Register(taskType string, ex Executor) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.executors[taskType] = ex
}

func (r *Registry) Get(taskType string) (Executor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ex, ok := r.executors[taskType]
	if !ok {
		return nil, fmt.Errorf("unknown task type: %s", taskType)
	}

	return ex, nil
}
