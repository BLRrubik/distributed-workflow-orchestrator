package executor

import (
	"context"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor/shell"
)

type Executor interface {
	Execute(ctx context.Context, spec domain.TaskSpec, timeout time.Duration) (domain.TaskResult, error)
}

type Executors struct {
	Shell *shell.Executor
}

func NewExecutors() *Executors {
	return &Executors{
		Shell: &shell.Executor{},
	}
}
