package executor

import (
	"context"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
)

type Executor interface {
	Execute(ctx context.Context, spec domain.TaskSpec, timeout time.Duration) (domain.TaskResult, error)
}
