package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor/shell"
	executrorregistry "github.com/blrrubik/distributed-workflow-orchestrator/pkg/executror_registry"
)

type Executor interface {
	Execute(ctx context.Context, spec domain.TaskSpec, timeout time.Duration) (domain.TaskResult, error)
}

func isExecutorExists(executorType string) bool {
	return executorType == "shell"
}

func RegisterExecutors(executorRegistry *executrorregistry.Registry, capabilities []string) error {
	for _, capability := range capabilities {
		if !isExecutorExists(capability) {
			return fmt.Errorf("executor %s does not exist", capability)
		}

		switch capability {
		case "shell":
			executorRegistry.Register(capability, &shell.Executor{})
		}
	}

	return nil
}
