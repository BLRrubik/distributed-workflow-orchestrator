package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	httpexecutor "github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor/http"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor/shell"
	executrorregistry "github.com/blrrubik/distributed-workflow-orchestrator/pkg/executror_registry"
)

type Executor interface {
	Execute(ctx context.Context, spec domain.TaskSpec, timeout time.Duration) (domain.TaskResult, error)
}

func isExecutorExists(executorType string) bool {
	return executorType == "shell" || executorType == "http"
}

func RegisterExecutors(executorRegistry *executrorregistry.Registry, capabilities []string) ([]string, error) {
	registered := make([]string, 0, len(capabilities))

	for _, capability := range capabilities {
		if !isExecutorExists(capability) {
			return nil, fmt.Errorf("executor %s does not exist", capability)
		}

		switch capability {
		case "shell":
			executorRegistry.Register(capability, &shell.Executor{})
		case "http":
			executorRegistry.Register(capability, &httpexecutor.Executor{})
		}

		registered = append(registered, capability)
	}

	return registered, nil
}
