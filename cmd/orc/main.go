package main

import (
	"context"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/engine"
)

func main() {
	log := logger.New(logger.INFO, true)

	ctx := context.Background()

	tasks := []domain.Task{
		{ID: "build", Name: "build", DependsOn: []string{}},
		{ID: "test", Name: "test", DependsOn: []string{"build"}},
		{ID: "deploy", Name: "deploy", DependsOn: []string{"test"}},
	}

	wf, err := domain.NewWorkflow("admin", "example-workflow", tasks)
	if err != nil {
		panic(err)
	}

	eng := engine.NewWorkflowEngine(log)

	workflowID, err := eng.SubmitWorkflow(ctx, wf)
	if err != nil {
		panic(err)
	}

	if err = eng.OnTaskCompleted(ctx, workflowID, "build", domain.TaskResult{Error: ""}); err != nil {
		panic(err)
	}

	if err = eng.OnTaskCompleted(ctx, workflowID, "test", domain.TaskResult{Error: ""}); err != nil {
		panic(err)
	}

	if err = eng.OnTaskCompleted(ctx, workflowID, "deploy", domain.TaskResult{Error: ""}); err != nil {
		panic(err)
	}
}
