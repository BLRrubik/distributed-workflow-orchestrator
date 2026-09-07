package main

import (
	"context"

	appctx "github.com/blrrubik/distributed-workflow-orchestrator/common/context"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/engine"
)

func main() {
	log := logger.New(logger.INFO, true)

	ctx := appctx.NewAppContext(context.Background(), log)

	tasks := []domain.Task{
		{ID: "build", Name: "build", Status: domain.TaskPending, DependsOn: []string{}},
		{ID: "test", Name: "test", Status: domain.TaskPending, DependsOn: []string{"build"}},
		{ID: "deploy", Name: "deploy", Status: domain.TaskPending, DependsOn: []string{"test"}},
	}

	wf, err := domain.NewWorkflow("admin", "example-workflow", tasks)
	if err != nil {
		panic(err)
	}

	eng := engine.NewWorkflowEngine()

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
}
