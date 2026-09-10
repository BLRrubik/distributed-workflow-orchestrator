package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/client"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/engine"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/orchestration"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/scheduler"
)

func main() {
	log := logger.New(logger.INFO, true)

	ctx := context.Background()

	tasks := []domain.Task{
		{
			ID:        "build",
			Name:      "build",
			DependsOn: []string{},
			Spec: domain.TaskSpec{
				Type: "shell",
				Payload: map[string]string{
					"command": "echo",
					"args":    "build",
				},
			},
			Timeout: 10 * time.Second,
		},
		{
			ID:        "test",
			Name:      "test",
			DependsOn: []string{"build"},
			Spec: domain.TaskSpec{
				Type: "shell",
				Payload: map[string]string{
					"command": "echo",
					"args":    "test",
				},
			},
			Timeout: 10 * time.Second,
		},
		{
			ID:        "deploy",
			Name:      "deploy",
			DependsOn: []string{"test"},
			Spec: domain.TaskSpec{
				Type: "shell",
				Payload: map[string]string{
					"command": "echo",
					"args":    "deploy",
				},
			},
			Timeout: 10 * time.Second,
		},
	}

	wf, err := domain.NewWorkflow("admin", "example-workflow", tasks)
	if err != nil {
		panic(err)
	}

	workerRegistry := orchestration.NewWorkerRegistry(log)
	workerRegistry.Register(domain.WorkerNode{
		ID:       "example-worker",
		Address:  "localhost:8081",
		Capacity: 100,
	})

	workerClient := client.NewWorkerClient(workerRegistry)

	workerRegistry.OnWorkerDead(func(workerID string) {
		workerClient.CloseConn(workerID)
	})

	taskScheduler := scheduler.New(workerRegistry, workerClient, log)
	go taskScheduler.Run(ctx)

	eng := engine.NewWorkflowEngine(log, taskScheduler)

	workflowID, err := eng.SubmitWorkflow(ctx, wf)
	if err != nil {
		panic(err)
	}

	termChan := make(chan os.Signal, 1)

	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	<-termChan

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
