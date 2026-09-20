package server

import (
	"context"
	"fmt"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/engine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (g *grpcServer) SubmitWorkflow(
	ctx context.Context,
	request *protogen.SubmitWorkflowRequest,
) (*protogen.SubmitWorkflowResponse, error) {
	tasks := make([]domain.Task, 0, len(request.Tasks))
	for _, task := range request.GetTasks() {
		tasks = append(tasks, domain.Task{
			Name:         task.GetName(),
			DependsOn:    task.GetDependsOn(),
			MaxRetries:   int(task.GetMaxRetries()),
			RetryBackoff: time.Duration(task.GetRetryBackoffSeconds()) * time.Second,
			Timeout:      time.Duration(task.GetTimeoutSeconds()) * time.Second,
			Spec: domain.TaskSpec{
				Type:    task.GetType(),
				Payload: task.GetPayload(),
			},
		})
	}

	wf, err := domain.NewWorkflow("admin", request.GetName(), tasks)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("failed to submit workflow: %s", err.Error()))
	}

	id, err := g.engine.SubmitWorkflow(ctx, wf)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to submit workflow: %w", err))
	}

	return &protogen.SubmitWorkflowResponse{
		WorkflowId: id,
	}, nil
}

func (g *grpcServer) GetWorkflow(
	ctx context.Context,
	request *protogen.GetWorkflowRequest,
) (*protogen.WorkflowStatusResponse, error) {
	workflow, err := g.engine.GetWorkflow(request.GetWorkflowId())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("workflow not found: %s", request.GetWorkflowId()))
	}

	tasks := make([]*protogen.TaskStatusInfo, 0, len(workflow.Tasks))
	for _, task := range workflow.Tasks {
		taskResp := &protogen.TaskStatusInfo{
			Id:         task.ID,
			Name:       task.Name,
			Status:     task.GetStatus().String(),
			AssignedTo: task.AssignedTo,
			Attempt:    int32(task.Attempt),
		}

		if res, ok := task.GetResult(); ok {
			taskResp.Error = res.Error
		}

		tasks = append(tasks, taskResp)
	}

	resp := &protogen.WorkflowStatusResponse{
		WorkflowId: workflow.ID,
		Name:       workflow.Name,
		Status:     workflow.GetStatus().String(),
		Tasks:      tasks,
	}

	return resp, nil
}

// CancelWorkflow — см. §4.1 docs/about.md. Пометка CANCELLED синхронна (engine),
// уведомление воркеров о реально исполняющихся задачах — best-effort, не блокирует ответ.
func (g *grpcServer) CancelWorkflow(
	ctx context.Context,
	request *protogen.CancelWorkflowRequest,
) (*protogen.CancelWorkflowResponse, error) {
	running, err := g.engine.CancelWorkflow(ctx, request.GetWorkflowId())
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("failed to cancel workflow: %s", err.Error()))
	}

	for _, ref := range running {
		go func(ref engine.RunningTaskRef) {
			cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if _, err := g.workerClient.CancelTask(cancelCtx, ref.WorkerID, &protogen.CancelTaskRequest{TaskId: ref.TaskID}); err != nil {
				g.log.Warn("failed to notify worker about cancellation",
					logger.String("task", ref.TaskID),
					logger.String("worker", ref.WorkerID),
					logger.Error(err),
				)
			}
		}(ref)
	}

	return &protogen.CancelWorkflowResponse{Accepted: true}, nil
}

func (g *grpcServer) StreamWorkflowEvents(
	request *protogen.GetWorkflowRequest,
	stream grpc.ServerStreamingServer[protogen.WorkflowEvent],
) error {
	//TODO implement me
	panic("implement me")
}
