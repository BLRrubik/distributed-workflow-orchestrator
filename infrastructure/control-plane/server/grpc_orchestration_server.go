package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
	"github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/control-plane/events"
	eventbus "github.com/blrrubik/distributed-workflow-orchestrator/pkg/event_bus"
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
				Payload: json.RawMessage(task.GetPayload()),
			},
		})
	}

	wf, err := domain.NewWorkflow("admin", request.GetName(), tasks)
	if err != nil {
		return nil, fmt.Errorf(
			"orchestration request: %w",
			status.Error(codes.InvalidArgument, fmt.Sprintf("failed to submit workflow: %s", err.Error())),
		)
	}

	id, err := g.engine.SubmitWorkflow(ctx, wf)
	if err != nil {
		return nil, fmt.Errorf(
			"orchestration request: %w",
			status.Error(codes.Internal, fmt.Sprintf("failed to submit workflow: %s", err.Error())),
		)
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
		return nil, fmt.Errorf(
			"orchestration request: %w",
			status.Error(codes.NotFound, fmt.Sprintf("workflow not found: %s", request.GetWorkflowId())),
		)
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

// CancelWorkflow — см. §4.1/§4.3 docs/about.md. Вся бизнес-логика (пометка
// CANCELLED + best-effort уведомление воркеров, сгруппированное в батчи) живёт
// в engine — обработчик его не дублирует и сам по сети к воркерам не ходит.
func (g *grpcServer) CancelWorkflow(
	ctx context.Context,
	request *protogen.CancelWorkflowRequest,
) (*protogen.CancelWorkflowResponse, error) {
	if err := g.engine.CancelWorkflow(ctx, request.GetWorkflowId()); err != nil {
		return nil, fmt.Errorf(
			"orchestration request: %w",
			status.Error(codes.NotFound, fmt.Sprintf("failed to cancel workflow: %s", err.Error())),
		)
	}

	return &protogen.CancelWorkflowResponse{Accepted: true}, nil
}

// CancelTask — см. §4.4 docs/about.md. Отменяет одну задачу и каскадом всё,
// что от неё зависит; уведомление воркеров — тоже внутри engine.
func (g *grpcServer) CancelTask(
	ctx context.Context,
	request *protogen.CancelTaskRequest,
) (*protogen.CancelTaskResponse, error) {
	if err := g.engine.CancelTask(ctx, request.GetTaskId()); err != nil {
		return nil, fmt.Errorf("orchestration request: %w", status.Error(codes.NotFound, fmt.Sprintf("failed to cancel task: %s", err.Error())))
	}

	return &protogen.CancelTaskResponse{Accepted: true}, nil
}

func (g *grpcServer) StreamWorkflowEvents(
	request *protogen.GetWorkflowRequest,
	stream grpc.ServerStreamingServer[protogen.WorkflowEvent],
) error {
	wfID := request.GetWorkflowId()

	wf, err := g.engine.GetWorkflow(wfID)
	if err != nil {
		return fmt.Errorf("stream workflow events: %w", status.Error(codes.NotFound, fmt.Sprintf("workflow not found: %s", wfID)))
	}

	if wf.IsFinished() {
		return nil
	}

	err = stream.Send(snapshotToEvent(wf))
	if err != nil {
		return fmt.Errorf(
			"stream workflow events: %w",
			status.Error(codes.Internal, fmt.Sprintf("failed to send workflow event: %s", err.Error())),
		)
	}

	ctx := stream.Context()
	subID := wfID + "/" + uuid.NewString()

	out := make(chan eventbus.Event, 64)

	handler := func(ev eventbus.Event) {
		select {
		case <-ctx.Done():
			return
		case out <- ev:
		default:
		}
	}

	if err = g.eventBus.Subscribe(subID, handler); err != nil {
		return fmt.Errorf("subscribe to workflow events: %w", err)
	}

	defer func() {
		if err := g.eventBus.Unsubscribe(subID); err != nil {
			g.log.Warn("failed to unsubscribe", logger.Error(err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-out:
			if g.sendWorkflowEvent(stream, wfID, ev) {
				return nil
			}
		}
	}
}

func snapshotToEvent(wf *domain.Workflow) *protogen.WorkflowEvent {
	return &protogen.WorkflowEvent{
		WorkflowId: wf.ID,
		Status:     wf.GetStatus().String(),
		Timestamp:  time.Now().Unix(),
	}
}

func (g *grpcServer) sendWorkflowEvent(stream grpc.ServerStreamingServer[protogen.WorkflowEvent], wfID string, ev eventbus.Event) bool {
	switch ev.GetType() {
	case events.WorkflowStatusChanged:
		wev, ok := ev.(*events.WorkflowStatusChangedEvent)
		if !ok {
			return false
		}

		if wev.Status.IsTerminated() {
			return true
		}

		if wev.WorkflowID != wfID {
			return false
		}

		if err := stream.Send(wev.ToProto()); err != nil {
			g.log.Error("failed to send event", "workflow_id", wfID, logger.Error(err))
		}
	case events.TaskStatusChanged:
		tev, ok := ev.(*events.TaskStatusChangedEvent)
		if !ok {
			return false
		}

		if err := stream.Send(tev.ToProto()); err != nil {
			g.log.Error("failed to send event", "workflow_id", wfID, logger.Error(err))
		}
	}

	return false
}
