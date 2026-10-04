package events

import (
	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
)

const TaskStatusChanged = "task.status_changed"

type TaskStatusChangedEvent struct {
	WorkflowID string
	TaskID     string
	Status     domain.TaskStatus
	Timestamp  int64
}

func (e *TaskStatusChangedEvent) GetType() string {
	return TaskStatusChanged
}

func (e *TaskStatusChangedEvent) ToProto() *protogen.WorkflowEvent {
	return &protogen.WorkflowEvent{
		WorkflowId: e.WorkflowID,
		TaskId:     e.TaskID,
		Status:     e.Status.String(),
		Timestamp:  e.Timestamp,
	}
}
