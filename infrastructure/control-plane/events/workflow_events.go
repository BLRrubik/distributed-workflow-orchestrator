package events

import (
	"github.com/blrrubik/distributed-workflow-orchestrator/common/api/protogen"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
)

const WorkflowStatusChanged = "workflow.status_changed"

type WorkflowStatusChangedEvent struct {
	WorkflowID string
	Status     domain.WorkflowStatus
	Timestamp  int64
}

func (e *WorkflowStatusChangedEvent) GetType() string {
	return WorkflowStatusChanged
}

func (e *WorkflowStatusChangedEvent) ToProto() *protogen.WorkflowEvent {
	return &protogen.WorkflowEvent{
		WorkflowId: e.WorkflowID,
		Status:     e.Status.String(),
		Timestamp:  e.Timestamp,
	}
}
