package domain

// WorkflowStatus — состояние графа целиком.
type WorkflowStatus byte

const (
	WorkflowPending   WorkflowStatus = 0
	WorkflowRunning   WorkflowStatus = 1
	WorkflowSucceeded WorkflowStatus = 2
	WorkflowFailed    WorkflowStatus = 3
	WorkflowCancelled WorkflowStatus = 4

	WorkflowStatusCount = 5
)

var workflowStatusesGraph = [WorkflowStatusCount][WorkflowStatusCount]bool{
	WorkflowPending: {
		WorkflowRunning:   true,
		WorkflowCancelled: true,
	},
	WorkflowRunning: {
		WorkflowSucceeded: true,
		WorkflowFailed:    true,
		WorkflowCancelled: true,
	},
}

func (s WorkflowStatus) CanTransitTo(newStatus WorkflowStatus) error {
	if s == newStatus {
		return ErrTransitToSameStatus
	}

	if !workflowStatusesGraph[s][newStatus] {
		return ErrTransitNotAllowed
	}

	return nil
}

func (s WorkflowStatus) String() string {
	switch s {
	case WorkflowPending:
		return "PENDING"
	case WorkflowRunning:
		return "RUNNING"
	case WorkflowSucceeded:
		return "SUCCEEDED"
	case WorkflowFailed:
		return "FAILED"
	case WorkflowCancelled:
		return "CANCELLED"
	default:
		return "UNKNOWN"
	}
}
