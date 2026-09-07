package domain

import "time"

// WorkflowStatus — состояние графа целиком
type WorkflowStatus string

const (
	WorkflowPending   WorkflowStatus = "PENDING"
	WorkflowRunning   WorkflowStatus = "RUNNING"
	WorkflowSucceeded WorkflowStatus = "SUCCEEDED"
	WorkflowFailed    WorkflowStatus = "FAILED"
	WorkflowCancelled WorkflowStatus = "CANCELLED"
)

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

// Workflow — DAG задач
type Workflow struct {
	ID        string
	TenantID  string
	Name      string
	Tasks     map[string]*Task
	Status    WorkflowStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}
