package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// WorkflowStatus — состояние графа целиком.
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

// Workflow — DAG задач.
type Workflow struct {
	ID        string
	TenantID  string
	Name      string
	Tasks     map[string]*Task
	Status    WorkflowStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewWorkflow(tenantID string, name string, tasks []Task) (*Workflow, error) {
	tasksMap := make(map[string]*Task, len(tasks))
	seenNames := make(map[string]struct{}, len(tasks))

	for _, task := range tasks {
		if _, ok := seenNames[task.Name]; ok {
			return nil, fmt.Errorf("duplicate task name: %s", task.Name)
		}

		seenNames[task.Name] = struct{}{}

		tasksMap[task.ID] = &task
	}

	if err := validateDependencies(tasksMap); err != nil {
		return nil, fmt.Errorf("invalid tasks: %w", err)
	}

	if err := validateTasksCycle(tasksMap); err != nil {
		return nil, fmt.Errorf("tasks cycle failed: %s", err.Error())
	}

	return &Workflow{
		ID:        uuid.NewString(),
		TenantID:  tenantID,
		Name:      name,
		Tasks:     tasksMap,
		Status:    WorkflowPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}, nil
}

func validateDependencies(tasks map[string]*Task) error {
	for _, task := range tasks {
		for _, dep := range task.DependsOn {
			if _, ok := tasks[dep]; !ok {
				return fmt.Errorf("invalid dependency: %s", dep)
			}
		}
	}

	return nil
}

func validateTasksCycle(tasks map[string]*Task) error {
	for _, task := range tasks {
		if hasCycle(task.ID, tasks, make(map[string]byte)) != nil {
			return fmt.Errorf("cycle detected in task: %s", task.ID)
		}
	}

	return nil
}

func hasCycle(taskID string, tasks map[string]*Task, colors map[string]byte) error {
	colors[taskID] = 1 // gray
	for _, task := range tasks[taskID].DependsOn {
		if colors[task] == 1 {
			return fmt.Errorf("cycle detected: %s -> %s", taskID, task)
		}

		if colors[task] == 0 { // white
			if err := hasCycle(task, tasks, colors); err != nil {
				return fmt.Errorf("cycle detected: %s -> %s", taskID, task)
			}
		}
	}

	colors[taskID] = 2 // black

	return nil
}
