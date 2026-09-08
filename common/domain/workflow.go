package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	appcontext "github.com/blrrubik/distributed-workflow-orchestrator/common/context"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
)

// Workflow — DAG задач.
type Workflow struct {
	ID        string
	TenantID  string
	Name      string
	Tasks     map[string]*Task
	status    WorkflowStatus
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
		status:    WorkflowPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}, nil
}

func (w *Workflow) GetStatus() WorkflowStatus {
	return w.status
}

func (w *Workflow) UpdateStatus(ctx appcontext.AppContext, status WorkflowStatus) bool {
	if err := w.status.CanTransitTo(status); err != nil {
		ctx.GetLogger().Error(
			"update workflow status failed",
			logger.String("workflow_id", w.ID),
			logger.String("from", w.status.String()),
			logger.String("to", status.String()),
			logger.Error(err),
		)

		return false
	}

	w.status = status

	return true
}

// IsFinished — завершён ли workflow целиком (терминальный статус).
func (w *Workflow) IsFinished() bool {
	return w.status == WorkflowSucceeded || w.status == WorkflowFailed || w.status == WorkflowCancelled
}

// AllTasksFinished — завершены ли все задачи графа (успешно/с ошибкой/отменены).
func (w *Workflow) AllTasksFinished() bool {
	for _, task := range w.Tasks {
		if !task.IsFinished() {
			return false
		}
	}

	return true
}

// HasFailedTask — есть ли в графе задача, завершившаяся неуспешно.
func (w *Workflow) HasFailedTask() bool {
	for _, task := range w.Tasks {
		if status := task.GetStatus(); status == TaskFailed || status == TaskCancelled {
			return true
		}
	}

	return false
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
