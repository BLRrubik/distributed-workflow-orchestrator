package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
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
	tasksMapByID := make(map[string]*Task, len(tasks))
	taskMapByName := make(map[string]*Task, len(tasks))

	for i := range tasks {
		task := &tasks[i]

		if task.ID == "" {
			task.ID = uuid.NewString()
		}

		if _, ok := taskMapByName[task.Name]; ok {
			return nil, fmt.Errorf("duplicate task name: %s", task.Name)
		}

		if _, ok := tasksMapByID[task.ID]; ok {
			return nil, fmt.Errorf("duplicate task id: %s", task.ID)
		}

		taskMapByName[task.Name] = task
		tasksMapByID[task.ID] = task
	}

	// task_id уже сгенерирован вызывающей стороной, а depends_on в запросе
	// ссылается на задачи по имени — граф ниже (cycle-check, AllDepsSucceeded,
	// cancelDownstream) работает по ID, поэтому имена резолвим в ID один раз здесь.
	if err := resolveDependencies(taskMapByName); err != nil {
		return nil, fmt.Errorf("invalid tasks: %w", err)
	}

	if err := validateTasksCycle(tasksMapByID); err != nil {
		return nil, fmt.Errorf("tasks cycle failed: %s", err.Error())
	}

	wf := &Workflow{
		ID:        uuid.NewString(),
		TenantID:  tenantID,
		Name:      name,
		Tasks:     tasksMapByID,
		status:    WorkflowPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	for i := range wf.Tasks {
		wf.Tasks[i].WorkflowID = wf.ID
		wf.Tasks[i].CreatedAt = time.Now()
		wf.Tasks[i].UpdatedAt = time.Now()

		if wf.Tasks[i].Timeout == 0 {
			wf.Tasks[i].Timeout = 30 * time.Second
		}

		if wf.Tasks[i].RetryBackoff == 0 {
			wf.Tasks[i].RetryBackoff = 200 * time.Millisecond
		}
	}

	return wf, nil
}

func (w *Workflow) GetStatus() WorkflowStatus {
	return w.status
}

func (w *Workflow) UpdateStatus(status WorkflowStatus) error {
	if err := w.status.CanTransitTo(status); err != nil {
		return err
	}

	w.status = status

	return nil
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

// HasFailedTask — есть ли в графе задача, реально провалившаяся (не отменённая).
func (w *Workflow) HasFailedTask() bool {
	for _, task := range w.Tasks {
		if task.GetStatus() == TaskFailed {
			return true
		}
	}

	return false
}

// HasCancelledTask — есть ли в графе отменённая задача.
func (w *Workflow) HasCancelledTask() bool {
	for _, task := range w.Tasks {
		if task.GetStatus() == TaskCancelled {
			return true
		}
	}

	return false
}

// resolveDependencies валидирует depends_on (заданный по имени задачи) и
// на месте заменяет имена на ID зависимых задач.
func resolveDependencies(tasksByName map[string]*Task) error {
	for _, task := range tasksByName {
		resolved := make([]string, len(task.DependsOn))

		for i, depName := range task.DependsOn {
			depTask, ok := tasksByName[depName]
			if !ok {
				return fmt.Errorf("invalid dependency: %s", depName)
			}

			resolved[i] = depTask.ID
		}

		task.DependsOn = resolved
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
