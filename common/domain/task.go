package domain

import "time"

// TaskStatus — конечный автомат состояния задачи.
type TaskStatus string

const (
	TaskPending    TaskStatus = "PENDING"    // создана, ждёт, пока разрешатся зависимости
	TaskReady      TaskStatus = "READY"      // зависимости выполнены, ждёт свободного воркера
	TaskDispatched TaskStatus = "DISPATCHED" // отправлена воркеру, ждём подтверждения
	TaskRunning    TaskStatus = "RUNNING"    // воркер подтвердил запуск
	TaskSucceeded  TaskStatus = "SUCCEEDED"
	TaskFailed     TaskStatus = "FAILED"
	TaskRetrying   TaskStatus = "RETRYING"
	TaskCancelled  TaskStatus = "CANCELLED"
)

func (s TaskStatus) String() string {
	switch s {
	case TaskPending:
		return "PENDING"
	case TaskReady:
		return "READY"
	case TaskDispatched:
		return "DISPATCHED"
	case TaskRunning:
		return "RUNNING"
	case TaskSucceeded:
		return "SUCCEEDED"
	case TaskFailed:
		return "FAILED"
	case TaskRetrying:
		return "RETRYING"
	case TaskCancelled:
		return "CANCELLED"
	default:
		return "UNKNOWN"
	}
}

// Task — узел графа выполнения (DAG node).
type Task struct {
	ID           string
	WorkflowID   string
	Name         string
	DependsOn    []string // рёбра графа: этот таск ждёт завершения перечисленных
	Command      TaskSpec // что именно выполнять
	MaxRetries   int
	RetryBackoff time.Duration
	Timeout      time.Duration
	Status       TaskStatus
	AssignedTo   string
	Attempt      int
	Result       *TaskResult
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (t *Task) IsFinished() bool {
	return t.Status == TaskSucceeded || t.Status == TaskFailed || t.Status == TaskCancelled
}

func (t *Task) AllDepsSucceeded(wf *Workflow) bool {
	for _, dep := range t.DependsOn {
		task, ok := wf.Tasks[dep]
		if !ok {
			continue
		}

		if !task.IsFinished() {
			return false
		}
	}

	return true
}

// TaskSpec — абстракция типа задачи.
type TaskSpec struct {
	Type    string            // "shell" | "http" | "webhook"
	Payload map[string]string // например {"cmd": "echo hello"} или {"url": "...", "method": "POST"}
}

type TaskResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Error    string
	Duration time.Duration
}
