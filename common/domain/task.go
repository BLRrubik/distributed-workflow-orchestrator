package domain

import "time"

// TaskStatus — конечный автомат состояния задачи
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

// Task — узел графа выполнения (DAG node)
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

// TaskSpec — абстракция типа задачи. Начните с одного типа (Shell), потом добавите HTTP/gRPC.
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
