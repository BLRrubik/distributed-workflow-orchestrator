package domain

import "errors"

var (
	ErrTransitToSameStatus = errors.New("transit to same status")
	ErrTransitNotAllowed   = errors.New("transit not allowed")
)

// TaskStatus — конечный автомат состояния задачи.
type TaskStatus byte

const (
	TaskStatusUnknown TaskStatus = 0
	TaskPending       TaskStatus = 1 // создана, ждёт, пока разрешатся зависимости
	TaskReady         TaskStatus = 2 // зависимости выполнены, ждёт свободного воркера
	TaskDispatched    TaskStatus = 3 // отправлена воркеру, ждём подтверждения
	TaskRunning       TaskStatus = 4 // воркер подтвердил запуск
	TaskSucceeded     TaskStatus = 5
	TaskFailed        TaskStatus = 6
	TaskRetrying      TaskStatus = 7
	TaskCancelled     TaskStatus = 8

	TaskStatusCount = 9
)

var taskStatusesGraph = [TaskStatusCount][TaskStatusCount]bool{
	TaskPending: {
		TaskReady:     true,
		TaskCancelled: true,
	}, // PENDING -> PENDING | READY
	TaskReady: {
		TaskDispatched: true,
		TaskCancelled:  true,
	},
	TaskDispatched: {
		TaskRunning:   true,
		TaskSucceeded: true,
		TaskCancelled: true,
	},
	TaskRunning: {
		TaskSucceeded: true,
		TaskCancelled: true,
		TaskFailed:    true,
	},
	TaskFailed: {
		TaskRetrying: true,
	},
	TaskRetrying: {
		TaskDispatched: true, // ???
		TaskRunning:    true,
	},
}

func (s TaskStatus) CanTransitTo(newStatus TaskStatus) error {
	if s == newStatus {
		return ErrTransitToSameStatus
	}

	if !taskStatusesGraph[s][newStatus] {
		return ErrTransitNotAllowed
	}

	return nil
}

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
