package domain

import "time"

// WorkerNode — регистрация воркера в кластере.
type WorkerNode struct {
	ID            string
	Address       string            // host:port для gRPC
	Labels        map[string]string // например {"gpu":"true","region":"eu"} — для селекторов
	Capacity      int               // сколько задач параллельно может исполнять
	RunningTasks  int
	LastHeartbeat time.Time
	status        WorkerStatus
}

func (n *WorkerNode) GetStatus() WorkerStatus {
	return n.status
}

func (n *WorkerNode) IsReady() bool {
	return n.status == WorkerAlive
}

func (n *WorkerNode) SetAlive() {
	n.status = WorkerAlive
}

func (n *WorkerNode) SetSuspect() {
	n.status = WorkerSuspect
}

func (n *WorkerNode) SetDead() {
	n.status = WorkerDead
}

type WorkerStatus byte

const (
	WorkerAlive   WorkerStatus = 0
	WorkerSuspect WorkerStatus = 1 // пропустил heartbeat, но ещё в пределах grace period
	WorkerDead    WorkerStatus = 2
)

func (s WorkerStatus) String() string {
	switch s {
	case WorkerAlive:
		return "ALIVE"
	case WorkerSuspect:
		return "SUSPECT"
	case WorkerDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}
