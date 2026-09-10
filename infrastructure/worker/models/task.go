package models

import "github.com/blrrubik/distributed-workflow-orchestrator/common/domain"

type TaskInfo struct {
	ID               string
	WorkflowID       string
	Spec             domain.TaskSpec
	TimeoutInSeconds int64
}
