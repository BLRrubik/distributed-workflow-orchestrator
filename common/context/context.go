package context

import (
	"context"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
)

type AppContext interface {
	Deadline() (deadline time.Time, ok bool)
	Done() <-chan struct{}
	Err() error
	Value(key any) any

	GetContext() context.Context
	GetLogger() *logger.Logger
}

type appContext struct {
	context.Context

	log *logger.Logger
}

func NewAppContext(
	ctx context.Context,
	log *logger.Logger,
) AppContext {
	return &appContext{
		Context: ctx,
		log:     log,
	}
}

func (ctx *appContext) GetContext() context.Context {
	return ctx.Context
}

func (ctx *appContext) GetLogger() *logger.Logger {
	return ctx.log
}
