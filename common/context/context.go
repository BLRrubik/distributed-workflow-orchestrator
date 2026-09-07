package context

import (
	"context"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
)

type AppContext struct {
	context.Context

	log *logger.Logger
}

func NewAppContext(
	ctx context.Context,
	log *logger.Logger,
) *AppContext {
	return &AppContext{
		Context: ctx,
		log:     log,
	}
}

func (ctx *AppContext) GetContext() context.Context {
	return ctx.Context
}

func (ctx *AppContext) GetLogger() *logger.Logger {
	return ctx.log
}
