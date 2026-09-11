package retry_queue

import (
	"context"
	"time"
)

type RetryTrigger interface {
	GetKey() string
	GetTimeout() time.Duration
	// Do return true to continue retry or false to stop
	Do(ctx context.Context) bool
	Log(ctx context.Context)
}

// CommonRetryTrigger общая имплементация RetryTrigger, что бы не плодить однотипных ретраев.
type CommonRetryTrigger struct {
	key     string
	timeout time.Duration
	// Do return true to continue retry or false to stop
	do  func(ctx context.Context) bool
	log func(ctx context.Context)
}

func NewCommonRetryTrigger(
	key string,
	timeout time.Duration,
	do func(ctx context.Context) bool,
	log func(ctx context.Context),
) *CommonRetryTrigger {
	return &CommonRetryTrigger{
		key:     key,
		timeout: timeout,
		do:      do,
		log:     log,
	}
}

// GetKey возвращает уникальный ключ.
func (c CommonRetryTrigger) GetKey() string {
	return c.key
}

// GetTimeout возвращает таймаут.
func (c CommonRetryTrigger) GetTimeout() time.Duration {
	return c.timeout
}

// Do retry функция.
func (c CommonRetryTrigger) Do(ctx context.Context) bool {
	return c.do(ctx)
}

// Log функция логирования.
func (c CommonRetryTrigger) Log(ctx context.Context) {
	c.log(ctx)
}
