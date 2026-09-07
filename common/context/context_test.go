package context_test

import (
	stdcontext "context"
	"testing"

	appcontext "github.com/blrrubik/distributed-workflow-orchestrator/common/context"
	"github.com/blrrubik/distributed-workflow-orchestrator/common/logger"
)

func TestNewAppContext(t *testing.T) {
	baseCtx := stdcontext.Background()
	log := logger.New(logger.INFO, true)

	appCtx := appcontext.NewAppContext(baseCtx, log)

	if appCtx == nil {
		t.Fatal("NewAppContext returned nil")
	}
	if appCtx.Context != baseCtx {
		t.Error("embedded context.Context mismatch")
	}
}

func TestAppContext_GetContext(t *testing.T) {
	baseCtx := stdcontext.Background()
	log := logger.New(logger.INFO, true)

	appCtx := appcontext.NewAppContext(baseCtx, log)

	if got := appCtx.GetContext(); got != baseCtx {
		t.Errorf("GetContext() = %v, want %v", got, baseCtx)
	}
}

func TestAppContext_GetLogger(t *testing.T) {
	baseCtx := stdcontext.Background()
	log := logger.New(logger.INFO, true)

	appCtx := appcontext.NewAppContext(baseCtx, log)

	if got := appCtx.GetLogger(); got != log {
		t.Errorf("GetLogger() = %v, want %v", got, log)
	}
}

func TestAppContext_ImplementsContext(t *testing.T) {
	baseCtx, cancel := stdcontext.WithCancel(stdcontext.Background())
	defer cancel()

	appCtx := appcontext.NewAppContext(baseCtx, logger.New(logger.INFO, true))

	var _ stdcontext.Context = appCtx

	cancel()

	select {
	case <-appCtx.Done():
	default:
		t.Error("expected Done() channel to be closed after cancel")
	}
	if appCtx.Err() != stdcontext.Canceled {
		t.Errorf("Err() = %v, want %v", appCtx.Err(), stdcontext.Canceled)
	}
}

func TestAppContext_ValuePropagation(t *testing.T) {
	type ctxKey string
	const key ctxKey = "test-key"

	baseCtx := stdcontext.WithValue(stdcontext.Background(), key, "test-value")
	appCtx := appcontext.NewAppContext(baseCtx, logger.New(logger.INFO, true))

	if got := appCtx.Value(key); got != "test-value" {
		t.Errorf("Value(%v) = %v, want %q", key, got, "test-value")
	}
}
