package http_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
	httpexecutor "github.com/blrrubik/distributed-workflow-orchestrator/infrastructure/worker/executor/http"
)

func TestExecutor_Execute_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "pong")
	}))
	defer srv.Close()

	e := &httpexecutor.Executor{}

	result, err := e.Execute(t.Context(), domain.TaskSpec{
		Payload: []byte(fmt.Sprintf(`{"url": %q}`, srv.URL)),
	}, time.Second)

	assert.NoError(t, err)
	assert.Equal(t, 0, result.ExitCode)
	assert.Equal(t, "pong", result.Stdout)
	assert.Empty(t, result.Error)
}

func TestExecutor_Execute_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	e := &httpexecutor.Executor{}

	result, err := e.Execute(t.Context(), domain.TaskSpec{
		Payload: []byte(fmt.Sprintf(`{"url": %q}`, srv.URL)),
	}, time.Second)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, result.ExitCode)
	assert.NotEmpty(t, result.Error)
}

func TestExecutor_Execute_MissingURL(t *testing.T) {
	e := &httpexecutor.Executor{}

	result, err := e.Execute(t.Context(), domain.TaskSpec{
		Payload: []byte(`{}`),
	}, time.Second)

	assert.Error(t, err)
	assert.Equal(t, "url not found in task spec", result.Error)
}

func TestExecutor_Execute_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()

	e := &httpexecutor.Executor{}

	result, err := e.Execute(t.Context(), domain.TaskSpec{
		Payload: []byte(fmt.Sprintf(`{"url": %q}`, srv.URL)),
	}, 20*time.Millisecond)

	assert.NoError(t, err)
	assert.Equal(t, -1, result.ExitCode)
	assert.Equal(t, "task timed out", result.Error)
}

func TestExecutor_Execute_PostWithBody(t *testing.T) {
	var gotMethod, gotBody string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method

		buf := make([]byte, 4)
		n, _ := r.Body.Read(buf)
		gotBody = string(buf[:n])
	}))
	defer srv.Close()

	e := &httpexecutor.Executor{}

	_, err := e.Execute(t.Context(), domain.TaskSpec{
		Payload: []byte(fmt.Sprintf(`{"url": %q, "method": "POST", "body": "ping"}`, srv.URL)),
	}, time.Second)

	assert.NoError(t, err)
	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "ping", gotBody)
}
