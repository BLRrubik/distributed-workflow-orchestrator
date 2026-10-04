package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
)

type Executor struct{}

// payload — форма, которую http-задача ожидает в domain.TaskSpec.Payload
// (например {"url": "...", "method": "POST", "body": "..."}).
type payload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Body    string            `json:"body"`
	Headers map[string]string `json:"headers"`
}

func (e *Executor) Execute(ctx context.Context, spec domain.TaskSpec, timeout time.Duration) (domain.TaskResult, error) {
	var p payload
	if err := json.Unmarshal(spec.Payload, &p); err != nil {
		return domain.TaskResult{
			Error: fmt.Sprintf("invalid http payload: %s", err.Error()),
		}, fmt.Errorf("invalid http payload: %w", err)
	}

	if p.URL == "" {
		return domain.TaskResult{
			Error: "url not found in task spec",
		}, errors.New("url not found in task spec")
	}

	method := p.Method
	if method == "" {
		method = http.MethodGet
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(timeoutCtx, method, p.URL, strings.NewReader(p.Body))
	if err != nil {
		// запрос даже не собрался (битый URL/метод) — это реальная ошибка Executor'а,
		// а не результат исполнения задачи (аналог "бинарник не найден" в shell.Executor)
		return domain.TaskResult{Error: err.Error()}, fmt.Errorf("build request: %w", err)
	}

	for key, value := range p.Headers {
		req.Header.Set(key, value)
	}

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	duration := time.Since(start)

	if err != nil {
		if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
			return domain.TaskResult{
				ExitCode: -1,
				Error:    "task timed out",
				Duration: duration,
			}, nil
		}

		return domain.TaskResult{
			ExitCode: -1,
			Error:    err.Error(),
			Duration: duration,
		}, nil
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)

	result := domain.TaskResult{
		Stdout:   string(body),
		Duration: duration,
	}

	switch {
	case readErr != nil:
		result.ExitCode = -1
		result.Error = fmt.Sprintf("read response body: %s", readErr.Error())
	case resp.StatusCode >= 400:
		result.ExitCode = resp.StatusCode
		result.Error = fmt.Sprintf("unexpected status code: %d", resp.StatusCode)
	}

	return result, nil
}
