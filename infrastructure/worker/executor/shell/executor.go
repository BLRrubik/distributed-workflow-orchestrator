package shell

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/common/domain"
)

type Executor struct{}

// payload — форма, которую shell-задача ожидает в domain.TaskSpec.Payload.
type payload struct {
	Command string `json:"command"`
	Args    string `json:"args"`
}

func (e *Executor) Execute(ctx context.Context, spec domain.TaskSpec, timeout time.Duration) (domain.TaskResult, error) {
	var p payload
	if err := json.Unmarshal(spec.Payload, &p); err != nil {
		return domain.TaskResult{
			Error: fmt.Sprintf("invalid shell payload: %s", err.Error()),
		}, fmt.Errorf("invalid shell payload: %w", err)
	}

	if p.Command == "" {
		return domain.TaskResult{
			Error: "command not found in task spec",
		}, errors.New("command not found in task spec")
	}

	outWriter := bytes.NewBuffer(nil)
	errWriter := bytes.NewBuffer(nil)

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(timeoutCtx, p.Command, p.Args)
	cmd.Stdout = outWriter
	cmd.Stderr = errWriter

	start := time.Now()
	err := cmd.Run()
	duration := time.Since(start)

	result := domain.TaskResult{
		Stdout:   outWriter.String(),
		Stderr:   errWriter.String(),
		Duration: duration,
	}

	switch {
	case errors.Is(timeoutCtx.Err(), context.DeadlineExceeded):
		// таймаут: процесс убит через ctx, exec.CommandContext сам шлёт kill
		result.ExitCode = -1
		result.Error = "task timed out"

		return result, nil // это НЕ ошибка исполнителя — это нормальный результат "не успел"
	case err != nil:
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			// команда запустилась и завершилась с ненулевым кодом — это НЕ Go-ошибка,
			result.ExitCode = exitErr.ExitCode()

			return result, nil
		}

		// сюда попадаем, только если команда вообще не смогла стартовать
		// (бинарник не найден, нет прав на исполнение и т.п.) — это уже реальная ошибка Executor'а
		return result, fmt.Errorf("failed to start command: %w", err)
	default:
		result.ExitCode = 0

		return result, nil
	}
}
