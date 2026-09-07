package logger

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestToSlogLevel(t *testing.T) {
	tests := []struct {
		name  string
		level LogLevel
		want  slog.Level
	}{
		{"INFO", INFO, slog.LevelInfo},
		{"DEBUG", DEBUG, slog.LevelDebug},
		{"WARN", WARN, slog.LevelWarn},
		{"ERROR", ERROR, slog.LevelError},
		{"unknown defaults to INFO", LogLevel(99), slog.LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toSlogLevel(tt.level); got != tt.want {
				t.Errorf("toSlogLevel(%v) = %v, want %v", tt.level, got, tt.want)
			}
		})
	}
}

// captureStdout redirects os.Stdout for the duration of fn and returns
// everything written to it. The logger writes directly to os.Stdout, so
// there is no other way to observe its output.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	orig := os.Stdout

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}

	os.Stdout = w

	fn()

	if err = w.Close(); err != nil {
		t.Fatalf("w.Close() error = %v", err)
	}

	os.Stdout = orig

	var buf bytes.Buffer
	if _, err = io.Copy(&buf, r); err != nil {
		t.Fatalf("io.Copy() error = %v", err)
	}

	return buf.String()
}

func TestNew_NotNil(t *testing.T) {
	log := New(INFO, true)
	if log == nil {
		t.Fatal("New() returned nil")
	}

	if log.Logger == nil {
		t.Fatal("New().Logger is nil")
	}
}

func TestLogger_JSONHandler_WritesValidJSON(t *testing.T) {
	out := captureStdout(t, func() {
		log := New(INFO, false)
		log.Info("hello", String("k", "v"))
	})

	out = strings.TrimSpace(out)
	if out == "" {
		t.Fatal("expected output, got none")
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}

	if parsed["msg"] != "hello" {
		t.Errorf("msg = %v, want %q", parsed["msg"], "hello")
	}

	if parsed["k"] != "v" {
		t.Errorf("k = %v, want %q", parsed["k"], "v")
	}
}

func TestLogger_DevHandler_WritesNonJSON(t *testing.T) {
	out := captureStdout(t, func() {
		log := New(INFO, true)
		log.Info("hello")
	})

	out = strings.TrimSpace(out)
	if out == "" {
		t.Fatal("expected output, got none")
	}

	if json.Valid([]byte(out)) {
		t.Errorf("expected non-JSON tint output, got JSON: %s", out)
	}

	if !strings.Contains(out, "hello") {
		t.Errorf("output = %q, want it to contain %q", out, "hello")
	}
}

func TestLogger_LevelFiltering(t *testing.T) {
	out := captureStdout(t, func() {
		log := New(WARN, false)
		log.Debug("debug msg")
		log.Info("info msg")
		log.Warn("warn msg")
	})

	if strings.Contains(out, "debug msg") {
		t.Errorf("expected DEBUG message to be filtered out, got: %s", out)
	}

	if strings.Contains(out, "info msg") {
		t.Errorf("expected INFO message to be filtered out, got: %s", out)
	}

	if !strings.Contains(out, "warn msg") {
		t.Errorf("expected WARN message to be logged, got: %s", out)
	}
}

func TestLogger_AllLevelsEmitAtDebug(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		fn   func(log *Logger, msg string)
	}{
		{"Debug", "debug msg", func(l *Logger, msg string) { l.Debug(msg) }},
		{"Info", "info msg", func(l *Logger, msg string) { l.Info(msg) }},
		{"Warn", "warn msg", func(l *Logger, msg string) { l.Warn(msg) }},
		{"Error", "error msg", func(l *Logger, msg string) { l.Error(msg) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := captureStdout(t, func() {
				log := New(DEBUG, false)
				tt.fn(log, tt.msg)
			})
			if !strings.Contains(out, tt.msg) {
				t.Errorf("%s() output = %q, want it to contain %q", tt.name, out, tt.msg)
			}
		})
	}
}
