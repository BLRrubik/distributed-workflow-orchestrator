package logger

import (
	"log/slog"
	"os"

	"github.com/lmittmann/tint"
)

type LogLevel byte

const (
	INFO  LogLevel = 0
	DEBUG LogLevel = 1
	WARN  LogLevel = 2
	ERROR LogLevel = 3
)

func toSlogLevel(level LogLevel) slog.Level {
	switch level {
	case DEBUG:
		return slog.LevelDebug
	case WARN:
		return slog.LevelWarn
	case ERROR:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type Logger struct {
	*slog.Logger
}

func New(level LogLevel, isDev bool) *Logger {
	handler := tint.NewTextHandler(os.Stdout, &tint.Options{
		Level: toSlogLevel(level),
	})

	if !isDev {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: toSlogLevel(level),
		})
	}

	return &Logger{
		Logger: slog.New(handler),
	}
}

func (l *Logger) Info(msg string, args ...any) {
	l.Logger.Info(msg, args...)
}

func (l *Logger) Debug(msg string, args ...any) {
	l.Logger.Debug(msg, args...)
}

func (l *Logger) Warn(msg string, args ...any) {
	l.Logger.Warn(msg, args...)
}

func (l *Logger) Error(msg string, args ...any) {
	l.Logger.Error(msg, args...)
}
