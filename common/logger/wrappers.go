package logger

import (
	"log/slog"
	"strings"
)

func Error(err error) slog.Attr {
	return slog.Any("error", err)
}

func String(key, msg string) slog.Attr {
	return slog.String(key, msg)
}

func Any(key string, data any) slog.Attr {
	return slog.Any(key, data)
}

func Int(key string, data int) slog.Attr {
	return slog.Int(key, data)
}

func UInt64(key string, data uint64) slog.Attr {
	return slog.Int(key, int(data))
}

func Bool(key string, data bool) slog.Attr {
	return slog.Bool(key, data)
}

func UInt32(key string, data uint32) slog.Attr {
	return slog.Int(key, int(data))
}

func Other(fields ...string) slog.Attr {
	var str strings.Builder

	if len(fields)%2 != 0 {
		return slog.String("other", strings.Join(fields, ", "))
	}

	str.Grow(20 * len(fields))

	for i := 0; i+1 < len(fields); i += 2 {
		str.WriteString(fields[i])
		str.WriteString(": ")
		str.WriteString(fields[i+1])

		if i+2 < len(fields) {
			str.WriteString(", ")
		}
	}

	return slog.String("other", str.String())
}
