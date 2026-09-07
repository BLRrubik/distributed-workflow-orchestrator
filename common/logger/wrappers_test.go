package logger

import (
	"errors"
	"log/slog"
	"testing"
)

func TestError(t *testing.T) {
	err := errors.New("boom")
	attr := Error(err)

	if attr.Key != "error" {
		t.Errorf("Key = %q, want %q", attr.Key, "error")
	}

	if attr.Value.Any() != err {
		t.Errorf("Value = %v, want %v", attr.Value.Any(), err)
	}
}

func TestString(t *testing.T) {
	attr := String("key", "value")

	if attr.Key != "key" {
		t.Errorf("Key = %q, want %q", attr.Key, "key")
	}

	if attr.Value.Kind() != slog.KindString || attr.Value.String() != "value" {
		t.Errorf("Value = %v, want %q", attr.Value, "value")
	}
}

func TestAny(t *testing.T) {
	type payload struct{ N int }

	data := payload{N: 42}
	attr := Any("key", data)

	if attr.Key != "key" {
		t.Errorf("Key = %q, want %q", attr.Key, "key")
	}

	if attr.Value.Any() != data {
		t.Errorf("Value = %v, want %v", attr.Value.Any(), data)
	}
}

func TestInt(t *testing.T) {
	attr := Int("key", -5)

	if attr.Key != "key" {
		t.Errorf("Key = %q, want %q", attr.Key, "key")
	}

	if attr.Value.Kind() != slog.KindInt64 || attr.Value.Int64() != -5 {
		t.Errorf("Value = %v, want %d", attr.Value, -5)
	}
}

func TestUInt64(t *testing.T) {
	attr := UInt64("key", 42)

	if attr.Key != "key" {
		t.Errorf("Key = %q, want %q", attr.Key, "key")
	}

	if attr.Value.Kind() != slog.KindInt64 || attr.Value.Int64() != 42 {
		t.Errorf("Value = %v, want %d", attr.Value, 42)
	}
}

func TestBool(t *testing.T) {
	attr := Bool("key", true)

	if attr.Key != "key" {
		t.Errorf("Key = %q, want %q", attr.Key, "key")
	}

	if attr.Value.Kind() != slog.KindBool || !attr.Value.Bool() {
		t.Errorf("Value = %v, want %v", attr.Value, true)
	}
}

func TestUInt32(t *testing.T) {
	attr := UInt32("key", 7)

	if attr.Key != "key" {
		t.Errorf("Key = %q, want %q", attr.Key, "key")
	}

	if attr.Value.Kind() != slog.KindInt64 || attr.Value.Int64() != 7 {
		t.Errorf("Value = %v, want %d", attr.Value, 7)
	}
}

func TestOther_EvenFields(t *testing.T) {
	attr := Other("a", "1", "b", "2")

	if attr.Key != "other" {
		t.Errorf("Key = %q, want %q", attr.Key, "other")
	}

	want := "a: 1, b: 2"

	if got := attr.Value.String(); got != want {
		t.Errorf("Value = %q, want %q", got, want)
	}
}

func TestOther_SingleField(t *testing.T) {
	attr := Other("a", "1")

	want := "a: 1"

	if got := attr.Value.String(); got != want {
		t.Errorf("Value = %q, want %q", got, want)
	}
}

func TestOther_OddFields(t *testing.T) {
	attr := Other("a", "1", "b")

	if attr.Key != "other" {
		t.Errorf("Key = %q, want %q", attr.Key, "other")
	}

	want := "a, 1, b"

	if got := attr.Value.String(); got != want {
		t.Errorf("Value = %q, want %q", got, want)
	}
}

func TestOther_Empty(t *testing.T) {
	attr := Other()

	if got := attr.Value.String(); got != "" {
		t.Errorf("Value = %q, want empty string", got)
	}
}
