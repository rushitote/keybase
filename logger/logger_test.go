package logger

import (
	"testing"
)

func TestLogger(t *testing.T) {
	l := NewLogger(DEBUG)

	l.Debug("debug message")
	l.Info("info message")
	l.Warn("warn message")
}
