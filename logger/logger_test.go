package logger

import (
	"testing"

	"github.com/matryer/is"
)

func TestLogger(t *testing.T) {
	is := is.New(t)
	l, err := NewLogger(DEBUG)
	is.NoErr(err)

	l.Debug("debug message")
	l.Info("info message")
	l.Warn("warn message")
}
