package logger

import "fmt"

type Level uint8

const (
	DEBUG Level = iota
	INFO
	WARN
)

func (l Level) String() string {
	switch l {
	case DEBUG:
		return "\033[92m" + "DEBUG" + "\033[0m"
	case INFO:
		return "\033[94m" + "INFO" + "\033[0m"
	case WARN:
		return "\033[91m" + "WARN" + "\033[0m"
	default:
		return "UNKNOWN"
	}
}

type Logger struct {
	maxLevel Level
}

func NewLogger(maxLevel Level) (*Logger, error) {
	return &Logger{
		maxLevel: maxLevel,
	}, nil
}

func (l *Logger) log(level Level, msg string) {
	if l.maxLevel <= level {
		fmt.Printf("[%s]: %s\n", level.String(), msg)
	}
}

func (l *Logger) Debug(msg string) {
	l.log(DEBUG, msg)
}

func (l *Logger) Info(msg string) {
	l.log(INFO, msg)
}

func (l *Logger) Warn(msg string) {
	l.log(WARN, msg)
}
