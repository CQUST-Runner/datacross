package storage

import (
	"fmt"
	"io"
	"log"
	"os"
)

type Logger interface {
	Category(catetory string) Logger
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	Info(format string, args ...interface{})
}

type builtinLoggerAdapter struct {
	l   *log.Logger
	cat string
	out io.Writer
}

func newLogger(out io.Writer, cat string) *builtinLoggerAdapter {
	return &builtinLoggerAdapter{
		l:   log.New(out, cat, log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile|log.Lmsgprefix),
		cat: cat,
		out: out,
	}
}

func (a *builtinLoggerAdapter) Category(catetory string) Logger {
	// TODO: make sure a.out thread-safe
	return newLogger(a.out, catetory)
}

func (a *builtinLoggerAdapter) Warn(format string, args ...interface{}) {
	a.l.Output(2, fmt.Sprintf(" WARN "+format, args...))
}

func (a *builtinLoggerAdapter) Error(format string, args ...interface{}) {
	a.l.Output(2, fmt.Sprintf(" ERROR "+format, args...))
}

func (a *builtinLoggerAdapter) Info(format string, args ...interface{}) {
	a.l.Output(2, fmt.Sprintf(" INFO "+format, args...))
}

var logger Logger = newLogger(os.Stdout, "Core")

// suppress warning
var _ = logger

func SetLogger(l Logger) {
	logger = l
}
