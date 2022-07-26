package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	glog "gorm.io/gorm/logger"
)

type gormLoggerImpl struct {
	logger Logger
}

func (l *gormLoggerImpl) Init(logger Logger) {
	l.logger = logger.Category("GORM").AddSkip(1).SetLevel(Error)
}

func (l *gormLoggerImpl) LogMode(lvl glog.LogLevel) glog.Interface {
	switch lvl {
	case glog.Silent:
		return &gormLoggerImpl{logger: l.logger.SetLevel(Silent)}
	default:
		return l
	}
}

func (l *gormLoggerImpl) Info(c context.Context, fmt string, args ...interface{}) {
	l.logger.Info(fmt, args...)
}

func (l *gormLoggerImpl) Warn(c context.Context, fmt string, args ...interface{}) {
	l.logger.Warn(fmt, args...)
}

func (l *gormLoggerImpl) Error(c context.Context, fmt string, args ...interface{}) {
	l.logger.Error(fmt, args...)
}

func (l *gormLoggerImpl) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	elapsed := time.Since(begin)
	sql, rows := fc()
	l.logger.Info("sql: %v, rowsAffected: %v, elapsed: %vms, err: %v", sql, rows, elapsed.Milliseconds(), err)
}

type Logger interface {
	SetLevel(level int) Logger
	Category(catetory string) Logger
	AddSkip(skip int) Logger
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	Info(format string, args ...interface{})
}

const (
	Silent = iota
	Error
	Warn
	Info
)

type builtinLoggerAdapter struct {
	lvl       int
	l         *log.Logger
	cat       string
	out       io.Writer
	extraSkip int
}

func newLogger(out io.Writer, cat string, extraSkip int, level int) *builtinLoggerAdapter {
	return &builtinLoggerAdapter{
		l:         log.New(out, cat, log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile|log.Lmsgprefix),
		cat:       cat,
		out:       out,
		extraSkip: extraSkip,
		lvl:       level,
	}
}

func (a *builtinLoggerAdapter) Category(catetory string) Logger {
	// TODO: make sure a.out thread-safe
	return newLogger(a.out, catetory, a.extraSkip, a.lvl)
}

func (a *builtinLoggerAdapter) AddSkip(skip int) Logger {
	l := newLogger(a.out, a.cat, a.extraSkip+skip, a.lvl)
	return l
}

func (a *builtinLoggerAdapter) SetLevel(level int) Logger {
	l := newLogger(a.out, a.cat, a.extraSkip, level)
	return l
}

func (a *builtinLoggerAdapter) Warn(format string, args ...interface{}) {
	if a.lvl < Warn {
		return
	}
	_ = a.l.Output(2+a.extraSkip, fmt.Sprintf(" WARN "+format, args...))
}

func (a *builtinLoggerAdapter) Error(format string, args ...interface{}) {
	if a.lvl < Error {
		return
	}
	_ = a.l.Output(2+a.extraSkip, fmt.Sprintf(" ERROR "+format, args...))
}

func (a *builtinLoggerAdapter) Info(format string, args ...interface{}) {
	if a.lvl < Info {
		return
	}
	_ = a.l.Output(2+a.extraSkip, fmt.Sprintf(" INFO "+format, args...))
}

var logger Logger = newLogger(os.Stdout, "Core", 0, Info)

// suppress warning
var _ = logger

func SetLogger(l Logger) {
	logger = l
}
