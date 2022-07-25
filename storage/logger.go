package storage

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	glog "gorm.io/gorm/logger"
)

type gormLoggerImpl struct {
	logger Logger
}

func (l *gormLoggerImpl) Init(logger Logger) {
	l.logger = logger.AddSkip(1)
}

func (l *gormLoggerImpl) LogMode(lvl glog.LogLevel) glog.Interface {
	switch lvl {
	case glog.Silent:
		return &gormLoggerImpl{logger: newLogger(ioutil.Discard, "GORM")}
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
	Category(catetory string) Logger
	AddSkip(skip int) Logger
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	Info(format string, args ...interface{})
}

type builtinLoggerAdapter struct {
	l         *log.Logger
	cat       string
	out       io.Writer
	extraSkip int
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

func (a *builtinLoggerAdapter) AddSkip(skip int) Logger {
	l := newLogger(a.out, a.cat)
	l.extraSkip = a.extraSkip + skip
	return l
}

func (a *builtinLoggerAdapter) Warn(format string, args ...interface{}) {
	_ = a.l.Output(2+a.extraSkip, fmt.Sprintf(" WARN "+format, args...))
}

func (a *builtinLoggerAdapter) Error(format string, args ...interface{}) {
	_ = a.l.Output(2+a.extraSkip, fmt.Sprintf(" ERROR "+format, args...))
}

func (a *builtinLoggerAdapter) Info(format string, args ...interface{}) {
	_ = a.l.Output(2+a.extraSkip, fmt.Sprintf(" INFO "+format, args...))
}

var logger Logger = newLogger(os.Stdout, "Core")

// suppress warning
var _ = logger

func SetLogger(l Logger) {
	logger = l
}
