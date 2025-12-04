package logger

import (
	"context"
	"os"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// global logger instance.
	global         *zap.SugaredLogger
	mu             sync.RWMutex
	disableLogger  atomic.Bool
	defaultLevel   = zap.NewAtomicLevelAt(zap.DebugLevel)
	generationArgs = []any{"@gen", "1"}
)

func init() {
	SetLogger(newSugaredLogger(defaultLevel))
}

// SetLogger sets to global logger a new *zap.SugaredLogger.
func SetLogger(l *zap.SugaredLogger) {
	mu.Lock()
	global = l
	mu.Unlock()
}

// GetLogger returns current global logger.
func GetLogger() *zap.SugaredLogger {
	mu.RLock()
	defer mu.RUnlock()
	return global
}

// DisableLogger turn off all logs, globally.
func DisableLogger() {
	disableLogger.Store(true)
}

// IsDisable checks the status of the logger (true - disabled, false - enabled)
func IsDisable() bool {
	return disableLogger.Load()
}

func newSugaredLogger(level zapcore.LevelEnabler, options ...zap.Option) *zap.SugaredLogger {
	if level == nil {
		level = defaultLevel
	}
	return zap.New(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(zapcore.EncoderConfig{
				TimeKey:        "ts",
				LevelKey:       "level",
				NameKey:        "logger",
				CallerKey:      "caller",
				MessageKey:     "message",
				StacktraceKey:  "stacktrace",
				LineEnding:     zapcore.DefaultLineEnding,
				EncodeLevel:    capitalLevelEncoder,
				EncodeTime:     zapcore.ISO8601TimeEncoder,
				EncodeDuration: zapcore.SecondsDurationEncoder,
				EncodeCaller:   zapcore.ShortCallerEncoder,
			}),
			zapcore.AddSync(os.Stdout),
			level,
		),
		options...,
	).Sugar().With(generationArgs...)
}

func capitalLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	level := ""
	switch l {
	case zapcore.ErrorLevel:
		level = "ERR"
	case zapcore.WarnLevel:
		level = "WARNING"
	default:
		level = l.CapitalString()
	}
	enc.AppendString(level)
}

// Debug ...
func Debug(ctx context.Context, args ...any) {
	if !IsDisable() {
		fromContext(ctx, GetLogger()).Debug(args...)
	}
}

// Debugf ...
func Debugf(ctx context.Context, format string, args ...any) {
	if !IsDisable() {
		fromContext(ctx, GetLogger()).Debugf(format, args...)
	}
}

// Info ...
func Info(ctx context.Context, args ...any) {
	if !IsDisable() {
		fromContext(ctx, GetLogger()).Info(args...)
	}
}

// Infof ...
func Infof(ctx context.Context, format string, args ...any) {
	if !IsDisable() {
		fromContext(ctx, GetLogger()).Infof(format, args...)
	}
}

// Warn ...
func Warn(ctx context.Context, args ...any) {
	if !IsDisable() {
		fromContext(ctx, GetLogger()).Warn(args...)
	}
}

// Warnf ...
func Warnf(ctx context.Context, format string, args ...any) {
	if !IsDisable() {
		fromContext(ctx, GetLogger()).Warnf(format, args...)
	}
}

// Error ...
func Error(ctx context.Context, args ...any) {
	if !IsDisable() {
		fromContext(ctx, GetLogger()).Error(args...)
	}
}

// Errorf ...
func Errorf(ctx context.Context, format string, args ...any) {
	if !IsDisable() {
		fromContext(ctx, GetLogger()).Errorf(format, args...)
	}
}

// Fatal ...
func Fatal(ctx context.Context, args ...any) {
	if !IsDisable() {
		fromContext(ctx, GetLogger()).Error(args...)
	}
}

// Fatalf ...
func Fatalf(ctx context.Context, format string, args ...any) {
	if !IsDisable() {
		fromContext(ctx, GetLogger()).Fatalf(format, args...)
	}
}
