package logger

import (
	"os"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// global logger instance.
	global         *zap.SugaredLogger
	disableLogger  atomic.Bool
	defaultLevel   = zap.NewAtomicLevelAt(zap.DebugLevel)
	generationArgs = []any{"@gen", "1"}
)

func init() {
	SetLogger(newSugaredLogger(defaultLevel))
}

// SetLogger sets to global logger a new *zap.SugaredLogger.
func SetLogger(l *zap.SugaredLogger) {
	global = l
}

// GetLogger returns current global logger.
func GetLogger() *zap.SugaredLogger {
	return global
}

// DisableLogger turn off all logs, globally.
func DisableLogger() {
	disableLogger.Store(true)
}

// LoggerIsDisable checks the status of the logger (true - disabled, false - enabled)
func LoggerIsDisable() bool {
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
func Debug(args ...any) {
	if log := GetLogger(); !LoggerIsDisable() {
		log.Debug(args...)
	}
}

// Debugf ...
func Debugf(format string, args ...any) {
	if log := GetLogger(); !LoggerIsDisable() {
		log.Debugf(format, args...)
	}
}

// Info ...
func Info(args ...any) {
	if log := GetLogger(); !LoggerIsDisable() {
		log.Info(args...)
	}
}

// Infof ...
func Infof(format string, args ...any) {
	if log := GetLogger(); !LoggerIsDisable() {
		log.Infof(format, args...)
	}
}

// Warn ...
func Warn(args ...any) {
	if log := GetLogger(); !LoggerIsDisable() {
		log.Warn(args...)
	}
}

// Warnf ...
func Warnf(format string, args ...any) {
	if log := GetLogger(); !LoggerIsDisable() {
		log.Warnf(format, args...)
	}
}

// Error ...
func Error(args ...any) {
	if log := GetLogger(); !LoggerIsDisable() {
		log.Error(args...)
	}
}

// Errorf ...
func Errorf(format string, args ...any) {
	if log := GetLogger(); !LoggerIsDisable() {
		log.Errorf(format, args...)
	}
}

// Fatal ...
func Fatal(args ...any) {
	if log := GetLogger(); !LoggerIsDisable() {
		log.Fatal(args...)
	}
}

// Fatalf ...
func Fatalf(format string, args ...any) {
	if log := GetLogger(); !LoggerIsDisable() {
		log.Fatalf(format, args...)
	}
}
