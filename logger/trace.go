package logger

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func fromContext(ctx context.Context, g *zap.SugaredLogger) *zap.SugaredLogger {
	l := g

	if spl, ok := loggerWithOtelSpanContext(l, trace.SpanFromContext(ctx)); ok {
		return spl
	}

	return l
}

func loggerWithOtelSpanContext(l *zap.SugaredLogger, span trace.Span) (*zap.SugaredLogger, bool) {
	if span == nil || !span.SpanContext().HasSpanID() {
		return l, false
	}
	return l.Desugar().With(tracingFieldsOtel(span)...).Sugar(), true
}

func tracingFieldsOtel(span trace.Span) []zapcore.Field {
	if span != nil && span.SpanContext().HasSpanID() {
		return []zapcore.Field{
			zap.Stringer("trace_id", span.SpanContext().TraceID()),
			zap.Stringer("span_id", span.SpanContext().SpanID()),
		}
	}
	return nil
}
