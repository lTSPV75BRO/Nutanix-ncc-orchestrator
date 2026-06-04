// Package trace is a thin, opt-in wrapper around OpenTelemetry tracing for the
// NCC orchestrator. Tracing is OFF unless an OTLP endpoint is configured, in
// which case spans are exported over OTLP/HTTP. When disabled, the global
// no-op tracer is used so Start* calls are effectively free and callers need
// no conditional logic.
//
// Enable by setting either OTEL_EXPORTER_OTLP_ENDPOINT (standard OTel env) or
// NCC_OTEL_ENABLED=1. All standard OTEL_* env vars (endpoint, headers,
// protocol) are honoured by the underlying exporter.
package trace

import (
	"context"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const tracerName = "goncc/ncc-orchestrator"

// Enabled reports whether OTLP tracing should be initialised.
func Enabled() bool {
	if truthy(os.Getenv("NCC_OTEL_ENABLED")) {
		return true
	}
	return strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) != "" ||
		strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")) != ""
}

func truthy(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// Init configures the global tracer provider with an OTLP/HTTP exporter when
// tracing is enabled. It always returns a non-nil shutdown function (a no-op
// when disabled) so callers can unconditionally `defer shutdown(ctx)`.
func Init(ctx context.Context, serviceVersion string) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	if !Enabled() {
		return noop, nil
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return noop, err
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", "ncc-orchestrator"),
		attribute.String("service.version", serviceVersion),
	))
	if err != nil {
		res = resource.Default()
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// StartCluster begins a span covering a single cluster's NCC run. The returned
// context must be threaded into downstream calls so child spans nest
// correctly; call span.End() when the cluster work completes.
func StartCluster(ctx context.Context, cluster string) (context.Context, oteltrace.Span) {
	return otel.Tracer(tracerName).Start(ctx, "cluster.run",
		oteltrace.WithAttributes(attribute.String("ncc.cluster", cluster)))
}

// Start begins a generic span by name.
func Start(ctx context.Context, name string) (context.Context, oteltrace.Span) {
	return otel.Tracer(tracerName).Start(ctx, name)
}

// RecordError marks the span as failed and records the error (no-op for a
// non-recording span or nil error).
func RecordError(span oteltrace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
