package nakadi

import (
	"net/http"
	"net/http/httptrace"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.31.0"
	"go.opentelemetry.io/otel/trace"
)

type TracingOptions struct {
	Tracer        trace.Tracer
	ComponentName string
	Verbose       bool
}

type TracingMiddleware struct {
	tr            *http.Transport
	tracer        trace.Tracer
	componentName string
	verbose       bool
}

func (t *TracingMiddleware) CloseIdleConnections() {
	t.tr.CloseIdleConnections()
}

// RoundTrip the request with tracing.
// Client traces are added as logs into the created span.
func (t *TracingMiddleware) RoundTrip(req *http.Request) (*http.Response, error) {
	var span trace.Span
	if t.componentName != "" {
		req, span = t.injectSpan(req)
		defer span.End()
		if t.verbose {
			req = injectRequestSpanLogs(req, span)
		}
		span.SetAttributes(attribute.String("http_do", "start"))
	}
	rsp, err := t.tr.RoundTrip(req)
	if span != nil {
		span.SetAttributes(attribute.String("http_do", "stop"))
		if rsp != nil {
			span.SetAttributes(semconv.HTTPResponseStatusCode(rsp.StatusCode))
		}
	}
	return rsp, err
}

func (t *TracingMiddleware) injectSpan(req *http.Request) (*http.Request, trace.Span) {
	ctx := req.Context()
	operationName := getOperationName(req.URL.Path, req.Method)

	ctx, span := t.tracer.Start(ctx, operationName,
		trace.WithAttributes(
			// https://opentelemetry.io/docs/specs/semconv/http/http-spans/#http-client-span
			attribute.String("otel.component.name", t.componentName),
			attribute.String("url.full", req.URL.String()),
			attribute.String("http.request.method", req.Method),

			// The attributes below are no longer valid according to semantic convention.
			// Only here to ensure backward compatibility.
			attribute.String("component", t.componentName),
			attribute.String("http.url", req.URL.String()),
			attribute.String("http.method", req.Method),
		),
		trace.WithSpanKind(trace.SpanKindClient))

	req = req.WithContext(ctx)

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	return req, span
}

func injectRequestSpanLogs(req *http.Request, span trace.Span) *http.Request {
	trace := &httptrace.ClientTrace{
		ConnectStart: func(string, string) {
			span.SetAttributes(attribute.String("connect", "start"))
		},
		GetConn: func(string) {
			span.SetAttributes(attribute.String("get_conn", "start"))
		},
		WroteHeaders: func() {
			span.SetAttributes(attribute.String("wrote_headers", "done"))
		},
		WroteRequest: func(wri httptrace.WroteRequestInfo) {
			if wri.Err != nil {
				span.SetAttributes(attribute.String("wrote_request", wri.Err.Error()))
			} else {
				span.SetAttributes(attribute.String("wrote_request", "done"))
			}
		},
		GotFirstResponseByte: func() {
			span.SetAttributes(attribute.String("got_first_byte", "done"))
		},
	}
	return req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
}

func NewTracingMiddleware(options *TracingOptions) Middleware {
	return func(transport *http.Transport) http.RoundTripper {
		return &TracingMiddleware{
			tr:            transport,
			tracer:        options.Tracer,
			componentName: options.ComponentName,
			verbose:       options.Verbose}
	}
}

func getOperationName(reqPath, reqMethod string) string {
	operationName := strings.ToLower(reqMethod)
	switch {
	case strings.Contains(reqPath, "subscriptions"):
		operationName = operationName + "_subscription"
	case strings.Contains(reqPath, "events"):
		operationName = operationName + "_event"
	case strings.Contains(reqPath, "event-types"):
		operationName = operationName + "_event-type"
	}

	return operationName
}
