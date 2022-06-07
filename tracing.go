package nakadi

import (
	"net/http"
	"net/http/httptrace"
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

type TracingOptions struct {
	Tracer        opentracing.Tracer
	ComponentName string
	Verbose       bool
}

type TracingMiddleware struct {
	tr            *http.Transport
	tracer        opentracing.Tracer
	componentName string
	verbose       bool
}

func (t *TracingMiddleware) CloseIdleConnections() {
	t.tr.CloseIdleConnections()
}

// RoundTrip the request with tracing.
// Client traces are added as logs into the created span.
func (t *TracingMiddleware) RoundTrip(req *http.Request) (*http.Response, error) {
	var span opentracing.Span
	if t.componentName != "" {
		req, span = t.injectSpan(req)
		defer span.Finish()
		if t.verbose {
			req = injectRequestSpanLogs(req, span)
		}
		span.LogKV("http_do", "start")
	}
	rsp, err := t.tr.RoundTrip(req)
	if span != nil {
		span.LogKV("http_do", "stop")
		if rsp != nil {
			ext.HTTPStatusCode.Set(span, uint16(rsp.StatusCode))
		}
	}
	return rsp, err
}

func (t *TracingMiddleware) injectSpan(req *http.Request) (*http.Request, opentracing.Span) {
	parentSpan := opentracing.SpanFromContext(req.Context())
	var span opentracing.Span
	operationName := getOperationName(req.URL.Path, req.Method)

	if parentSpan != nil {
		req = req.WithContext(opentracing.ContextWithSpan(req.Context(), parentSpan))
		span = t.tracer.StartSpan(operationName, opentracing.ChildOf(parentSpan.Context()))
	} else {
		span = t.tracer.StartSpan(operationName)
	}

	// add Tags
	ext.Component.Set(span, t.componentName)
	ext.HTTPUrl.Set(span, req.URL.String())
	ext.HTTPMethod.Set(span, req.Method)
	ext.SpanKind.Set(span, "client")

	_ = t.tracer.Inject(span.Context(), opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(req.Header))

	return req, span
}

func injectRequestSpanLogs(req *http.Request, span opentracing.Span) *http.Request {
	trace := &httptrace.ClientTrace{
		ConnectStart: func(string, string) {
			span.LogKV("connect", "start")
		},
		GetConn: func(string) {
			span.LogKV("get_conn", "start")
		},
		WroteHeaders: func() {
			span.LogKV("wrote_headers", "done")
		},
		WroteRequest: func(wri httptrace.WroteRequestInfo) {
			if wri.Err != nil {
				span.LogKV("wrote_request", wri.Err.Error())
			} else {
				span.LogKV("wrote_request", "done")
			}
		},
		GotFirstResponseByte: func() {
			span.LogKV("got_first_byte", "done")
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
