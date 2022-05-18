package nakadi

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/cenkalti/backoff/v3"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/pkg/errors"
)

const (
	// defaults used by http.DefaultTransport
	defaultKeepAlive       = 30 * time.Second
	defaultIdleConnTimeout = 90 * time.Second
	// nakadi specific timeouts
	nakadiHeartbeatInterval = 30 * time.Second
)

type Transport struct {
	tr            *http.Transport
	tracer        opentracing.Tracer
	spanName      string
	componentName string
}

func (t *Transport) CloseIdleConnections() {
	t.tr.CloseIdleConnections()
}

// RoundTrip the request with tracing.
// Client traces are added as logs into the created span.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	var span opentracing.Span
	if t.spanName != "" {
		req, span = t.injectSpan(req)
		defer span.Finish()
		req = injectClientTrace(req, span)
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

func (t *Transport) injectSpan(req *http.Request) (*http.Request, opentracing.Span) {
	parentSpan := opentracing.SpanFromContext(req.Context())
	var span opentracing.Span
	if parentSpan != nil {
		req = req.WithContext(opentracing.ContextWithSpan(req.Context(), parentSpan))
		span = t.tracer.StartSpan(t.spanName, opentracing.ChildOf(parentSpan.Context()))
	} else {
		span = t.tracer.StartSpan(t.spanName)
	}

	// add Tags
	ext.Component.Set(span, t.componentName)
	ext.HTTPUrl.Set(span, req.URL.String())
	ext.HTTPMethod.Set(span, req.Method)
	ext.SpanKind.Set(span, "client")

	_ = t.tracer.Inject(span.Context(), opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(req.Header))

	return req, span
}

func injectClientTrace(req *http.Request, span opentracing.Span) *http.Request {
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

// newHTTPClient crates an http client which is used for non streaming requests.
func newHTTPClient(timeout time.Duration, tracingOptions *TracingOptions) *http.Client {
	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   timeout,
			KeepAlive: defaultKeepAlive,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:        100,
		IdleConnTimeout:     defaultIdleConnTimeout,
		TLSHandshakeTimeout: timeout,
	}
	return &http.Client{
		Timeout: timeout,
		Transport: &Transport{
			tr:            t,
			tracer:        tracingOptions.Tracer,
			spanName:      tracingOptions.SpanName,
			componentName: tracingOptions.ComponentName},
	}
}

// newHTTPStream creates an http client which is used for streaming purposes.
func newHTTPStream(timeout time.Duration, tracingOptions *TracingOptions) *http.Client {
	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   timeout,
			KeepAlive: 2 * nakadiHeartbeatInterval,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:        100,
		IdleConnTimeout:     2 * nakadiHeartbeatInterval,
		TLSHandshakeTimeout: timeout,
	}
	return &http.Client{
		Transport: &Transport{
			tr:            t,
			tracer:        tracingOptions.Tracer,
			spanName:      tracingOptions.SpanName,
			componentName: tracingOptions.ComponentName},
	}
}

// problemJSON is used to decode error responses.
type problemJSON struct {
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
	Type   string `json:"type"`
}

type errorJSON struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// decodeResponseToError will try do decode into problemJSON then errorJSON
// and extract details from this defined formats.
// It will fallback to creating and error with message body
// Second parameter is an error message
func decodeResponseToError(buffer []byte, msg string) error {
	problem := problemJSON{}
	err := json.Unmarshal(buffer, &problem)
	if err == nil {
		return errors.Errorf("%s: %s", msg, problem.Detail)
	}
	errJSON := &errorJSON{}

	err = json.Unmarshal(buffer, &errJSON)
	if err == nil {
		return errors.Errorf("%s: %s", msg, errJSON.ErrorDescription)
	}
	return errors.Errorf("%s: %s", msg, string(buffer))
}

// backOffConfiguration holds initial values for the initialization of a backoff that can
// be used in retries.
type backOffConfiguration struct {
	// Whether to retry failed request or not.
	Retry bool
	// The initial (minimal) retry interval used for the exponential backoff.
	InitialRetryInterval time.Duration
	// MaxRetryInterval the maximum retry interval.
	MaxRetryInterval time.Duration
	// MaxElapsedTime is the maximum time spent on retries.
	MaxElapsedTime time.Duration
}

// create initializes a new backoff from configured parameters.
func (rc *backOffConfiguration) create() backoff.BackOff {
	if !rc.Retry {
		return &backoff.StopBackOff{}
	}

	back := backoff.NewExponentialBackOff()
	back.InitialInterval = rc.InitialRetryInterval
	back.MaxInterval = rc.MaxRetryInterval
	back.MaxElapsedTime = rc.MaxElapsedTime
	back.Reset()

	return back
}
