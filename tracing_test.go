package nakadi

import (
	"net/http"

	"net/http/httptest"
	"net/http/httptrace"
	"testing"

	basic "github.com/opentracing/basictracer-go"
	"github.com/stretchr/testify/assert"
)

func TestTracingTransport(t *testing.T) {
	options := basic.DefaultOptions()
	options.Recorder = basic.NewInMemoryRecorder()
	tracer := basic.NewWithOptions(options)
	for _, tt := range []struct {
		name           string
		tracingOptions TracingOptions
	}{
		{
			name: "All defaults",
		},
		{
			name:           "Empty TracingOptions",
			tracingOptions: TracingOptions{},
		},
		{
			name: "With opentracing",
			tracingOptions: TracingOptions{
				Tracer:        tracer,
				ComponentName: "nakadi"},
		},
		{
			name: "verbose opentracing",
			tracingOptions: TracingOptions{
				Tracer:        tracer,
				ComponentName: "nakadi",
				Verbose:       true},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.tracingOptions.Tracer != nil {
					if r.Header.Get("Ot-Tracer-Sampled") == "" ||
						r.Header.Get("Ot-Tracer-Traceid") == "" ||
						r.Header.Get("Ot-Tracer-Spanid") == "" {
						t.Errorf("One of the OT Tracer headers are missing: %v", r.Header)
					}
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			req := httptest.NewRequest("GET", ts.URL, nil)

			client := newHTTPClient(0, NewTracingMiddleware(&tt.tracingOptions))
			rt := client.Transport
			resp, err := rt.RoundTrip(req)

			if tt.tracingOptions.Verbose {
				assert.NotNil(t, httptrace.ContextClientTrace(resp.Request.Context()))
			}
			if err != nil {
				t.Errorf("Transport RoundTrip error : %v", err)
				return
			}

		})
	}
}

func TestGetOperationName(t *testing.T) {
	for _, tt := range []struct {
		name                  string
		reqPath               string
		reqMethod             string
		expectedOperationName string
	}{
		{
			"event-type management operation",
			"/event-types",
			"GET",
			"get_event-type",
		},
		{
			"event stream operation",
			"/event-types/test/events",
			"POST",
			"post_event",
		},
		{
			"subscription operation",
			"/subscriptions/id",
			"DELETE",
			"delete_subscription",
		},
	} {

		t.Run(tt.name, func(t *testing.T) {
			operationName := getOperationName(tt.reqPath, tt.reqMethod)
			assert.Equal(t, tt.expectedOperationName, operationName)
		})
	}
}
