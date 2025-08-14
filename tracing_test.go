package nakadi

import (
	"net/http"

	"net/http/httptest"
	"net/http/httptrace"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestTracingTransport(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	traceProvider := trace.NewTracerProvider(
		trace.WithSyncer(exporter),
	)

	otel.SetTracerProvider(traceProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tracer := traceProvider.Tracer("test-tracer")

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
			name: "With otel",
			tracingOptions: TracingOptions{
				Tracer:        tracer,
				ComponentName: "nakadi",
			},
		},
		{
			name: "verbose otel",
			tracingOptions: TracingOptions{
				Tracer:        tracer,
				ComponentName: "nakadi",
				Verbose:       true,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			exporter.Reset()

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.tracingOptions.Tracer != nil {
					if r.Header.Get("traceparent") == "" {
						t.Errorf("traceparent header is missing: %v", r.Header)
					}
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			req := httptest.NewRequest(http.MethodGet, ts.URL, nil)

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

			if tt.tracingOptions.Tracer != nil {
				spans := exporter.GetSpans()
				assert.Greater(t, len(spans), 0, "Expected at least one span to be created")

				if len(spans) > 0 {
					span := spans[0]
					assert.NotEmpty(t, span.Name, "Span should have a name")

					// Check for HTTP semantic convention attributes.
					attrs := span.Attributes
					hasHTTPMethod := false
					hasURL := false

					for _, attr := range attrs {
						switch string(attr.Key) {
						case "http.request.method":
							hasHTTPMethod = true
						case "url.full":
							hasURL = true
						}
					}

					assert.True(t, hasHTTPMethod, "Span should have 'http.request.method' attribute")
					assert.True(t, hasURL, "Span should have 'url.full' attribute")
				}
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
