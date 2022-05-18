package nakadi

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v3"
	basic "github.com/opentracing/basictracer-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHTTPClient(t *testing.T) {
	timeout := 20 * time.Second
	client := newHTTPClient(timeout, &TracingOptions{})

	require.NotNil(t, client)
	assert.Equal(t, timeout, client.Timeout)
}

func TestNewHTTPStream(t *testing.T) {
	timeout := 20 * time.Second
	client := newHTTPStream(timeout, &TracingOptions{})

	require.NotNil(t, client)
	assert.Equal(t, 0*time.Second, client.Timeout)
}

func TestProblemJSON_Marshal(t *testing.T) {
	problem := &problemJSON{}
	expected := helperLoadTestData(t, "problem-json.json", problem)

	serialized, err := json.Marshal(problem)
	require.NoError(t, err)
	assert.JSONEq(t, string(expected), string(serialized))
}

func TestErrorJSON_Marshal(t *testing.T) {
	errJSON := &errorJSON{}
	expected := helperLoadTestData(t, "error-json.json", errJSON)

	serialized, err := json.Marshal(errJSON)
	require.NoError(t, err)
	assert.JSONEq(t, string(expected), string(serialized))
}

func TestBackOffConfiguration_createBackOff(t *testing.T) {

	t.Run("stop backoff", func(t *testing.T) {
		backOffConf := backOffConfiguration{
			Retry:                false,
			InitialRetryInterval: 1 * time.Millisecond,
			MaxRetryInterval:     1 * time.Second,
			MaxElapsedTime:       1 * time.Minute}

		backOff := backOffConf.create()
		assert.IsType(t, &backoff.StopBackOff{}, backOff)
	})

	t.Run("exponential backoff", func(t *testing.T) {
		backOffConf := backOffConfiguration{
			Retry:                true,
			InitialRetryInterval: 1 * time.Millisecond,
			MaxRetryInterval:     1 * time.Second,
			MaxElapsedTime:       1 * time.Minute}

		backOff := backOffConf.create()
		require.IsType(t, &backoff.ExponentialBackOff{}, backOff)

		expBackOff := backOff.(*backoff.ExponentialBackOff)
		assert.Equal(t, 1*time.Millisecond, expBackOff.InitialInterval)
		assert.Equal(t, 1*time.Second, expBackOff.MaxInterval)
		assert.Equal(t, 1*time.Minute, expBackOff.MaxElapsedTime)
	})
}

func helperLoadTestData(t *testing.T, name string, target interface{}) []byte {
	path := filepath.Join("testdata", name)
	bytes, err := ioutil.ReadFile(path)
	require.NoError(t, err)
	if target != nil {
		err = json.Unmarshal(bytes, target)
		require.NoError(t, err)
	}
	return bytes
}

func helperMakeCounter(n int) chan int {
	counter := make(chan int)
	go func() {
		for i := 0; i <= n; i++ {
			counter <- i
		}
		close(counter)
	}()
	return counter
}

func TestTransport(t *testing.T) {
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
				SpanName:      "span",
				ComponentName: "nakadi"},
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

			client := newHTTPClient(0, &tt.tracingOptions)
			rt := client.Transport

			_, err := rt.RoundTrip(req)
			if err != nil {
				t.Errorf("Transport RoundTrip error : %v", err)
				return
			}

		})
	}
}

// brokenBodyReader is an implementation of ReadCloser interface to be used for
// mocking errors while reading from body
type brokenBodyReader struct{}

func (brokenBodyReader) Read(p []byte) (n int, err error) { return 0, assert.AnError }
func (brokenBodyReader) Close() error                     { return nil }
