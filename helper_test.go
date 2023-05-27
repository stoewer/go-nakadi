package nakadi

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHTTPClient(t *testing.T) {
	timeout := 20 * time.Second
	client := newHTTPClient(timeout, func(transport *http.Transport) http.RoundTripper { return transport })

	require.NotNil(t, client)
	assert.Equal(t, timeout, client.Timeout)
}

func TestNewHTTPStream(t *testing.T) {
	timeout := 20 * time.Second
	client := newHTTPStream(timeout)

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
	bytes, err := os.ReadFile(path)
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

// brokenBodyReader is an implementation of ReadCloser interface to be used for
// mocking errors while reading from body
type brokenBodyReader struct{}

func (brokenBodyReader) Read(_ []byte) (n int, err error) {
	return 0, assert.AnError
}

func (brokenBodyReader) Close() error {
	return nil
}
