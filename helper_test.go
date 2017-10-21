package nakadi

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHTTPClient(t *testing.T) {
	timeout := 20 * time.Second
	client := newHTTPClient(timeout)

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
