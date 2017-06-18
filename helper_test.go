// Copyright (c) 2017, A. Stoewer <adrian.stoewer@rz.ifi.lmu.de>
// All rights reserved.

package nakadi

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func helperLoadGolden(t *testing.T, name string) []byte {
	path := filepath.Join("testdata", name) // relative path
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

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

func TestProblemJSON(t *testing.T) {
	serialized := helperLoadGolden(t, "problem-json.json.golden")
	problem := &problemJSON{
		Type:   "http://httpstatus.es/404",
		Title:  "Not Found",
		Status: http.StatusNotFound,
		Detail: "topic not found"}

	t.Run("marshal", func(t *testing.T) {
		result, err := json.Marshal(problem)
		require.NoError(t, err)
		assert.JSONEq(t, string(serialized), string(result))
	})

	t.Run("unmarshal", func(t *testing.T) {
		result := &problemJSON{}
		err := json.Unmarshal(serialized, result)
		require.NoError(t, err)
		assert.Equal(t, problem, result)
	})
}
