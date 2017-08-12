// Copyright (c) 2017, A. Stoewer <adrian.stoewer@rz.ifi.lmu.de>
// All rights reserved.

package nakadi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/jarcoal/httpmock.v1"
)

func TestEventType_Marshal(t *testing.T) {
	eventType := &EventType{}
	expected := helperLoadTestData(t, "event-type-complete.json", eventType)

	serialized, err := json.Marshal(eventType)
	require.NoError(t, err)
	assert.JSONEq(t, string(expected), string(serialized))
}

func TestHttpEventTypeManager_Get(t *testing.T) {
	expected := &EventType{}
	serialized := helperLoadTestData(t, "event-type-complete.json", expected)

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	api := NewEvents(client)
	url := fmt.Sprintf("%s/event-types/%s", defaultNakadiURL, expected.Name)

	t.Run("fail connection error", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewErrorResponder(assert.AnError))

		_, err := api.Get(expected.Name)
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail decode error", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusNotFound, ""))

		_, err := api.Get(expected.Name)
		require.Error(t, err)
		assert.Regexp(t, "unable to decode response body", err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		problem := `{"detail": "not found"}`
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusNotFound, problem))

		_, err := api.Get(expected.Name)
		require.Error(t, err)
		assert.Regexp(t, "not found", err)
	})

	t.Run("fail decode response", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusOK, ""))

		_, err := api.Get(expected.Name)
		require.Error(t, err)
		assert.Regexp(t, "unable to decode response body", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewBytesResponder(http.StatusOK, serialized))

		requested, err := api.Get(expected.Name)
		require.NoError(t, err)
		assert.Equal(t, expected, requested)
	})
}

func TestHttpEventTypeManager_List(t *testing.T) {
	expected := []*EventType{}
	serialized := helperLoadTestData(t, "event-types-complete.json", &expected)

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	api := NewEvents(client)
	url := fmt.Sprintf("%s/event-types", defaultNakadiURL)

	t.Run("fail connection error", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewErrorResponder(assert.AnError))

		_, err := api.List()
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail decode error", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusInternalServerError, ""))

		_, err := api.List()
		require.Error(t, err)
		assert.Regexp(t, "unable to decode response body", err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		problem := `{"detail": "not found"}`
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusInternalServerError, problem))

		_, err := api.List()
		require.Error(t, err)
		assert.Regexp(t, "not found", err)
	})

	t.Run("fail decode response", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusOK, ""))

		_, err := api.List()
		require.Error(t, err)
		assert.Regexp(t, "unable to decode response body", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewBytesResponder(http.StatusOK, serialized))

		requested, err := api.List()
		require.NoError(t, err)
		assert.Equal(t, expected, requested)
	})
}

func TestHttpEventTypeManager_Save(t *testing.T) {
	eventType := &EventType{}
	serialized := helperLoadTestData(t, "event-type-complete.json", eventType)

	client := &Client{
		nakadiURL:     defaultNakadiURL,
		httpClient:    http.DefaultClient,
		tokenProvider: func() (string, error) { return "token", nil }}
	api := NewEvents(client)
	url := fmt.Sprintf("%s/event-types/%s", defaultNakadiURL, eventType.Name)

	t.Run("fail connection error", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("PUT", url, httpmock.NewErrorResponder(assert.AnError))

		_, err := api.Save(eventType)
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		problem := `{"detail": "not found"}`
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("PUT", url, httpmock.NewStringResponder(http.StatusNotFound, problem))

		_, err := api.Save(eventType)
		require.Error(t, err)
		assert.Regexp(t, "not found", err)
	})

	t.Run("fail decode body", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("PUT", url, httpmock.NewStringResponder(http.StatusOK, ""))

		_, err := api.Save(eventType)
		require.Error(t, err)
		assert.Regexp(t, "unable to decode response body", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("PUT", url, httpmock.Responder(func(r *http.Request) (*http.Response, error) {
			uploaded := &EventType{}
			err := json.NewDecoder(r.Body).Decode(uploaded)
			require.NoError(t, err)
			assert.Equal(t, eventType, uploaded)
			return httpmock.NewBytesResponse(http.StatusOK, serialized), nil
		}))

		requested, err := api.Save(eventType)
		require.NoError(t, err)
		assert.Equal(t, eventType, requested)
	})
}

func TestHttpEventTypeManager_Delete(t *testing.T) {
	name := "test-event.change"

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	api := NewEvents(client)
	url := fmt.Sprintf("%s/event-types/%s", defaultNakadiURL, name)

	t.Run("fail connection error", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("DELETE", url, httpmock.NewErrorResponder(assert.AnError))

		err := api.Delete(name)
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail decode body", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("DELETE", url, httpmock.NewStringResponder(http.StatusNotFound, ""))

		err := api.Delete(name)
		require.Error(t, err)
		assert.Regexp(t, "unable to decode response body", err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		problem := `{"detail": "not found"}`
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("DELETE", url, httpmock.NewStringResponder(http.StatusNotFound, problem))

		err := api.Delete(name)
		require.Error(t, err)
		assert.Regexp(t, "not found", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("DELETE", url, httpmock.NewStringResponder(http.StatusNoContent, ""))

		err := api.Delete(name)
		assert.NoError(t, err)
	})
}
