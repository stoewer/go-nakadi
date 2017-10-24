package nakadi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"time"

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

func TestEventAPI_Get(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	expected := &EventType{}
	serialized := helperLoadTestData(t, "event-type-complete.json", expected)

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	api := NewEventAPI(client, nil)
	url := fmt.Sprintf("%s/event-types/%s", defaultNakadiURL, expected.Name)

	t.Run("fail connection error", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewErrorResponder(assert.AnError))

		_, err := api.Get(expected.Name)
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail decode error", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusNotFound, "most-likely-stacktrace"))

		_, err := api.Get(expected.Name)
		require.Error(t, err)
		assert.Regexp(t, "unable to request event types: most-likely-stacktrace", err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		problem := `{"detail": "not found"}`
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusNotFound, problem))

		_, err := api.Get(expected.Name)
		require.Error(t, err)
		assert.Regexp(t, "not found", err)
	})

	t.Run("fail decode response", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusOK, ""))

		_, err := api.Get(expected.Name)
		require.Error(t, err)
		assert.Regexp(t, "unable to decode response body", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewBytesResponder(http.StatusOK, serialized))

		requested, err := api.Get(expected.Name)
		require.NoError(t, err)
		assert.Equal(t, expected, requested)
	})
}

func TestEventAPI_List(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	expected := []*EventType{}
	serialized := helperLoadTestData(t, "event-types-complete.json", &expected)

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	api := NewEventAPI(client, nil)
	url := fmt.Sprintf("%s/event-types", defaultNakadiURL)

	t.Run("fail connection error", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewErrorResponder(assert.AnError))

		_, err := api.List()
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail decode error", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusInternalServerError, "most-likely-stacktrace"))

		_, err := api.List()
		require.Error(t, err)
		assert.Regexp(t, "unable to request event types: most-likely-stacktrace", err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		problem := `{"detail": "not found"}`
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusInternalServerError, problem))

		_, err := api.List()
		require.Error(t, err)
		assert.Regexp(t, "not found", err)
	})

	t.Run("fail decode response", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusOK, ""))

		_, err := api.List()
		require.Error(t, err)
		assert.Regexp(t, "unable to decode response body", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewBytesResponder(http.StatusOK, serialized))

		requested, err := api.List()
		require.NoError(t, err)
		assert.Equal(t, expected, requested)
	})
}

func TestEventAPI_Create(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	eventType := &EventType{}
	helperLoadTestData(t, "event-type-complete.json", eventType)

	client := &Client{
		nakadiURL:     defaultNakadiURL,
		httpClient:    http.DefaultClient,
		tokenProvider: func() (string, error) { return "token", nil }}
	api := NewEventAPI(client, nil)
	url := fmt.Sprintf("%s/event-types", defaultNakadiURL)

	t.Run("fail connection error", func(t *testing.T) {
		httpmock.RegisterResponder("POST", url, httpmock.NewErrorResponder(assert.AnError))

		err := api.Create(eventType)
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		problem := `{"detail": "not valid"}`
		httpmock.RegisterResponder("POST", url, httpmock.NewStringResponder(http.StatusConflict, problem))

		err := api.Create(eventType)
		require.Error(t, err)
		assert.Regexp(t, "not valid", err)
	})

	t.Run("fail to read body", func(t *testing.T) {
		responder := httpmock.ResponderFromResponse(&http.Response{
			Status:     strconv.Itoa(http.StatusBadRequest),
			StatusCode: http.StatusBadRequest,
			Body:       brokenBodyReader{},
		})
		httpmock.RegisterResponder("POST", url, responder)

		err := api.Create(eventType)
		require.Error(t, err)
		assert.Regexp(t, "unable to read response body", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.RegisterResponder("POST", url, httpmock.Responder(func(r *http.Request) (*http.Response, error) {
			uploaded := &EventType{}
			err := json.NewDecoder(r.Body).Decode(uploaded)
			require.NoError(t, err)
			assert.Equal(t, eventType, uploaded)
			return httpmock.NewStringResponse(http.StatusCreated, ""), nil
		}))

		err := api.Create(eventType)
		require.NoError(t, err)
	})
}

func TestEventAPI_Update(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	eventType := &EventType{}
	helperLoadTestData(t, "event-type-complete.json", eventType)

	client := &Client{
		nakadiURL:     defaultNakadiURL,
		httpClient:    http.DefaultClient,
		tokenProvider: func() (string, error) { return "token", nil }}
	api := NewEventAPI(client, nil)
	url := fmt.Sprintf("%s/event-types/%s", defaultNakadiURL, eventType.Name)

	t.Run("fail connection error", func(t *testing.T) {
		httpmock.RegisterResponder("PUT", url, httpmock.NewErrorResponder(assert.AnError))

		err := api.Update(eventType)
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		problem := `{"detail": "not found"}`
		httpmock.RegisterResponder("PUT", url, httpmock.NewStringResponder(http.StatusNotFound, problem))

		err := api.Update(eventType)
		require.Error(t, err)
		assert.Regexp(t, "not found", err)
	})

	t.Run("fail to read body", func(t *testing.T) {
		responder := httpmock.ResponderFromResponse(&http.Response{
			Status:     strconv.Itoa(http.StatusBadRequest),
			StatusCode: http.StatusBadRequest,
			Body:       brokenBodyReader{},
		})
		httpmock.RegisterResponder("PUT", url, responder)

		err := api.Update(eventType)
		require.Error(t, err)
		assert.Regexp(t, "unable to read response body", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.RegisterResponder("PUT", url, httpmock.Responder(func(r *http.Request) (*http.Response, error) {
			uploaded := &EventType{}
			err := json.NewDecoder(r.Body).Decode(uploaded)
			require.NoError(t, err)
			assert.Equal(t, eventType, uploaded)
			return httpmock.NewStringResponse(http.StatusOK, ""), nil
		}))

		err := api.Update(eventType)
		require.NoError(t, err)
	})
}

func TestEventAPI_Delete(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	name := "test-event.change"

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	api := NewEventAPI(client, nil)
	url := fmt.Sprintf("%s/event-types/%s", defaultNakadiURL, name)

	t.Run("fail connection error", func(t *testing.T) {
		httpmock.RegisterResponder("DELETE", url, httpmock.NewErrorResponder(assert.AnError))

		err := api.Delete(name)
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail decode body", func(t *testing.T) {
		httpmock.RegisterResponder("DELETE", url, httpmock.NewStringResponder(http.StatusNotFound, "most-likely-stacktrace"))

		err := api.Delete(name)
		require.Error(t, err)
		assert.Regexp(t, "unable to delete event type: most-likely-stacktrace", err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		problem := `{"detail": "not found"}`
		httpmock.RegisterResponder("DELETE", url, httpmock.NewStringResponder(http.StatusNotFound, problem))

		err := api.Delete(name)
		require.Error(t, err)
		assert.Regexp(t, "not found", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.RegisterResponder("DELETE", url, httpmock.NewStringResponder(http.StatusNoContent, ""))

		err := api.Delete(name)
		assert.NoError(t, err)
	})
}

func TestEventOptions_withDefaults(t *testing.T) {
	tests := []struct {
		Options  *EventOptions
		Expected *EventOptions
	}{
		{
			Options: nil,
			Expected: &EventOptions{
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				MaxElapsedTime:       defaultMaxElapsedTime,
			},
		},
		{
			Options: &EventOptions{InitialRetryInterval: time.Hour},
			Expected: &EventOptions{
				InitialRetryInterval: time.Hour,
				MaxRetryInterval:     defaultMaxRetryInterval,
				MaxElapsedTime:       defaultMaxElapsedTime,
			},
		},
		{
			Options: &EventOptions{MaxRetryInterval: time.Hour},
			Expected: &EventOptions{
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     time.Hour,
				MaxElapsedTime:       defaultMaxElapsedTime,
			},
		},
		{
			Options: &EventOptions{MaxElapsedTime: time.Hour},
			Expected: &EventOptions{
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				MaxElapsedTime:       time.Hour,
			},
		},
		{
			Options: &EventOptions{Retry: true},
			Expected: &EventOptions{
				Retry:                true,
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				MaxElapsedTime:       defaultMaxElapsedTime,
			},
		},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.Expected, tt.Options.withDefaults())
	}
}
