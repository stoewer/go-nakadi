package nakadi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscription_Marshal(t *testing.T) {
	subscription := &Subscription{}
	expected := helperLoadTestData(t, "subscription.json", subscription)
	fmt.Println(subscription)

	serialized, err := json.Marshal(subscription)
	require.NoError(t, err)
	require.NoError(t, err)
	assert.JSONEq(t, string(expected), string(serialized))
}

func TestSubscriptionAPI_Get(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	expected := &Subscription{}
	serialized := helperLoadTestData(t, "subscription.json", expected)

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	api := NewSubscriptionAPI(client, nil)
	url := fmt.Sprintf("%s/subscriptions/%s", defaultNakadiURL, expected.ID)

	t.Run("fail connection error", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewErrorResponder(assert.AnError))

		_, err := api.Get(expected.ID)
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail decode error", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusNotFound, "most-likely-stacktrace"))

		_, err := api.Get(expected.ID)
		require.Error(t, err)
		assert.Regexp(t, "unable to request subscription: most-likely-stacktrace", err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusNotFound, testProblemJSON))

		_, err := api.Get(expected.ID)
		require.Error(t, err)
		assert.Regexp(t, "some problem detail", err)
	})

	t.Run("fail decode response", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusOK, ""))

		_, err := api.Get(expected.ID)
		require.Error(t, err)
		assert.Regexp(t, "unable to decode response body", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewBytesResponder(http.StatusOK, serialized))

		requested, err := api.Get(expected.ID)
		require.NoError(t, err)
		assert.Equal(t, expected, requested)
	})
}

func TestSubscriptionAPI_List(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	expected := struct {
		Items []*Subscription `json:"items"`
	}{}
	helperLoadTestData(t, "subscriptions.json", &expected.Items)

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	api := NewSubscriptionAPI(client, nil)
	url := fmt.Sprintf("%s/subscriptions", defaultNakadiURL)

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
		assert.Regexp(t, "unable to request subscriptions: most-likely-stacktrace", err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusInternalServerError, testProblemJSON))

		_, err := api.List()
		require.Error(t, err)
		assert.Regexp(t, "some problem detail", err)
	})

	t.Run("fail decode response", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusOK, ""))

		_, err := api.List()
		require.Error(t, err)
		assert.Regexp(t, "unable to decode response body", err)
	})

	t.Run("success", func(t *testing.T) {
		responder, err := httpmock.NewJsonResponder(http.StatusOK, expected)
		require.NoError(t, err)
		httpmock.RegisterResponder("GET", url, responder)

		requested, err := api.List()
		require.NoError(t, err)
		assert.Equal(t, expected.Items, requested)
	})
}

func TestSubscriptionAPI_Create(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	auth := &SubscriptionAuthorization{
		Admins:  []AuthorizationAttribute{{DataType: "service", Value: "test-service"}},
		Readers: []AuthorizationAttribute{{DataType: "service", Value: "test-service"}},
	}

	subscription := &Subscription{OwningApplication: "test-app", EventTypes: []string{"test-event.data"},
		Authorization: auth}
	expected := &Subscription{}
	serialized := helperLoadTestData(t, "subscription.json", expected)

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	api := NewSubscriptionAPI(client, nil)
	url := fmt.Sprintf("%s/subscriptions", defaultNakadiURL)

	t.Run("fail connection error", func(t *testing.T) {
		httpmock.RegisterResponder("POST", url, httpmock.NewErrorResponder(assert.AnError))

		_, err := api.Create(subscription)
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		httpmock.RegisterResponder("POST", url, httpmock.NewStringResponder(http.StatusUnauthorized, testProblemJSON))

		_, err := api.Create(subscription)
		require.Error(t, err)
		assert.Regexp(t, "some problem detail", err)
	})

	t.Run("fail decode body with error", func(t *testing.T) {
		httpmock.RegisterResponder("POST", url, httpmock.NewStringResponder(http.StatusUnauthorized, "most-likely-stacktrace"))

		_, err := api.Create(subscription)
		require.Error(t, err)
		assert.Regexp(t, "unable to create subscription: most-likely-stacktrace", err)
	})

	t.Run("fail decode body", func(t *testing.T) {
		httpmock.RegisterResponder("POST", url, httpmock.NewStringResponder(http.StatusOK, ""))

		_, err := api.Create(subscription)
		require.Error(t, err)
		assert.Regexp(t, "unable to decode response body", err)
	})
	t.Run("fail to read body", func(t *testing.T) {
		responder := httpmock.ResponderFromResponse(&http.Response{
			Status:     strconv.Itoa(http.StatusBadRequest),
			StatusCode: http.StatusBadRequest,
			Body:       brokenBodyReader{},
		})
		httpmock.RegisterResponder("POST", url, responder)

		_, err := api.Create(subscription)
		require.Error(t, err)
		assert.Regexp(t, "unable to read response body", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.RegisterResponder("POST", url, httpmock.Responder(func(r *http.Request) (*http.Response, error) {
			uploaded := &Subscription{}
			err := json.NewDecoder(r.Body).Decode(uploaded)
			require.NoError(t, err)
			assert.Equal(t, subscription, uploaded)
			return httpmock.NewBytesResponse(http.StatusOK, serialized), nil
		}))

		requested, err := api.Create(subscription)
		require.NoError(t, err)
		assert.Equal(t, expected, requested)
	})
}

func TestSubscriptionAPI_Delete(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	id := "7dd69d58-7f20-11e7-9748-133d6a0dbfb3"

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	api := NewSubscriptionAPI(client, nil)
	url := fmt.Sprintf("%s/subscriptions/%s", defaultNakadiURL, id)

	t.Run("fail connection error", func(t *testing.T) {
		httpmock.RegisterResponder("DELETE", url, httpmock.NewErrorResponder(assert.AnError))

		err := api.Delete(id)
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail decode body", func(t *testing.T) {
		httpmock.RegisterResponder("DELETE", url, httpmock.NewStringResponder(http.StatusNotFound, "most-likely-stacktrace"))

		err := api.Delete(id)
		require.Error(t, err)
		assert.Regexp(t, "unable to delete subscription: most-likely-stacktrace", err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		problem := `{"detail": "not found"}`
		httpmock.RegisterResponder("DELETE", url, httpmock.NewStringResponder(http.StatusNotFound, problem))

		err := api.Delete(id)
		require.Error(t, err)
		assert.Regexp(t, "not found", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.RegisterResponder("DELETE", url, httpmock.NewStringResponder(http.StatusNoContent, ""))

		err := api.Delete(id)
		assert.NoError(t, err)
	})
}

func TestSubscriptionAPI_GetStats(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	id := "7dd69d58-7f20-11e7-9748-133d6a0dbfb3"

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	api := NewSubscriptionAPI(client, nil)
	url := fmt.Sprintf("%s/subscriptions/%s/stats", defaultNakadiURL, id)

	t.Run("fail connection error", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewErrorResponder(assert.AnError))

		stats, err := api.GetStats(id)
		require.Error(t, err)
		require.Nil(t, stats)
		assert.Regexp(t, assert.AnError, err)

	})
	t.Run("fail decode error", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusInternalServerError, "most-likely-stacktrace"))

		stats, err := api.GetStats(id)
		require.Error(t, err)
		require.Nil(t, stats)
		assert.Regexp(t, "unable to get stats for subscription: most-likely-stacktrace", err)
	})

	t.Run("fail with problem", func(t *testing.T) {
		problem := `{"detail": "not found"}`
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusInternalServerError, problem))

		stats, err := api.GetStats(id)
		require.Error(t, err)
		require.Nil(t, stats)
		assert.Regexp(t, "not found", err)
	})

	t.Run("fail decode response", func(t *testing.T) {
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusOK, ""))

		stats, err := api.GetStats(id)
		require.Error(t, err)
		require.Nil(t, stats)
		assert.Regexp(t, "unable to decode response body", err)
	})

	t.Run("success", func(t *testing.T) {
		expected := struct {
			Items json.RawMessage `json:"items"`
		}{}
		_ = helperLoadTestData(t, "subscription-stats.json", &expected)

		responder, err := httpmock.NewJsonResponder(http.StatusOK, expected)
		require.NoError(t, err)
		httpmock.RegisterResponder("GET", url, responder)

		stats, err := api.GetStats(id)
		require.NoError(t, err)
		require.NotNil(t, stats)
		actual, err := json.Marshal(stats)
		require.NoError(t, err)
		assert.JSONEq(t, string(expected.Items), string(actual))
	})
}

func TestSubscriptionOptions_withDefaults(t *testing.T) {
	tests := []struct {
		Options  *SubscriptionOptions
		Expected *SubscriptionOptions
	}{
		{
			Options: nil,
			Expected: &SubscriptionOptions{
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				MaxElapsedTime:       defaultMaxElapsedTime,
			},
		},
		{
			Options: &SubscriptionOptions{InitialRetryInterval: time.Hour},
			Expected: &SubscriptionOptions{
				InitialRetryInterval: time.Hour,
				MaxRetryInterval:     defaultMaxRetryInterval,
				MaxElapsedTime:       defaultMaxElapsedTime,
			},
		},
		{
			Options: &SubscriptionOptions{MaxRetryInterval: time.Hour},
			Expected: &SubscriptionOptions{
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     time.Hour,
				MaxElapsedTime:       defaultMaxElapsedTime,
			},
		},
		{
			Options: &SubscriptionOptions{MaxElapsedTime: time.Hour},
			Expected: &SubscriptionOptions{
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				MaxElapsedTime:       time.Hour,
			},
		},
		{
			Options: &SubscriptionOptions{Retry: true},
			Expected: &SubscriptionOptions{
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
