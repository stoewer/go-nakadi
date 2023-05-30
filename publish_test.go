package nakadi

import (
	"compress/gzip"
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

type SomeData struct {
	Test string `json:"test"`
}

type SomeUndefinedEvent struct {
	UndefinedEvent
	Test string `json:"test"`
}

func TestUndefinedEvent_Marshal(t *testing.T) {
	event := &SomeUndefinedEvent{}
	expected := helperLoadTestData(t, "event-undefined-complete.json", event)

	serialized, err := json.Marshal(event)
	require.NoError(t, err)
	assert.JSONEq(t, string(expected), string(serialized))
}

func TestBusinessEvent_Marshal(t *testing.T) {
	event := &BusinessEvent{}
	expected := helperLoadTestData(t, "event-business-complete.json", event)

	serialized, err := json.Marshal(event)
	require.NoError(t, err)
	assert.JSONEq(t, string(expected), string(serialized))
}

func TestDataChangeEvent_Marshal(t *testing.T) {
	event := &DataChangeEvent{Data: SomeData{}}
	expected := helperLoadTestData(t, "event-data-complete.json", event)

	serialized, err := json.Marshal(event)
	require.NoError(t, err)
	assert.JSONEq(t, string(expected), string(serialized))
}

func TestPublishAPI_Publish(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	events := []SomeUndefinedEvent{}
	helperLoadTestData(t, "events-undefined-create.json", &events)

	url := fmt.Sprintf("%s/event-types/%s/events", defaultNakadiURL, "test-event.undefined")

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	publishAPI := NewPublishAPI(client, "test-event.undefined", nil)

	t.Run("fail to connect", func(t *testing.T) {
		httpmock.RegisterResponder("POST", url, httpmock.NewErrorResponder(assert.AnError))

		err := publishAPI.Publish(events)

		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail decode body", func(t *testing.T) {
		httpmock.RegisterResponder("POST", url, httpmock.NewStringResponder(http.StatusMultiStatus, ""))

		err := publishAPI.Publish(events)
		require.Error(t, err)
		assert.Regexp(t, "unable to decode response body", err)

		httpmock.RegisterResponder("POST", url, httpmock.NewStringResponder(http.StatusUnauthorized, "most-likely-stacktrace"))

		err = publishAPI.Publish(events)
		require.Error(t, err)
		assert.Regexp(t, "unable to request event types: most-likely-stacktrace", err)
	})

	t.Run("fail multi status", func(t *testing.T) {
		batchItemResp := []BatchItemResponse{{Detail: "error one"}, {Detail: "error two"}}
		responder, _ := httpmock.NewJsonResponder(http.StatusMultiStatus, batchItemResp)
		httpmock.RegisterResponder("POST", url, responder)

		err := publishAPI.Publish(events)
		require.Error(t, err)

		batchItemErr, ok := err.(BatchItemsError)
		require.True(t, ok)
		assert.Equal(t, "error one", batchItemErr[0].Detail)
		assert.Equal(t, "error two", batchItemErr[1].Detail)
	})

	t.Run("fail unprocessable", func(t *testing.T) {
		batchItemResp := []BatchItemResponse{{Detail: "error one"}, {Detail: "error two"}}
		responder, _ := httpmock.NewJsonResponder(http.StatusUnprocessableEntity, batchItemResp)
		httpmock.RegisterResponder("POST", url, responder)

		err := publishAPI.Publish(events)

		require.Error(t, err)
		assert.Regexp(t, BatchItemsError{}, err)

		batchItemErr, ok := err.(BatchItemsError)
		require.True(t, ok)
		assert.Equal(t, "error one", batchItemErr[0].Detail)
		assert.Equal(t, "error two", batchItemErr[1].Detail)
	})

	t.Run("fail unauthorized", func(t *testing.T) {
		problem := problemJSON{Detail: "not authorized"}
		responder, _ := httpmock.NewJsonResponder(http.StatusUnauthorized, problem)
		httpmock.RegisterResponder("POST", url, responder)

		err := publishAPI.Publish(events)

		require.Error(t, err)
		assert.Regexp(t, "not authorized", err)
	})

	t.Run("fail to read body", func(t *testing.T) {
		responder := httpmock.ResponderFromResponse(&http.Response{
			Status:     strconv.Itoa(http.StatusBadRequest),
			StatusCode: http.StatusBadRequest,
			Body:       brokenBodyReader{},
		})
		httpmock.RegisterResponder("POST", url, responder)

		err := publishAPI.Publish(events)
		require.Error(t, err)
		assert.Regexp(t, "unable to read response body", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.RegisterResponder("POST", url, httpmock.Responder(func(r *http.Request) (*http.Response, error) {
			uploaded := []SomeUndefinedEvent{}
			err := json.NewDecoder(r.Body).Decode(&uploaded)
			require.NoError(t, err)
			assert.Equal(t, events, uploaded)
			return httpmock.NewStringResponse(http.StatusOK, ""), nil
		}))

		err := publishAPI.Publish(events)

		assert.NoError(t, err)
	})
}

func TestPublishAPI_PublishWithCompression(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	events := []SomeUndefinedEvent{}
	helperLoadTestData(t, "events-undefined-create.json", &events)

	url := fmt.Sprintf("%s/event-types/%s/events", defaultNakadiURL, "test-event.undefined")

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	publishOptions := &PublishOptions{
		EnableCompression:    true,
		CompressionAlgorithm: CompressionAlgorithmGzip,
		CompressionLevel:     gzip.DefaultCompression,
	}
	publishAPI := NewPublishAPI(client, "test-event.undefined", publishOptions)

	t.Run("fail to connect", func(t *testing.T) {
		httpmock.RegisterResponder("POST", url, httpmock.NewErrorResponder(assert.AnError))

		err := publishAPI.Publish(events)

		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail decode body", func(t *testing.T) {
		httpmock.RegisterResponder("POST", url, httpmock.NewStringResponder(http.StatusMultiStatus, ""))

		err := publishAPI.Publish(events)
		require.Error(t, err)
		assert.Regexp(t, "unable to decode response body", err)

		httpmock.RegisterResponder("POST", url, httpmock.NewStringResponder(http.StatusUnauthorized, "most-likely-stacktrace"))

		err = publishAPI.Publish(events)
		require.Error(t, err)
		assert.Regexp(t, "unable to request event types: most-likely-stacktrace", err)
	})

	t.Run("fail multi status", func(t *testing.T) {
		batchItemResp := []BatchItemResponse{{Detail: "error one"}, {Detail: "error two"}}
		responder, _ := httpmock.NewJsonResponder(http.StatusMultiStatus, batchItemResp)
		httpmock.RegisterResponder("POST", url, responder)

		err := publishAPI.Publish(events)
		require.Error(t, err)

		batchItemErr, ok := err.(BatchItemsError)
		require.True(t, ok)
		assert.Equal(t, "error one", batchItemErr[0].Detail)
		assert.Equal(t, "error two", batchItemErr[1].Detail)
	})

	t.Run("fail unprocessable", func(t *testing.T) {
		batchItemResp := []BatchItemResponse{{Detail: "error one"}, {Detail: "error two"}}
		responder, _ := httpmock.NewJsonResponder(http.StatusUnprocessableEntity, batchItemResp)
		httpmock.RegisterResponder("POST", url, responder)

		err := publishAPI.Publish(events)

		require.Error(t, err)
		assert.Regexp(t, BatchItemsError{}, err)

		batchItemErr, ok := err.(BatchItemsError)
		require.True(t, ok)
		assert.Equal(t, "error one", batchItemErr[0].Detail)
		assert.Equal(t, "error two", batchItemErr[1].Detail)
	})

	t.Run("fail unauthorized", func(t *testing.T) {
		problem := problemJSON{Detail: "not authorized"}
		responder, _ := httpmock.NewJsonResponder(http.StatusUnauthorized, problem)
		httpmock.RegisterResponder("POST", url, responder)

		err := publishAPI.Publish(events)

		require.Error(t, err)
		assert.Regexp(t, "not authorized", err)
	})

	t.Run("fail to read body", func(t *testing.T) {
		responder := httpmock.ResponderFromResponse(&http.Response{
			Status:     strconv.Itoa(http.StatusBadRequest),
			StatusCode: http.StatusBadRequest,
			Body:       brokenBodyReader{},
		})
		httpmock.RegisterResponder("POST", url, responder)

		err := publishAPI.Publish(events)
		require.Error(t, err)
		assert.Regexp(t, "unable to read response body", err)
	})

	t.Run("success", func(t *testing.T) {
		httpmock.RegisterResponder("POST", url, httpmock.Responder(func(r *http.Request) (*http.Response, error) {
			uploaded := []SomeUndefinedEvent{}

			assert.Equal(t, "gzip", r.Header.Get("Content-Encoding"))
			reader, err := gzip.NewReader(r.Body)
			if err != nil {
				panic(err)
			}
			err = json.NewDecoder(reader).Decode(&uploaded)

			require.NoError(t, err)
			assert.Equal(t, events, uploaded)
			return httpmock.NewStringResponse(http.StatusOK, ""), nil
		}))

		err := publishAPI.Publish(events)
		assert.NoError(t, err)
	})
}

func TestPublishAPI_PublishDataChangeEvent(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	events := []DataChangeEvent{}
	helperLoadTestData(t, "events-data-create.json", &events)

	url := fmt.Sprintf("%s/event-types/%s/events", defaultNakadiURL, "test-event.data")

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	publishAPI := NewPublishAPI(client, "test-event.data", nil)

	httpmock.RegisterResponder("POST", url, httpmock.Responder(func(r *http.Request) (*http.Response, error) {
		uploaded := []DataChangeEvent{}
		err := json.NewDecoder(r.Body).Decode(&uploaded)
		require.NoError(t, err)
		assert.Equal(t, events, uploaded)
		return httpmock.NewStringResponse(http.StatusOK, ""), nil
	}))

	err := publishAPI.PublishDataChangeEvent(events)

	assert.NoError(t, err)
}

func TestPublishAPI_PublishBusinessEvent(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	events := []BusinessEvent{}
	helperLoadTestData(t, "events-business-create.json", &events)

	url := fmt.Sprintf("%s/event-types/%s/events", defaultNakadiURL, "test-event.business")

	client := &Client{nakadiURL: defaultNakadiURL, httpClient: http.DefaultClient}
	publishAPI := NewPublishAPI(client, "test-event.business", nil)

	httpmock.RegisterResponder("POST", url, httpmock.Responder(func(r *http.Request) (*http.Response, error) {
		uploaded := []BusinessEvent{}
		err := json.NewDecoder(r.Body).Decode(&uploaded)
		require.NoError(t, err)
		assert.Equal(t, events, uploaded)
		return httpmock.NewStringResponse(http.StatusOK, ""), nil
	}))

	err := publishAPI.PublishBusinessEvent(events)

	assert.NoError(t, err)
}

func TestPublishOptions_withDefaults(t *testing.T) {
	tests := []struct {
		Options  *PublishOptions
		Expected *PublishOptions
	}{
		{
			Options: nil,
			Expected: &PublishOptions{
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				MaxElapsedTime:       defaultMaxElapsedTime,
				CompressionAlgorithm: defaultCompressionAlgorithm,
				CompressionLevel:     defaultGzipCompressionLevel,
			},
		},
		{
			Options: &PublishOptions{InitialRetryInterval: time.Hour},
			Expected: &PublishOptions{
				InitialRetryInterval: time.Hour,
				MaxRetryInterval:     defaultMaxRetryInterval,
				MaxElapsedTime:       defaultMaxElapsedTime,
				CompressionAlgorithm: defaultCompressionAlgorithm,
				CompressionLevel:     defaultGzipCompressionLevel,
			},
		},
		{
			Options: &PublishOptions{MaxRetryInterval: time.Hour},
			Expected: &PublishOptions{
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     time.Hour,
				MaxElapsedTime:       defaultMaxElapsedTime,
				CompressionAlgorithm: defaultCompressionAlgorithm,
				CompressionLevel:     defaultGzipCompressionLevel,
			},
		},
		{
			Options: &PublishOptions{MaxElapsedTime: time.Hour},
			Expected: &PublishOptions{
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				MaxElapsedTime:       time.Hour,
				CompressionAlgorithm: defaultCompressionAlgorithm,
				CompressionLevel:     defaultGzipCompressionLevel,
			},
		},
		{
			Options: &PublishOptions{Retry: true},
			Expected: &PublishOptions{
				Retry:                true,
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				MaxElapsedTime:       defaultMaxElapsedTime,
				CompressionAlgorithm: defaultCompressionAlgorithm,
				CompressionLevel:     defaultGzipCompressionLevel,
			},
		}, {
			Options: &PublishOptions{Retry: true},
			Expected: &PublishOptions{
				Retry:                true,
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				MaxElapsedTime:       defaultMaxElapsedTime,
				CompressionAlgorithm: defaultCompressionAlgorithm,
				CompressionLevel:     defaultGzipCompressionLevel,
			},
		},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.Expected, tt.Options.withDefaults())
	}
}

func TestBatchItemsError_Format(t *testing.T) {
	t.Run("format nil error", func(t *testing.T) {
		var batchItemErr BatchItemsError
		assert.Equal(t, "nil", fmt.Sprintf("%+v", batchItemErr))
		assert.Equal(t, "", batchItemErr.Error())
	})

	t.Run("format unknown error", func(t *testing.T) {
		batchItemErr := BatchItemsError{}
		assert.Equal(t, "an unknown error occurred while publishing event", fmt.Sprintf("%+v", batchItemErr))
		assert.Equal(t, "one or many events may have not been published", batchItemErr.Error())
	})

	t.Run("format batch items error", func(t *testing.T) {
		batchItemErr := BatchItemsError{{Detail: assert.AnError.Error()}}
		assert.Regexp(t, assert.AnError, fmt.Sprintf("%+v", batchItemErr))
	})

	t.Run("format multiple batch items error", func(t *testing.T) {
		batchItemErr := BatchItemsError{
			{EID: "1", Detail: "error 1"},
			{EID: "2", Detail: "error 2"},
		}
		assert.Regexp(t, "errors occurred while publishing events:", fmt.Sprintf("%+v", batchItemErr))
	})
}
