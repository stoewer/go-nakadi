package nakadi

import (
	"bufio"
	"bytes"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimpleStreamOpener_openStream(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	createdAt := time.Time{}
	createdAt.UnmarshalText([]byte("2017-08-17T00:00:23+02:00"))

	sub := &Subscription{
		ID:                "4e6f4b42-5459-11e7-8b76-97cbdf1f5274",
		OwningApplication: "nakadi-client",
		EventTypes:        []string{"test"},
		ConsumerGroup:     "default",
		ReadFrom:          "end",
		CreatedAt:         createdAt}

	url := fmt.Sprintf("%s/subscriptions/%s/events", defaultNakadiURL, sub.ID)

	setupOpener := func() *simpleStreamOpener {
		client := &Client{
			nakadiURL:        defaultNakadiURL,
			httpClient:       http.DefaultClient,
			httpStreamClient: http.DefaultClient,
			tokenProvider:    func() (string, error) { return testToken, nil }}
		return &simpleStreamOpener{
			client:         client,
			subscriptionID: sub.ID}
	}

	t.Run("fail retrieving token", func(t *testing.T) {
		opener := setupOpener()
		opener.client.tokenProvider = func() (string, error) { return "", assert.AnError }

		_, err := opener.openStream()
		require.Error(t, err)
		assert.Regexp(t, assert.AnError.Error(), err.Error())
	})

	t.Run("fail connect error", func(t *testing.T) {
		opener := setupOpener()
		httpmock.RegisterResponder("GET", url, httpmock.NewErrorResponder(assert.AnError))

		_, err := opener.openStream()
		require.Error(t, err)
		assert.Regexp(t, assert.AnError.Error(), err.Error())
	})

	t.Run("fail http error", func(t *testing.T) {
		problem := &problemJSON{Detail: "foo problem detail"}
		opener := setupOpener()
		responder, _ := httpmock.NewJsonResponder(400, &problem)
		httpmock.RegisterResponder("GET", url, responder)

		_, err := opener.openStream()
		require.Error(t, err)
		assert.Regexp(t, problem.Detail, err.Error())
	})
	t.Run("fail to read body", func(t *testing.T) {
		opener := setupOpener()
		responder := httpmock.ResponderFromResponse(&http.Response{
			Status:     strconv.Itoa(http.StatusBadRequest),
			StatusCode: http.StatusBadRequest,
			Body:       brokenBodyReader{},
		})
		httpmock.RegisterResponder("GET", url, responder)

		_, err := opener.openStream()
		require.Error(t, err)
		assert.Regexp(t, "unable to read response body", err.Error())
	})

	t.Run("success without token", func(t *testing.T) {
		opener := setupOpener()
		responder, _ := httpmock.NewJsonResponder(200, sub)
		httpmock.RegisterResponder("GET", url, responder)

		stream, err := opener.openStream()
		require.NoError(t, err)
		require.NotNil(t, stream)
	})

	t.Run("success with token", func(t *testing.T) {
		opener := setupOpener()
		responder, _ := httpmock.NewJsonResponder(200, sub)
		httpmock.RegisterResponder("GET", url, responder)

		stream, err := opener.openStream()
		require.NoError(t, err)
		require.NotNil(t, stream)
	})
}

func TestSimpleStream_nextEvents(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	id := "21e9e526-551e-11e7-bbe7-1f386750d2b6"
	url := fmt.Sprintf("%s/subscriptions/%s/events", defaultNakadiURL, id)

	setupStream := func(responder httpmock.Responder) *simpleStream {
		httpmock.RegisterResponder("GET", url, responder)
		response, err := http.DefaultClient.Get(url)
		require.NoError(t, err)
		return &simpleStream{
			nakadiStreamID: "stream-id",
			buffer:         bufio.NewReaderSize(response.Body, 0),
			closer:         response.Body,
			readTimeout:    1 * time.Second,
		}
	}

	t.Run("fail stream closed", func(t *testing.T) {
		stream := setupStream(httpmock.NewStringResponder(200, ""))
		stream.buffer = nil

		_, _, err := stream.nextEvents(bytes.Buffer{})
		require.Error(t, err)
		assert.Regexp(t, "stream is closed", err.Error())
	})

	t.Run("fail EOF", func(t *testing.T) {
		stream := setupStream(httpmock.NewStringResponder(200, ""))

		_, _, err := stream.nextEvents(bytes.Buffer{})
		require.Error(t, err)
		assert.Regexp(t, "EOF", err.Error())
	})

	t.Run("fail unmarshal event", func(t *testing.T) {
		stream := setupStream(httpmock.NewStringResponder(200, "not\n a\n event\n"))

		_, _, err := stream.nextEvents(bytes.Buffer{})
		require.Error(t, err)
		assert.Regexp(t, "failed to unmarshal next batch", err.Error())
	})

	t.Run("successfully read events", func(t *testing.T) {
		events := helperLoadTestData(t, "data-event-stream.json", nil)
		stream := setupStream(httpmock.NewStringResponder(200, string(events)))

		var readEventsInAllLines [][]byte
		for i := 0; i < 5; i++ {
			cursor, readEvents, err := stream.nextEvents(bytes.Buffer{})
			readEventsInAllLines = append(readEventsInAllLines, readEvents)
			require.NoError(t, err)
			assert.Equal(t, "stream-id", cursor.NakadiStreamID)
		}
		// test that nextEvents don't reuse returned bytes
		assert.NotEqual(t, readEventsInAllLines[0], readEventsInAllLines[2])
	})
}

type fakeCloser struct {
	Closed bool
}

func (fc *fakeCloser) Close() error {
	fc.Closed = true
	return nil
}

func TestSimpleStream_closeStream(t *testing.T) {
	closer := &fakeCloser{}
	stream := &simpleStream{
		nakadiStreamID: "stream-id",
		buffer:         bufio.NewReader(strings.NewReader("")),
		closer:         closer}

	err := stream.closeStream()
	assert.NoError(t, err)
	assert.Nil(t, stream.buffer)
	assert.True(t, closer.Closed)
}

func TestSimpleCommitter_commitEvents(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	id := "21e9e526-551e-11e7-bbe7-1f386750d2b6"
	url := fmt.Sprintf("%s/subscriptions/%s/cursors", defaultNakadiURL, id)

	setupCommitter := func(responder httpmock.Responder) *simpleCommitter {
		httpmock.RegisterResponder("POST", url, responder)
		client := &Client{
			nakadiURL:     defaultNakadiURL,
			httpClient:    http.DefaultClient,
			tokenProvider: func() (string, error) { return testToken, nil }}
		return &simpleCommitter{
			client:         client,
			subscriptionID: id}
	}

	t.Run("fail retrieving token", func(t *testing.T) {
		stream := setupCommitter(httpmock.NewStringResponder(200, ""))
		stream.client.tokenProvider = func() (string, error) { return "", assert.AnError }

		err := stream.commitCursor(Cursor{})
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail connect error", func(t *testing.T) {
		stream := setupCommitter(httpmock.NewErrorResponder(assert.AnError))

		err := stream.commitCursor(Cursor{})
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail http error", func(t *testing.T) {
		problem := &problemJSON{Detail: "foo problem detail"}
		responder, _ := httpmock.NewJsonResponder(400, &problem)
		stream := setupCommitter(responder)

		err := stream.commitCursor(Cursor{})
		require.Error(t, err)
		assert.Regexp(t, problem.Detail, err)
	})
	t.Run("fail to read body", func(t *testing.T) {
		responder := httpmock.ResponderFromResponse(&http.Response{
			Status:     strconv.Itoa(http.StatusBadRequest),
			StatusCode: http.StatusBadRequest,
			Body:       brokenBodyReader{},
		})
		stream := setupCommitter(responder)

		err := stream.commitCursor(Cursor{})
		require.Error(t, err)
		assert.Regexp(t, "unable to read response body", err)
	})

	t.Run("successful commit", func(t *testing.T) {
		stream := setupCommitter(httpmock.NewStringResponder(200, ""))

		err := stream.commitCursor(Cursor{})
		require.NoError(t, err)
	})
}
