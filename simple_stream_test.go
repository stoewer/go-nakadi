package nakadi

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/jarcoal/httpmock.v1"
)

func TestSimpleStreamOpener_openStream(t *testing.T) {
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
			tokenProvider:    func() (string, error) { return "token", nil }}
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

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewErrorResponder(assert.AnError))

		_, err := opener.openStream()
		require.Error(t, err)
		assert.Regexp(t, assert.AnError.Error(), err.Error())
	})

	t.Run("fail http error", func(t *testing.T) {
		problem := &problemJSON{Detail: "foo problem detail"}
		opener := setupOpener()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		responder, _ := httpmock.NewJsonResponder(400, &problem)
		httpmock.RegisterResponder("GET", url, responder)

		_, err := opener.openStream()
		require.Error(t, err)
		assert.Regexp(t, problem.Detail, err.Error())
	})

	t.Run("success without token", func(t *testing.T) {
		opener := setupOpener()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		responder, _ := httpmock.NewJsonResponder(200, sub)
		httpmock.RegisterResponder("GET", url, responder)

		stream, err := opener.openStream()
		require.NoError(t, err)
		require.NotNil(t, stream)
	})

	t.Run("success with token", func(t *testing.T) {
		opener := setupOpener()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		responder, _ := httpmock.NewJsonResponder(200, sub)
		// TODO check token here
		httpmock.RegisterResponder("GET", url, responder)

		stream, err := opener.openStream()
		require.NoError(t, err)
		require.NotNil(t, stream)
	})
}

func TestSimpleStream_nextEvents(t *testing.T) {
	id := "21e9e526-551e-11e7-bbe7-1f386750d2b6"
	url := fmt.Sprintf("%s/subscriptions/%s/events", defaultNakadiURL, id)

	setupStream := func(responder httpmock.Responder) *simpleStream {
		httpmock.RegisterResponder("GET", url, responder)
		response, err := http.DefaultClient.Get(url)
		require.NoError(t, err)
		return &simpleStream{
			nakadiStreamID: "stream-id",
			buffer:         bufio.NewReader(response.Body),
			closer:         response.Body}
	}

	t.Run("fail stream closed", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		stream := setupStream(httpmock.NewStringResponder(200, ""))
		stream.buffer = nil

		_, _, err := stream.nextEvents()
		require.Error(t, err)
		assert.Regexp(t, "stream is closed", err.Error())
	})

	t.Run("fail EOF", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		stream := setupStream(httpmock.NewStringResponder(200, ""))

		_, _, err := stream.nextEvents()
		require.Error(t, err)
		assert.Regexp(t, "EOF", err.Error())
	})

	t.Run("fail unmarshal event", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		stream := setupStream(httpmock.NewStringResponder(200, "not\n a\n event\n"))

		_, _, err := stream.nextEvents()
		require.Error(t, err)
		assert.Regexp(t, "failed to unmarshal next batch", err.Error())
	})

	t.Run("successfully read events", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		events := helperLoadTestData(t, "data-event-stream.json", nil)
		stream := setupStream(httpmock.NewStringResponder(200, string(events)))

		for i := 0; i < 5; i++ {
			cursor, _, err := stream.nextEvents()
			require.NoError(t, err)
			assert.Equal(t, "stream-id", cursor.NakadiStreamID)
		}
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
	id := "21e9e526-551e-11e7-bbe7-1f386750d2b6"
	url := fmt.Sprintf("%s/subscriptions/%s/cursors", defaultNakadiURL, id)

	setupCommitter := func(responder httpmock.Responder) *simpleCommitter {
		httpmock.RegisterResponder("POST", url, responder)
		client := &Client{
			nakadiURL:     defaultNakadiURL,
			httpClient:    http.DefaultClient,
			tokenProvider: func() (string, error) { return "token", nil }}
		return &simpleCommitter{
			client:         client,
			subscriptionID: id}
	}

	t.Run("fail retrieving token", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		stream := setupCommitter(httpmock.NewStringResponder(200, ""))
		stream.client.tokenProvider = func() (string, error) { return "", assert.AnError }

		err := stream.commitCursor(Cursor{})
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail connect error", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		stream := setupCommitter(httpmock.NewErrorResponder(assert.AnError))

		err := stream.commitCursor(Cursor{})
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail http error", func(t *testing.T) {
		problem := &problemJSON{Detail: "foo problem detail"}
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		responder, _ := httpmock.NewJsonResponder(400, &problem)
		stream := setupCommitter(responder)

		err := stream.commitCursor(Cursor{})
		require.Error(t, err)
		assert.Regexp(t, problem.Detail, err)
	})

	t.Run("successful commit", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		// TODO check body here
		stream := setupCommitter(httpmock.NewStringResponder(200, ""))

		err := stream.commitCursor(Cursor{})
		require.NoError(t, err)
	})
}
