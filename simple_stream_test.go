// Copyright (c) 2017, A. Stoewer <adrian.stoewer@rz.ifi.lmu.de>
// All rights reserved.

package nakadi

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"bufio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/jarcoal/httpmock.v1"
	"strings"
)

func TestSimpleStreamOpener_OpenStream(t *testing.T) {

	sub := &Subscription{
		ID:                "4e6f4b42-5459-11e7-8b76-97cbdf1f5274",
		OwningApplication: "nakadi-client",
		EventTypes:        []string{"test"},
		ConsumerGroup:     "default",
		ReadFrom:          "end",
		CreatedAt:         time.Now()}
	url := fmt.Sprintf("%s/subscriptions/%s/events", defaultNakadiURL, sub.ID)

	setupOpener := func() *SimpleStreamOpener {
		return &SimpleStreamOpener{
			NakadiURL:    defaultNakadiURL,
			HTTPClient:   http.DefaultClient,
			HTTPStream:   http.DefaultClient,
			Subscription: sub}
	}

	t.Run("fail retrieving token", func(t *testing.T) {
		opener := setupOpener()
		opener.TokenProvider = func() (string, error) { return "", assert.AnError }

		_, err := opener.OpenStream()
		require.Error(t, err)
		assert.Regexp(t, assert.AnError.Error(), err.Error())
	})

	t.Run("fail connect error", func(t *testing.T) {
		opener := setupOpener()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewErrorResponder(assert.AnError))

		_, err := opener.OpenStream()
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

		_, err := opener.OpenStream()
		require.Error(t, err)
		assert.Regexp(t, problem.Detail, err.Error())
	})

	t.Run("success without token", func(t *testing.T) {
		opener := setupOpener()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		responder, _ := httpmock.NewJsonResponder(200, sub)
		httpmock.RegisterResponder("GET", url, responder)

		stream, err := opener.OpenStream()
		require.NoError(t, err)
		require.NotNil(t, stream)
	})

	t.Run("success with token", func(t *testing.T) {
		opener := setupOpener()
		opener.TokenProvider = func() (string, error) { return "token", nil }

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		responder, _ := httpmock.NewJsonResponder(200, sub)
		// TODO check token here
		httpmock.RegisterResponder("GET", url, responder)

		stream, err := opener.OpenStream()
		require.NoError(t, err)
		require.NotNil(t, stream)
	})
}

func TestSimpleStream_Next(t *testing.T) {
	id := "21e9e526-551e-11e7-bbe7-1f386750d2b6"
	url := fmt.Sprintf("%s/subscriptions/%s/events", defaultNakadiURL, id)

	setupStream := func(responder httpmock.Responder) *SimpleStream {
		httpmock.RegisterResponder("GET", url, responder)
		response, err := http.DefaultClient.Get(url)
		require.NoError(t, err)
		return &SimpleStream{
			nakadiStreamID: "stream-id",
			buffer:         bufio.NewReader(response.Body),
			closer:         response.Body}
	}

	t.Run("fail stream closed", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		stream := setupStream(httpmock.NewStringResponder(200, ""))
		stream.buffer = nil

		_, _, err := stream.Next()
		require.Error(t, err)
		assert.Regexp(t, "stream is closed", err.Error())
	})

	t.Run("fail EOF", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		stream := setupStream(httpmock.NewStringResponder(200, ""))

		_, _, err := stream.Next()
		require.Error(t, err)
		assert.Regexp(t, "EOF", err.Error())
	})

	t.Run("fail unmarshal event", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		stream := setupStream(httpmock.NewStringResponder(200, "not\n a\n event\n"))

		_, _, err := stream.Next()
		require.Error(t, err)
		assert.Regexp(t, "failed to unmarshal next batch", err.Error())
	})

	t.Run("successfully read events", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		events := helperLoadGolden(t, "data-event-stream.json.golden")
		stream := setupStream(httpmock.NewStringResponder(200, string(events)))

		for i := 0; i < 5; i++ {
			cursor, _, err := stream.Next()
			require.NoError(t, err)
			assert.Equal(t, "stream-id", cursor.NakadiStreamID)
		}
	})
}

func TestSimpleStream_Commit(t *testing.T) {
	id := "21e9e526-551e-11e7-bbe7-1f386750d2b6"
	url := fmt.Sprintf("%s/subscriptions/%s/cursors", defaultNakadiURL, id)

	setupStream := func(responder httpmock.Responder) *SimpleStream {
		httpmock.RegisterResponder("POST", url, responder)
		return &SimpleStream{
			nakadiURL:      defaultNakadiURL,
			subscriptionID: id,
			nakadiStreamID: "stream-id",
			httpClient:     http.DefaultClient}
	}

	t.Run("fail retrieving token", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		stream := setupStream(httpmock.NewStringResponder(200, ""))
		stream.tokenProvider = func() (string, error) { return "", assert.AnError }

		err := stream.Commit(&Cursor{})
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail connect error", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		stream := setupStream(httpmock.NewErrorResponder(assert.AnError))

		err := stream.Commit(&Cursor{})
		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail http error", func(t *testing.T) {
		problem := &problemJSON{Detail: "foo problem detail"}
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		responder, _ := httpmock.NewJsonResponder(400, &problem)
		stream := setupStream(responder)

		err := stream.Commit(&Cursor{})
		require.Error(t, err)
		assert.Regexp(t, problem.Detail, err)
	})

	t.Run("successful commit", func(t *testing.T) {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		// TODO check body here
		stream := setupStream(httpmock.NewStringResponder(200, ""))

		err := stream.Commit(&Cursor{})
		require.NoError(t, err)
	})
}

type fakeCloser struct {
	Closed bool
}

func (fc *fakeCloser) Close() error {
	fc.Closed = true
	return nil
}

func TestSimpleStream_Close(t *testing.T) {
	closer := &fakeCloser{}
	stream := &SimpleStream{
		nakadiStreamID: "stream-id",
		buffer:         bufio.NewReader(strings.NewReader("")),
		closer:         closer}

	err := stream.Close()
	assert.NoError(t, err)
	assert.Nil(t, stream.buffer)
	assert.True(t, closer.Closed)
}
