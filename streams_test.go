package nakadi

import (
	"context"
	"testing"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestStreamAPI_startStream(t *testing.T) {
	retryCh := make(chan error, 1)
	okCh := make(chan struct{}, 1)
	blockCh := make(chan time.Time, 1)
	stream := &mockStreamer{}

	_, opener, _ := setupMockStream(retryCh, okCh)

	opener.On("openStream").Once().Return(nil, assert.AnError)
	opener.On("openStream").Once().Return(stream, nil).WaitUntil(blockCh)
	stream.On("nextEvents").Return(Cursor{}, nil, assert.AnError).WaitUntil(blockCh)

	select {
	case err := <-retryCh:
		assert.EqualError(t, assert.AnError, err.Error())
	case <-time.After(50 * time.Millisecond):
		assert.Fail(t, "no retry error detected")
	}

	blockCh <- time.Now()
	select {
	case <-okCh:
		// nothing
	case <-time.After(50 * time.Millisecond):
		assert.Fail(t, "no retry ok detected")
	}
}

func TestStreamAPI_NextEvents(t *testing.T) {
	expectedCursor := Cursor{NakadiStreamID: "stream-id"}
	expectedEvents := []byte(`"events":[{"metadata":{"eid":"74450ab6-5461-11e7-9dd2-87c3afa8811f"})]`)

	t.Run("fail with error", func(t *testing.T) {
		stream := &mockStreamer{}
		blockCh := make(chan time.Time, 1)
		streamAPI, opener, _ := setupMockStream(nil, nil)

		opener.On("openStream").Return(stream, nil).WaitUntil(blockCh)
		blockCh <- time.Now()

		stream.On("nextEvents").Return(Cursor{}, nil, assert.AnError).WaitUntil(blockCh)
		stream.On("closeStream").Return(nil)
		blockCh <- time.Now()

		_, _, err := streamAPI.NextEvents()

		assert.EqualError(t, assert.AnError, err.Error())
	})

	t.Run("successful read events", func(t *testing.T) {
		stream := &mockStreamer{}
		blockCh := make(chan time.Time, 1)
		streamAPI, opener, _ := setupMockStream(nil, nil)

		opener.On("openStream").Return(stream, nil).WaitUntil(blockCh)
		blockCh <- time.Now()

		stream.On("nextEvents").Return(expectedCursor, expectedEvents, nil).WaitUntil(blockCh)
		stream.On("closeStream").Return(nil)
		blockCh <- time.Now()

		cursor, events, err := streamAPI.NextEvents()

		require.NoError(t, err)
		assert.Equal(t, expectedCursor, cursor)
		assert.Equal(t, expectedEvents, events)

		blockCh <- time.Now()
		cursor, events, err = streamAPI.NextEvents()

		require.NoError(t, err)
		assert.Equal(t, expectedCursor, cursor)
		assert.Equal(t, expectedEvents, events)
	})

	t.Run("fail with canceled", func(t *testing.T) {
		stream := &mockStreamer{}
		blockCh := make(chan time.Time, 1)
		streamAPI, opener, _ := setupMockStream(nil, nil)

		opener.On("openStream").Return(stream, nil).WaitUntil(blockCh)
		blockCh <- time.Now()

		stream.On("nextEvents").Return(Cursor{}, nil, context.Canceled).WaitUntil(blockCh)
		stream.On("closeStream").Return(nil)
		blockCh <- time.Now()

		_, _, err := streamAPI.NextEvents()

		assert.EqualError(t, context.Canceled, err.Error())
	})
}

func TestStreamAPI_CommitCursor(t *testing.T) {
	retryCh := make(chan error, 1)
	okCh := make(chan struct{}, 1)
	errorCh := make(chan error, 1)
	blockCh := make(chan time.Time, 1)
	blockStreamer := make(chan time.Time, 1)
	expectedCursor := Cursor{NakadiStreamID: "stream-id"}

	t.Run("fail with time out", func(t *testing.T) {
		streamAPI, opener, committer := setupMockStream(retryCh, okCh)
		opener.On("openStream").WaitUntil(blockStreamer)
		committer.On("commitCursor", expectedCursor).WaitUntil(blockCh).Twice().Return(assert.AnError)

		go func() {
			err := streamAPI.CommitCursor(expectedCursor)
			errorCh <- err
		}()

		blockCh <- time.Now()

		err := <-retryCh
		assert.EqualError(t, assert.AnError, err.Error())

		time.Sleep(600 * time.Millisecond)
		blockCh <- time.Now()

		err = <-errorCh
		assert.EqualError(t, assert.AnError, err.Error())
	})

	t.Run("fail retry succeed", func(t *testing.T) {
		streamAPI, opener, committer := setupMockStream(retryCh, okCh)
		opener.On("openStream").WaitUntil(blockStreamer)
		committer.On("commitCursor", expectedCursor).WaitUntil(blockCh).Once().Return(assert.AnError)

		go func() {
			err := streamAPI.CommitCursor(expectedCursor)
			errorCh <- err
		}()

		blockCh <- time.Now()

		err := <-retryCh
		assert.EqualError(t, assert.AnError, err.Error())

		committer.On("commitCursor", expectedCursor).WaitUntil(blockCh).Once().Return(nil)
		blockCh <- time.Now()

		err = <-errorCh
		assert.NoError(t, err)

		select {
		case <-okCh:
			// nothing
		default:
			assert.Fail(t, "no retry ok detected")
		}
	})

	t.Run("success", func(t *testing.T) {
		streamAPI, opener, committer := setupMockStream(nil, nil)
		opener.On("openStream").WaitUntil(blockStreamer)
		committer.On("commitCursor", expectedCursor).Once().Return(nil).WaitUntil(blockCh)
		blockCh <- time.Now()
		err := streamAPI.CommitCursor(expectedCursor)

		assert.NoError(t, err)
	})
}

func TestStreamAPI_Close(t *testing.T) {
	errorCh := make(chan error, 1)
	blockCh := make(chan time.Time, 1)
	streamAPI, opener, _ := setupMockStream(nil, nil)

	stream := &mockStreamer{}
	opener.On("openStream").WaitUntil(blockCh).Return(stream, nil)
	stream.On("nextEvents").WaitUntil(blockCh).Once().Return(Cursor{}, nil, assert.AnError)
	stream.On("closeStream").Return(nil)

	go func() {
		err := streamAPI.Close()
		errorCh <- err
	}()

	blockCh <- time.Now()
	time.Sleep(50 * time.Millisecond)
	blockCh <- time.Now()

	err := <-errorCh
	assert.NoError(t, err)

	<-streamAPI.eventCh
	stream.AssertCalled(t, "closeStream")
}

func setupMockStream(errCh chan error, okCh chan struct{}) (*StreamAPI, *mockStreamOpener, *mockCommitter) {
	ctx, cancel := context.WithCancel(context.Background())

	streamBackOff := backoff.NewExponentialBackOff()
	streamBackOff.InitialInterval = 1 * time.Millisecond
	streamBackOff.MaxInterval = 100 * time.Millisecond
	streamBackOff.MaxElapsedTime = 500 * time.Millisecond

	commitBackOff := backoff.NewExponentialBackOff()
	commitBackOff.InitialInterval = 1 * time.Millisecond
	commitBackOff.MaxInterval = 100 * time.Millisecond
	commitBackOff.MaxElapsedTime = 500 * time.Millisecond

	opener := &mockStreamOpener{}
	committer := &mockCommitter{}

	stream := &StreamAPI{
		opener:        opener,
		committer:     committer,
		eventCh:       make(chan eventsOrError, 10),
		ctx:           ctx,
		cancel:        cancel,
		streamBackOff: backoff.WithContext(streamBackOff, ctx),
		commitBackOff: backoff.WithContext(commitBackOff, ctx),
		notifyErr: func(err error, _ time.Duration) {
			if errCh != nil {
				errCh <- err
			}
		},
		notifyOK: func() {
			if okCh != nil {
				okCh <- struct{}{}
			}
		},
	}

	go stream.startStream()

	return stream, opener, committer
}

type mockStreamOpener struct {
	mock.Mock
}

func (so *mockStreamOpener) openStream() (streamer, error) {
	args := so.Called()
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(streamer), nil
}

type mockStreamer struct {
	mock.Mock
}

func (s *mockStreamer) nextEvents() (Cursor, []byte, error) {
	args := s.Called()
	if args.Error(2) != nil {
		return Cursor{}, nil, args.Error(2)
	}
	return args.Get(0).(Cursor), args.Get(1).([]byte), nil
}

func (s *mockStreamer) closeStream() error {
	return s.Called().Error(0)
}

type mockCommitter struct {
	mock.Mock
}

func (c *mockCommitter) commitCursor(cursor Cursor) error {
	return c.Called(cursor).Error(0)
}
