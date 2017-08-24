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

	stream := &mockStreamer{}
	_, opener, _ := setupMockStream(retryCh, okCh)

	opener.On("openStream").Once().Return(nil, assert.AnError)
	opener.unblock <- struct{}{}
	time.Sleep(50 * time.Millisecond)

	select {
	case err := <-retryCh:
		assert.EqualError(t, assert.AnError, err.Error())
	default:
		assert.Fail(t, "no retry error detected")
	}

	opener.On("openStream").Once().Return(stream, nil)
	opener.unblock <- struct{}{}
	time.Sleep(50 * time.Millisecond)

	select {
	case <-okCh:
		// nothing
	default:
		assert.Fail(t, "no retry ok detected")
	}
}

func TestStreamAPI_NextEvents(t *testing.T) {
	expectedCursor := Cursor{NakadiStreamID: "stream-id"}
	expectedEvents := []byte(`"events":[{"metadata":{"eid":"74450ab6-5461-11e7-9dd2-87c3afa8811f"})]`)

	t.Run("fail with error", func(t *testing.T) {
		stream := &mockStreamer{unblock: make(chan struct{}, 1)}
		streamAPI, opener, _ := setupMockStream(nil, nil)

		opener.On("openStream").Once().Return(stream, nil)
		opener.unblock <- struct{}{}
		stream.On("nextEvents").Once().Return(Cursor{}, nil, assert.AnError)
		stream.On("closeStream").Once().Return(nil)

		stream.unblock <- struct{}{}
		_, _, err := streamAPI.NextEvents()

		assert.EqualError(t, assert.AnError, err.Error())
	})

	t.Run("successful read events", func(t *testing.T) {
		stream := &mockStreamer{unblock: make(chan struct{}, 1)}
		streamAPI, opener, _ := setupMockStream(nil, nil)

		opener.On("openStream").Once().Return(stream, nil)
		opener.unblock <- struct{}{}
		stream.On("nextEvents").Twice().Return(expectedCursor, expectedEvents, nil)

		stream.unblock <- struct{}{}
		cursor, events, err := streamAPI.NextEvents()

		require.NoError(t, err)
		assert.Equal(t, expectedCursor, cursor)
		assert.Equal(t, expectedEvents, events)

		stream.unblock <- struct{}{}
		cursor, events, err = streamAPI.NextEvents()

		require.NoError(t, err)
		assert.Equal(t, expectedCursor, cursor)
		assert.Equal(t, expectedEvents, events)

		opener.unblock <- struct{}{} // provoke an error when openStream is called
	})

	t.Run("fail with canceled", func(t *testing.T) {
		stream := &mockStreamer{unblock: make(chan struct{}, 1)}
		streamAPI, opener, _ := setupMockStream(nil, nil)

		opener.On("openStream").Once().Return(stream, nil)
		opener.unblock <- struct{}{}
		stream.On("nextEvents").Once().Return(Cursor{}, nil, context.Canceled)
		stream.On("closeStream").Once().Return(nil)

		stream.unblock <- struct{}{}
		_, _, err := streamAPI.NextEvents()

		assert.EqualError(t, context.Canceled, err.Error())

		opener.unblock <- struct{}{} // provoke an error when openStream is called
	})
}

func TestStreamAPI_CommitCursor(t *testing.T) {
	retryCh := make(chan error, 1)
	okCh := make(chan struct{}, 1)
	errorCh := make(chan error, 1)
	expectedCursor := Cursor{NakadiStreamID: "stream-id"}

	t.Run("fail with time out", func(t *testing.T) {
		streamAPI, _, committer := setupMockStream(retryCh, okCh)
		committer.On("commitCursor", expectedCursor).Twice().Return(assert.AnError)

		go func() {
			err := streamAPI.CommitCursor(expectedCursor)
			errorCh <- err
		}()

		committer.unblock <- struct{}{}

		err := <-retryCh
		assert.EqualError(t, assert.AnError, err.Error())

		time.Sleep(600 * time.Millisecond)
		committer.unblock <- struct{}{}

		err = <-errorCh
		assert.EqualError(t, assert.AnError, err.Error())
	})

	t.Run("fail retry succeed", func(t *testing.T) {
		streamAPI, _, committer := setupMockStream(retryCh, okCh)
		committer.On("commitCursor", expectedCursor).Once().Return(assert.AnError)

		go func() {
			err := streamAPI.CommitCursor(expectedCursor)
			errorCh <- err
		}()

		committer.unblock <- struct{}{}

		err := <-retryCh
		assert.EqualError(t, assert.AnError, err.Error())

		committer.On("commitCursor", expectedCursor).Once().Return(nil)
		committer.unblock <- struct{}{}

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
		streamAPI, _, committer := setupMockStream(nil, nil)
		committer.On("commitCursor", expectedCursor).Once().Return(nil)

		committer.unblock <- struct{}{}
		err := streamAPI.CommitCursor(expectedCursor)

		assert.NoError(t, err)
	})
}

func TestStreamAPI_Close(t *testing.T) {
	errorCh := make(chan error, 1)
	streamAPI, opener, _ := setupMockStream(nil, nil)

	stream := &mockStreamer{unblock: make(chan struct{}, 1)}
	opener.On("openStream").Once().Return(stream, nil)
	stream.On("nextEvents").Once().Return(Cursor{}, nil, assert.AnError)
	stream.On("closeStream").Once().Return(nil)

	go func() {
		err := streamAPI.Close()
		errorCh <- err
	}()

	opener.unblock <- struct{}{}
	time.Sleep(50 * time.Millisecond)
	stream.unblock <- struct{}{}

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

	opener := &mockStreamOpener{unblock: make(chan struct{}, 1)}
	committer := &mockCommitter{unblock: make(chan struct{}, 1)}

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
	unblock chan struct{}
}

func (so *mockStreamOpener) openStream() (streamer, error) {
	<-so.unblock
	args := so.Called()
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(streamer), nil
}

type mockStreamer struct {
	mock.Mock
	unblock chan struct{}
}

func (s *mockStreamer) nextEvents() (Cursor, []byte, error) {
	<-s.unblock
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
	unblock chan struct{}
}

func (c *mockCommitter) commitCursor(cursor Cursor) error {
	<-c.unblock
	return c.Called(cursor).Error(0)
}
