package nakadi

import (
	"context"
	"time"

	"github.com/cenkalti/backoff"
)

// A Cursor marks the current read position in a stream. It returned along with each received batch of
// events and is furthermore used to commit a batch of events (as well as all previous events).
type Cursor struct {
	Partition      string `json:"partition"`
	Offset         string `json:"offset"`
	EventType      string `json:"event_type"`
	CursorToken    string `json:"cursor_token"`
	NakadiStreamID string `json:"-"`
}

// A StreamAPI is a sub API which is used to consume events from a specific subscription using Nakadi's
// high level stream API. In order to ensure that only successfully processed events are committed, it is
// crucial to commit cursors of respective event batches in the same order they were received.
type StreamAPI interface {
	// NextEvents reads the next batch of events from the stream and returns the encoded events along with the
	// respective cursor. It blocks until the batch of events can be read from the stream, or the stream is closed.
	NextEvents() (Cursor, []byte, error)

	// CommitCursor commits a cursor to Nakadi.
	CommitCursor(cursor Cursor) error

	// Closes the stream.
	Close() error
}

// StreamOptions contains optional parameters that are used to create a StreamAPI.
type StreamOptions struct {
	// The initial (minimal) retry interval used for the exponential backoff. This value is applied for
	// stream initialization as well as for cursor commits.
	InitialRetryInterval time.Duration
	// MaxRetryInterval the maximum retry interval. Once the exponential backoff reaches this value
	// the retry intervals remain constant. This value is applied for stream initialization as well as
	// for cursor commits.
	MaxRetryInterval time.Duration
	// CommitMaxElapsedTime is the maximum time spent on retries when committing a cursor. Once this value
	// was reached the exponential backoff is halted and the cursor will not be committed.
	CommitMaxElapsedTime time.Duration
	// NotifyErr is called when an error occurs that leads to a retry. This notify function can be used to
	// detect unhealthy streams.
	NotifyErr func(error, time.Duration)
	// NotifyOK is called whenever a successful operation was completed. This notify function can be used
	// to detect that a stream is healthy again.
	NotifyOK func()
}

// defaultStreamOptions provides some default values
var defaultStreamOptions = StreamOptions{
	InitialRetryInterval: time.Millisecond * 50,
	MaxRetryInterval:     time.Minute,
	CommitMaxElapsedTime: time.Minute * 2,
}

// NewStream is used to instantiate a new steam processing sub API. As for all sub APIs of the `go-nakadi`
// package NewStream receives a configured Nakadi client. Furthermore a valid subscription ID must be
// provided. Use the SubscriptionAPI in order to obtain subscriptions. The options parameter can be used
// to configure the behavior of the stream. The options may be nil.
func NewStream(client *Client, subscriptionID string, options *StreamOptions) StreamAPI {
	var copyOptions StreamOptions
	if options != nil {
		copyOptions = *options
		if copyOptions.InitialRetryInterval == 0 {
			copyOptions.InitialRetryInterval = defaultStreamOptions.InitialRetryInterval
		}
		if copyOptions.MaxRetryInterval == 0 {
			copyOptions.MaxRetryInterval = defaultStreamOptions.MaxRetryInterval
		}
		if copyOptions.CommitMaxElapsedTime == 0 {
			copyOptions.CommitMaxElapsedTime = defaultStreamOptions.CommitMaxElapsedTime
		}
		if copyOptions.NotifyErr == nil {
			copyOptions.NotifyErr = func(_ error, _ time.Duration) {}
		}
		if copyOptions.NotifyOK == nil {
			copyOptions.NotifyOK = func() {}
		}
	} else {
		copyOptions = defaultStreamOptions
	}

	ctx, cancel := context.WithCancel(context.Background())

	streamBackOff := backoff.NewExponentialBackOff()
	streamBackOff.InitialInterval = copyOptions.InitialRetryInterval
	streamBackOff.MaxInterval = copyOptions.MaxRetryInterval

	commitBackOff := backoff.NewExponentialBackOff()
	commitBackOff.InitialInterval = copyOptions.InitialRetryInterval
	commitBackOff.MaxInterval = copyOptions.MaxRetryInterval

	s := &httpStreamAPI{
		opener: &simpleStreamOpener{
			client:         client,
			subscriptionID: subscriptionID},
		committer: &simpleCommitter{
			client:         client,
			subscriptionID: subscriptionID},
		eventCh:       make(chan eventsOrError, 10),
		ctx:           ctx,
		cancel:        cancel,
		streamBackOff: backoff.WithContext(streamBackOff, ctx),
		commitBackOff: backoff.WithContext(commitBackOff, ctx),
		notifyErr:     copyOptions.NotifyErr,
		notifyOK:      copyOptions.NotifyOK,
	}

	go s.startStream()

	return s
}

// httpStreamAPI the actual implementation of the StreamAPI
type httpStreamAPI struct {
	opener        streamOpener
	committer     committer
	eventCh       chan eventsOrError
	ctx           context.Context
	cancel        context.CancelFunc
	commitBackOff backoff.BackOff
	streamBackOff backoff.BackOff
	notifyErr     func(error, time.Duration)
	notifyOK      func()
}

func (s *httpStreamAPI) NextEvents() (Cursor, []byte, error) {
	select {
	case <-s.ctx.Done():
		return Cursor{}, nil, context.Canceled
	case next := <-s.eventCh:
		return next.cursor, next.events, next.err
	}
}

func (s *httpStreamAPI) CommitCursor(cursor Cursor) error {
	var err error

	backoff.RetryNotify(func() error {
		err = s.committer.commitCursor(cursor)
		return err
	}, s.commitBackOff, s.notifyErr)

	if err == nil {
		s.notifyOK()
	}

	return err
}

func (s *httpStreamAPI) Close() error {
	s.cancel()
	return nil
}

// startStream is used to start a background routine which consumes events using a streamOpener and streamer.
// this routine will never terminate (not even on errors) unless the stream is closed.
func (s *httpStreamAPI) startStream() {
	for {
		var err error
		var stream streamer

		backoff.RetryNotify(func() error {
			stream, err = s.opener.openStream()
			return err
		}, s.streamBackOff, s.notifyErr)

		if err != nil {
			continue
		}
		s.notifyOK()

		var cursor Cursor
		var events []byte
		for {
			// read the next events or close and return
			select {
			case <-s.ctx.Done():
				err = context.Canceled
			default:
				cursor, events, err = stream.nextEvents()
			}

			// write next result (events or error) to channel or close and return
			select {
			case <-s.ctx.Done():
				err = context.Canceled
			case s.eventCh <- eventsOrError{cursor: cursor, events: events, err: err}:
				// nothing
			}

			// in case of errors, close the stream and open a new one or abort when canceled
			if err != nil {
				if err == context.Canceled {
					stream.closeStream()
					return
				}
				break
			}
		}

		stream.closeStream()
	}
}

// streamOpener is a internally used interface which is used to establish a new stream.
type streamOpener interface {
	openStream() (streamer, error)
}

// streamer is a internally used interface which is used to consume events from a stream.
type streamer interface {
	nextEvents() (Cursor, []byte, error)
	closeStream() error
}

// committer is a internally used interface which is used to commit cursors.
type committer interface {
	commitCursor(cursor Cursor) error
}

// eventsOrError is used to represent a successful or failed batch read.
type eventsOrError struct {
	cursor Cursor
	events []byte
	err    error
}
