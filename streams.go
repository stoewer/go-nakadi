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

// StreamOptions contains optional parameters that are used to create a StreamAPI.
type StreamOptions struct {
	// The maximum number of Events in each chunk (and therefore per partition) of the stream (default: 1)
	BatchLimit uint
	// Maximum time in seconds to wait for the flushing of each chunk (per partition).(default: 30)
	FlushTimeout uint
	// The amount of uncommitted events Nakadi will stream before pausing the stream. When in paused
	// state and commit comes - the stream will resume. If MaxUncommittedEvents is lower than BatchLimit,
	// effective batch size will be upperbound by MaxUncommittedEvents. (default: 10, minimum: 1)
	MaxUncommittedEvents uint
	// The initial (minimal) retry interval used for the exponential backoff. This value is applied for
	// stream initialization as well as for cursor commits.
	InitialRetryInterval time.Duration
	// MaxRetryInterval the maximum retry interval. Once the exponential backoff reaches this value
	// the retry intervals remain constant. This value is applied for stream initialization as well as
	// for cursor commits.
	MaxRetryInterval time.Duration
	// MaxElapsedTime is the maximum time spent on retries when committing a cursor. Once this value
	// was reached the exponential backoff is halted and the cursor will not be committed.
	CommitMaxElapsedTime time.Duration
	// Whether or not CommitCursor will retry when a request fails. If
	// set to true InitialRetryInterval, MaxRetryInterval, and CommitMaxElapsedTime have
	// no effect for commit requests (default: false).
	CommitRetry bool
	// NotifyErr is called when an error occurs that leads to a retry. This notify function can be used to
	// detect unhealthy streams.
	NotifyErr func(error, time.Duration)
	// NotifyOK is called whenever a successful operation was completed. This notify function can be used
	// to detect that a stream is healthy again.
	NotifyOK func()
}

func (o *StreamOptions) withDefaults() *StreamOptions {
	var copyOptions StreamOptions
	if o != nil {
		copyOptions = *o
	}
	if copyOptions.InitialRetryInterval == 0 {
		copyOptions.InitialRetryInterval = defaultInitialRetryInterval
	}
	if copyOptions.MaxRetryInterval == 0 {
		copyOptions.MaxRetryInterval = defaultMaxRetryInterval
	}
	if copyOptions.CommitMaxElapsedTime == 0 {
		copyOptions.CommitMaxElapsedTime = defaultMaxElapsedTime
	}
	if copyOptions.NotifyErr == nil {
		copyOptions.NotifyErr = func(_ error, _ time.Duration) {}
	}
	if copyOptions.NotifyOK == nil {
		copyOptions.NotifyOK = func() {}
	}
	if copyOptions.MaxUncommittedEvents == 0 {
		copyOptions.MaxUncommittedEvents = 10
	}
	return &copyOptions
}

// NewStream is used to instantiate a new steam processing sub API. As for all sub APIs of the `go-nakadi`
// package NewStream receives a configured Nakadi client. Furthermore a valid subscription ID must be
// provided. Use the SubscriptionAPI in order to obtain subscriptions. The options parameter can be used
// to configure the behavior of the stream. The options may be nil.
func NewStream(client *Client, subscriptionID string, options *StreamOptions) *StreamAPI {
	options = options.withDefaults()

	ctx, cancel := context.WithCancel(context.Background())

	streamAPI := &StreamAPI{
		opener: &simpleStreamOpener{
			client:               client,
			subscriptionID:       subscriptionID,
			batchLimit:           options.BatchLimit,
			flushTimeout:         options.FlushTimeout,
			maxUncommittedEvents: options.MaxUncommittedEvents},
		committer: &simpleCommitter{
			client:         client,
			subscriptionID: subscriptionID},
		eventCh: make(chan eventsOrError, 10),
		ctx:     ctx,
		cancel:  cancel,
		streamBackOffConf: backOffConfiguration{
			Retry:                true,
			InitialRetryInterval: options.InitialRetryInterval,
			MaxRetryInterval:     options.MaxRetryInterval,
		},
		commitBackOffConf: backOffConfiguration{
			Retry:                options.CommitRetry,
			InitialRetryInterval: options.InitialRetryInterval,
			MaxRetryInterval:     options.MaxRetryInterval,
			MaxElapsedTime:       options.CommitMaxElapsedTime,
		},
		notifyErr: options.NotifyErr,
		notifyOK:  options.NotifyOK}

	go streamAPI.startStream()

	return streamAPI
}

// A StreamAPI is a sub API which is used to consume events from a specific subscription using Nakadi's
// high level stream API. In order to ensure that only successfully processed events are committed, it is
// crucial to commit cursors of respective event batches in the same order they were received.
type StreamAPI struct {
	opener            streamOpener
	committer         committer
	eventCh           chan eventsOrError
	ctx               context.Context
	cancel            context.CancelFunc
	commitBackOffConf backOffConfiguration
	streamBackOffConf backOffConfiguration
	notifyErr         func(error, time.Duration)
	notifyOK          func()
}

// NextEvents reads the next batch of events from the stream and returns the encoded events along with the
// respective cursor. It blocks until the batch of events can be read from the stream, or the stream is closed.
func (s *StreamAPI) NextEvents() (Cursor, []byte, error) {
	select {
	case <-s.ctx.Done():
		return Cursor{}, nil, context.Canceled
	case next := <-s.eventCh:
		return next.cursor, next.events, next.err
	}
}

// CommitCursor commits a cursor to Nakadi.
func (s *StreamAPI) CommitCursor(cursor Cursor) error {
	var err error

	commitBackOff := backoff.WithContext(s.commitBackOffConf.create(), s.ctx)
	backoff.RetryNotify(func() error {
		err = s.committer.commitCursor(cursor)
		return err
	}, commitBackOff, s.notifyErr)

	if err == nil {
		s.notifyOK()
	}

	return err
}

// Close ends the stream.
func (s *StreamAPI) Close() error {
	s.cancel()
	return nil
}

// startStream is used to start a background routine which consumes events using a streamOpener and streamer.
// this routine will never terminate (not even on errors) unless the stream is closed.
func (s *StreamAPI) startStream() {
	for {
		var err error
		var stream streamer

		streamBackOff := backoff.WithContext(s.streamBackOffConf.create(), s.ctx)
		backoff.RetryNotify(func() error {
			stream, err = s.opener.openStream()
			return err
		}, streamBackOff, s.notifyErr)

		if err != nil {
			continue
		}
		s.notifyOK()

		var cursor Cursor
		var events []byte
		for {
			select {
			case <-s.ctx.Done():
				err = context.Canceled
			default:
				cursor, events, err = stream.nextEvents()
			}

			if err == nil && len(events) == 0 {
				continue
			}

			select {
			case <-s.ctx.Done():
				err = context.Canceled
			case s.eventCh <- eventsOrError{cursor: cursor, events: events, err: err}:
				// nothing
			}

			if err != nil {
				if err == context.Canceled {
					stream.closeStream()
					close(s.eventCh)
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
