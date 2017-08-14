// Copyright (c) 2017, A. Stoewer <adrian.stoewer@rz.ifi.lmu.de>
// All rights reserved.

package nakadi

import (
	"context"
	"time"

	"github.com/cenkalti/backoff"
)

var defaultStreamOptions = StreamOptions{
	InitialRetryInterval: time.Millisecond * 50,
	MaxRetryInterval:     time.Minute,
	StreamMaxElapsedTime: time.Minute * 5,
	CommitMaxElapsedTime: time.Minute * 2,
}

type Cursor struct {
	Partition      string `json:"partition"`
	Offset         string `json:"offset"`
	EventType      string `json:"event_type"`
	CursorToken    string `json:"cursor_token"`
	NakadiStreamID string `json:"-"`
}

type StreamAPI interface {
	NextEvents() (Cursor, []byte, error)
	CommitCursor(cursor Cursor) error
	Close() error
}

type StreamOptions struct {
	InitialRetryInterval time.Duration
	MaxRetryInterval     time.Duration
	StreamMaxElapsedTime time.Duration
	CommitMaxElapsedTime time.Duration
	NotifyErr            func(error, time.Duration)
	NotifyOK             func()
}

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
		if copyOptions.StreamMaxElapsedTime == 0 {
			copyOptions.StreamMaxElapsedTime = defaultStreamOptions.StreamMaxElapsedTime
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
	streamBackOff.MaxElapsedTime = copyOptions.StreamMaxElapsedTime

	commitBackOff := backoff.NewExponentialBackOff()
	commitBackOff.InitialInterval = copyOptions.InitialRetryInterval
	commitBackOff.MaxInterval = copyOptions.MaxRetryInterval
	commitBackOff.MaxElapsedTime = copyOptions.StreamMaxElapsedTime

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

type streamOpener interface {
	openStream() (streamer, error)
}

type streamer interface {
	nextEvents() (Cursor, []byte, error)
	closeStream() error
}

type committer interface {
	commitCursor(cursor Cursor) error
}

type eventsOrError struct {
	cursor Cursor
	events []byte
	err    error
}
