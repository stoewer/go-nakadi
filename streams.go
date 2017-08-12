// Copyright (c) 2017, A. Stoewer <adrian.stoewer@rz.ifi.lmu.de>
// All rights reserved.

package nakadi

import "context"

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

type StreamOptions struct{}

func NewStream(client *Client, subscriptionID string, options *StreamOptions) StreamAPI {
	ctx, cancel := context.WithCancel(context.Background())
	s := &httpStreamAPI{
		opener: &simpleStreamOpener{
			client:         client,
			subscriptionID: subscriptionID},
		eventCh: make(chan eventsOrError, 10),
		ctx:     ctx,
		cancel:  cancel}
	go s.startStream()
	return s
}

type streamer interface {
	nextEvents() (Cursor, []byte, error)
	closeStream() error
}

type committer interface {
	commitCursor(cursor Cursor) error
}

type streamOpener interface {
	openStream() (streamer, error)
}

type eventsOrError struct {
	cursor Cursor
	events []byte
	err    error
}

type httpStreamAPI struct {
	opener    streamOpener
	committer committer
	eventCh   chan eventsOrError
	ctx       context.Context
	cancel    context.CancelFunc
}

func (rs *httpStreamAPI) NextEvents() (Cursor, []byte, error) {
	return Cursor{}, nil, nil
}

func (rs *httpStreamAPI) CommitCursor(cursor Cursor) error {
	return nil
}

func (rs *httpStreamAPI) Close() error {
	rs.cancel()
	return nil
}

func (rs *httpStreamAPI) startStream() {

}
