// Copyright (c) 2017, A. Stoewer <adrian.stoewer@rz.ifi.lmu.de>
// All rights reserved.

package nakadi

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/pkg/errors"
)

type SimpleStreamOpener struct {
	NakadiURL     string
	TokenProvider TokenProvider
	HTTPClient    *http.Client
	HTTPStream    *http.Client
	Subscription  *Subscription
}

func (sso *SimpleStreamOpener) OpenStream() (Stream, error) {
	req, err := http.NewRequest("GET", sso.streamURL(sso.Subscription.ID), nil)
	if sso.TokenProvider != nil {
		err = sso.TokenProvider.Authorize(req)
		if err != nil {
			return nil, errors.Wrap(err, "unable to open stream")
		}
	}

	response, err := sso.HTTPStream.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create stream")
	}

	if response.StatusCode >= 400 {
		decoder := json.NewDecoder(response.Body)
		problem := &problemJSON{}
		err = decoder.Decode(problem)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode subscription error")
		}
		return nil, errors.Errorf("unable to open stream: %s", problem.Detail)
	}

	if response.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unable to open stream: unexpected response code %d", response.StatusCode)
	}

	stream := &SimpleStream{
		nakadiURL:      sso.NakadiURL,
		tokenProvider:  sso.TokenProvider,
		subscriptionID: sso.Subscription.ID,
		nakadiStreamID: response.Header.Get("X-Nakadi-StreamId"),
		httpClient:     sso.HTTPClient,
		buffer:         bufio.NewReader(response.Body),
		closer:         response.Body}

	return stream, nil
}

func (sso *SimpleStreamOpener) streamURL(id string) string {
	return fmt.Sprintf("%s/subscriptions/%s/events", sso.NakadiURL, id)
}

type SimpleStream struct {
	nakadiURL      string
	tokenProvider  TokenProvider
	subscriptionID string
	nakadiStreamID string
	httpClient     *http.Client
	buffer         *bufio.Reader
	closer         io.Closer
}

func (s *SimpleStream) Next() (*Cursor, []byte, error) {
	if s.buffer == nil {
		return nil, nil, errors.New("failed to read next batch: stream is closed")
	}

	line, isPrefix, err := s.buffer.ReadLine()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to read next batch")
	}

	for isPrefix {
		var add []byte
		add, isPrefix, err = s.buffer.ReadLine()
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to read next batch")
		}
		line = append(line, add...)
	}

	batch := struct {
		Cursor *Cursor          `json:"cursor"`
		Events *json.RawMessage `json:"events"`
	}{}
	err = json.Unmarshal(line, &batch)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to unmarshal next batch")
	}
	batch.Cursor.NakadiStreamID = s.nakadiStreamID

	if batch.Events == nil {
		return batch.Cursor, nil, nil
	}
	return batch.Cursor, []byte(*batch.Events), nil
}

func (s *SimpleStream) Commit(cursor *Cursor) error {
	wrap := &struct {
		Items []*Cursor `json:"items"`
	}{
		Items: []*Cursor{cursor},
	}

	data, err := json.Marshal(wrap)
	if err != nil {
		return errors.Wrap(err, "unable to unmarshal cursor")
	}

	req, err := http.NewRequest("POST", s.commitURL(s.subscriptionID), bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	req.Header.Set("X-Nakadi-StreamId", cursor.NakadiStreamID)
	if s.tokenProvider != nil {
		err = s.tokenProvider.Authorize(req)
		if err != nil {
			return errors.Wrap(err, "unable to commit cursor")
		}
	}

	response, err := s.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "unable to commit cursor")
	}
	defer response.Body.Close()

	if response.StatusCode >= 400 {
		decoder := json.NewDecoder(response.Body)
		problem := &problemJSON{}
		err = decoder.Decode(problem)
		if err != nil {
			return errors.Wrap(err, "unable to decode commit error")
		}
		return errors.Errorf("unable to commit cursor: %s", problem.Detail)
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		return errors.Errorf("unable commit cursor: unexpected response code %d", response.StatusCode)
	}

	return nil
}

func (s *SimpleStream) Close() error {
	s.buffer = nil
	return s.closer.Close()
}

func (s *SimpleStream) commitURL(id string) string {
	return fmt.Sprintf("%s/subscriptions/%s/cursors", s.nakadiURL, id)
}
