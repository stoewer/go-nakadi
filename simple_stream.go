package nakadi

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/buger/jsonparser"
	"github.com/pkg/errors"
)

// simpleStreamOpener implements the streamOpener interface.
type simpleStreamOpener struct {
	client               *Client
	subscriptionID       string
	batchLimit           uint
	flushTimeout         uint
	maxUncommittedEvents uint
}

func (so *simpleStreamOpener) openStream() (streamer, error) {
	req, err := http.NewRequest("GET", so.streamURL(so.subscriptionID), nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create request")
	}

	if so.client.tokenProvider != nil {
		token, err := so.client.tokenProvider()
		if err != nil {
			return nil, errors.Wrap(err, "unable to open stream")
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := so.client.httpStreamClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create stream")
	}

	if response.StatusCode >= 400 {
		buffer, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return nil, errors.Wrap(err, "unable to read response body")
		}
		return nil, decodeResponseToError(buffer, "unable to open stream")
	}

	s := &simpleStream{
		nakadiStreamID: response.Header.Get("X-Nakadi-StreamId"),
		buffer:         bufio.NewReader(response.Body),
		closer:         response.Body,
		readTimeout:    2 * nakadiHeartbeatInterval,
	}

	return s, nil
}

func (so *simpleStreamOpener) streamURL(id string) string {
	queryParams := url.Values{}
	if so.batchLimit > 0 {
		queryParams.Add("batch_limit", strconv.FormatUint(uint64(so.batchLimit), 10))
	}
	if so.flushTimeout > 0 {
		queryParams.Add("batch_flush_timeout", strconv.FormatUint(uint64(so.flushTimeout), 10))
	}
	if so.maxUncommittedEvents > 0 {
		queryParams.Add("max_uncommitted_events", strconv.FormatUint(uint64(so.maxUncommittedEvents), 10))
	}

	return fmt.Sprintf("%s/subscriptions/%s/events?%s", so.client.nakadiURL, id, queryParams.Encode())
}

// simpleStream implements the streamer interface.
type simpleStream struct {
	nakadiStreamID string
	buffer         *bufio.Reader
	closer         io.Closer
	readTimeout    time.Duration
	line           bytes.Buffer
}

func (s *simpleStream) nextEvents(buffer bytes.Buffer) (Cursor, []byte, error) {
	buffer.Reset()
	if s.buffer == nil {
		return Cursor{}, nil, errors.New("failed to read next batch: stream is closed")
	}

	fragment, err := s.readLineTimeout()
	if err != nil {
		return Cursor{}, nil, errors.Wrap(err, "failed to read next batch")
	}
	buffer.Write(fragment)

	batch := struct {
		Cursor Cursor `json:"cursor"`
	}{}
	batch.Cursor.buffer = buffer
	err = json.Unmarshal(buffer.Bytes(), &batch)
	if err != nil {
		return Cursor{}, nil, errors.Wrap(err, "failed to unmarshal next batch")
	}
	batch.Cursor.NakadiStreamID = s.nakadiStreamID

	events, dataType, _, err := jsonparser.Get(buffer.Bytes(), "events")
	if err != nil && err != jsonparser.KeyPathNotFoundError {
		return Cursor{}, nil, errors.Wrap(err, "failed to read events in next batch")
	}
	if dataType == jsonparser.NotExist || dataType == jsonparser.Null {
		return batch.Cursor, nil, nil
	}
	return batch.Cursor, events, nil
}

func (s *simpleStream) readLineTimeout() ([]byte, error) {
	timer := time.AfterFunc(s.readTimeout, func() { s.closer.Close() })
	defer timer.Stop()
	return s.buffer.ReadSlice('\n')
}

func (s *simpleStream) closeStream() error {
	s.buffer = nil
	return s.closer.Close()
}

// simpleCommitter implements the committer interface.
type simpleCommitter struct {
	client         *Client
	subscriptionID string
}

func (s *simpleCommitter) commitCursor(cursor Cursor) error {
	wrap := &struct {
		Items []Cursor `json:"items"`
	}{Items: []Cursor{cursor}}

	data, err := json.Marshal(wrap)
	if err != nil {
		return errors.Wrap(err, "unable to unmarshal cursor")
	}

	req, err := http.NewRequest("POST", s.commitURL(s.subscriptionID), bytes.NewReader(data))
	if err != nil {
		return errors.Wrap(err, "unable to create request")
	}
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	req.Header.Set("X-Nakadi-StreamId", cursor.NakadiStreamID)
	if s.client.tokenProvider != nil {
		token, err := s.client.tokenProvider()
		if err != nil {
			return errors.Wrap(err, "unable to commit cursor")
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := s.client.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "unable to commit cursor")
	}
	defer response.Body.Close()

	if response.StatusCode >= 400 {
		buffer, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return errors.Wrap(err, "unable to read response body")
		}
		return decodeResponseToError(buffer, "unable to commit cursor")
	}

	return nil
}

func (s *simpleCommitter) commitURL(id string) string {
	return fmt.Sprintf("%s/subscriptions/%s/cursors", s.client.nakadiURL, id)
}
