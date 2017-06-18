// Copyright (c) 2017, A. Stoewer <adrian.stoewer@rz.ifi.lmu.de>
// All rights reserved.

package nakadi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

const (
	defaultTimeOut   = 30 * time.Second
	defaultNakadiURL = "http://localhost:8080"
)

type Stream interface {
	Next() (*Cursor, []byte, error)
	Commit(*Cursor) error
	Close() error
}

type TokenProvider func() (string, error)

func (fn TokenProvider) Authorize(req *http.Request) error {
	if fn != nil {
		return errors.New("no token func")
	}

	token, err := fn()
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	return nil
}

type Option func(*Client)

func URL(url string) Option {
	return func(c *Client) { c.nakadiURL = url }
}

func Tokens(tokens TokenProvider) Option {
	return func(c *Client) { c.tokenProvider = tokens }
}

func Timeout(timeout time.Duration) Option {
	return func(c *Client) { c.timeout = timeout }
}

func HTTPClient(httpClient *http.Client) Option {
	return func(c *Client) { c.httpClient = httpClient }
}

func HTTPStream(httpStream *http.Client) Option {
	return func(c *Client) { c.httpStream = httpStream }
}

type Client struct {
	nakadiURL     string
	tokenProvider TokenProvider
	timeout       time.Duration
	httpClient    *http.Client
	httpStream    *http.Client
}

func New(options ...Option) *Client {
	client := &Client{
		nakadiURL: defaultNakadiURL,
		timeout:   defaultTimeOut}

	for _, option := range options {
		option(client)
	}
	if client.httpClient == nil {
		client.httpClient = newHTTPClient(client.timeout)
	}
	if client.httpStream == nil {
		client.httpStream = newHTTPStream(client.timeout)
	}

	return client
}

func (c *Client) Subscribe(owningApplication, eventType, consumerGroup string) (*Subscription, error) {
	sub := &Subscription{
		OwningApplication: owningApplication,
		EventTypes:        []string{eventType},
		ConsumerGroup:     consumerGroup}

	body, err := json.Marshal(sub)
	if err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal subscription")
	}

	req, err := http.NewRequest("POST", c.subscriptionURL(), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	if c.tokenProvider != nil {
		token, err := c.tokenProvider()
		if err != nil {
			return nil, errors.Wrap(err, "unable to create subscription")
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create subscription")
	}
	defer response.Body.Close()

	decoder := json.NewDecoder(response.Body)
	if response.StatusCode >= 400 {
		problem := &problemJSON{}
		err = decoder.Decode(problem)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode subscription error")
		}
		return nil, errors.Errorf("unable to create subscription: %s", problem.Detail)
	}

	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unable to create subscription: unexpected response code %d", response.StatusCode)
	}

	err = decoder.Decode(sub)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decode subscription")
	}

	return sub, nil
}

func (c *Client) Stream(subscription *Subscription) (Stream, error) {
	opener := &SimpleStreamOpener{
		NakadiURL:     c.nakadiURL,
		TokenProvider: c.tokenProvider,
		HTTPClient:    c.httpClient,
		HTTPStream:    c.httpStream,
		Subscription:  subscription}

	return opener.OpenStream()
}

func (c *Client) Publish(eventType string, event interface{}) error {
	return nil
}

func (c *Client) subscriptionURL() string {
	return fmt.Sprintf("%s/subscriptions", c.nakadiURL)
}

type Subscription struct {
	ID                string   `json:"id,omitempty"`
	OwningApplication string   `json:"owning_application"`
	EventTypes        []string `json:"event_types"`
	ConsumerGroup     string   `json:"consumer_group,omitempty"`
	ReadFrom          string   `json:"read_from,omitempty"`
	CreatedAt         string   `json:"created_at,omitempty"`
}

type Cursor struct {
	Partition      string `json:"partition"`
	Offset         string `json:"offset"`
	EventType      string `json:"event_type"`
	CursorToken    string `json:"cursor_token"`
	NakadiStreamID string `json:"-"`
}
