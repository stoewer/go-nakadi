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

type ClientOptions struct {
	TokenProvider     func() (string, error)
	ConnectionTimeout time.Duration
}

type Client struct {
	nakadiURL     string
	tokenProvider func() (string, error)
	timeout       time.Duration
	httpClient    *http.Client
}

func New(url string, options *ClientOptions) *Client {
	var client *Client
	if options == nil {
		client = &Client{
			nakadiURL: url,
			timeout:   defaultTimeOut}
	} else {
		client = &Client{
			nakadiURL:     url,
			timeout:       options.ConnectionTimeout,
			tokenProvider: options.TokenProvider}

		if client.timeout == 0 {
			client.timeout = defaultTimeOut
		}
	}

	client.httpClient = newHTTPClient(client.timeout)
	return client
}

func (c *Client) httpGET(url string, body interface{}, msg string) error {
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return errors.Wrap(err, "unable to prepare request")
	}

	if c.tokenProvider != nil {
		token, err := c.tokenProvider()
		if err != nil {
			return errors.Wrap(err, "unable to prepare request")
		}
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return errors.Wrap(err, msg)
	}

	if response.StatusCode != http.StatusOK {
		problem := problemJSON{}
		err := json.NewDecoder(response.Body).Decode(&problem)
		if err != nil {
			return errors.Wrap(err, "unable to decode response body")
		}
		return errors.Errorf("%s: %s", msg, problem.Detail)
	}

	err = json.NewDecoder(response.Body).Decode(body)
	if err != nil {
		return errors.Wrap(err, "unable to decode response body")
	}

	return nil
}

func (c *Client) httpPUT(url string, body interface{}) (*http.Response, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, errors.Wrap(err, "unable to encode json body")
	}

	request, err := http.NewRequest("PUT", url, bytes.NewReader(encoded))
	if err != nil {
		return nil, errors.Wrap(err, "unable to prepare request")
	}

	if c.tokenProvider != nil {
		token, err := c.tokenProvider()
		if err != nil {
			return nil, errors.Wrap(err, "unable to prepare request")
		}
		request.Header.Set("Authorization", "Bearer "+token)
	}

	return c.httpClient.Do(request)
}

func (c *Client) httpPOST(url string, body interface{}) (*http.Response, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, errors.Wrap(err, "unable to encode json body")
	}

	request, err := http.NewRequest("POST", url, bytes.NewReader(encoded))
	if err != nil {
		return nil, errors.Wrap(err, "unable to prepare request")
	}

	if c.tokenProvider != nil {
		token, err := c.tokenProvider()
		if err != nil {
			return nil, errors.Wrap(err, "unable to prepare request")
		}
		request.Header.Set("Authorization", "Bearer "+token)
	}

	return c.httpClient.Do(request)
}

func (c *Client) httpDELETE(url, msg string) error {
	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return errors.Wrap(err, "unable to prepare request")
	}

	if c.tokenProvider != nil {
		token, err := c.tokenProvider()
		if err != nil {
			return errors.Wrap(err, "unable to prepare request")
		}
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return errors.Wrap(err, msg)
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		problem := problemJSON{}
		err := json.NewDecoder(response.Body).Decode(&problem)
		if err != nil {
			return errors.Wrap(err, "unable to decode response body")
		}
		return errors.Errorf("%s: %s", msg, problem.Detail)
	}

	return nil
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
		Subscription:  subscription}

	return opener.OpenStream()
}

func (c *Client) Publish(eventType string, event interface{}) error {
	return nil
}

func (c *Client) subscriptionURL() string {
	return fmt.Sprintf("%s/subscriptions", c.nakadiURL)
}

type Cursor struct {
	Partition      string `json:"partition"`
	Offset         string `json:"offset"`
	EventType      string `json:"event_type"`
	CursorToken    string `json:"cursor_token"`
	NakadiStreamID string `json:"-"`
}
