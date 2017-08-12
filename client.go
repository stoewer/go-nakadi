// Copyright (c) 2017, A. Stoewer <adrian.stoewer@rz.ifi.lmu.de>
// All rights reserved.

package nakadi

import (
	"bytes"
	"encoding/json"
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
	nakadiURL        string
	tokenProvider    func() (string, error)
	timeout          time.Duration
	httpClient       *http.Client
	httpStreamClient *http.Client
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
	client.httpStreamClient = newHTTPStream(client.timeout)

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
	defer response.Body.Close()

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

	request.Header.Set("Content-Type", "application/json;charset=UTF-8")
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

	request.Header.Set("Content-Type", "application/json;charset=UTF-8")
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
	defer response.Body.Close()

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
