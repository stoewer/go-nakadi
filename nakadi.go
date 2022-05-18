/*
Package nakadi is a client library for the Nakadi event broker. It provides convenient access
to many features of Nakadi's API. The package can be used to manage event type definitions.

The EventAPI can be used to inspect existing event types and allows further to create new event
types and to update existing ones. The SubscriptionAPI provides subscription management: existing
subscriptions can be fetched from Nakadi and of course it is also possible to create new ones.
The PublishAPI of this package is used to broadcast event types of all event type categories via
Nakadi. Last but not least, the package also implements a StreamAPI, which enables event processing
on top of Nakadi's subscription based high level API.

To make the communication with Nakadi more resilient all sub APIs of this package can be configured
to retry failed requests using an exponential back-off algorithm.
*/
package nakadi

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v3"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

const (
	defaultTimeOut              = 30 * time.Second
	defaultInitialRetryInterval = time.Millisecond * 10
	defaultMaxRetryInterval     = 10 * time.Second
	defaultMaxElapsedTime       = 30 * time.Second
)

// A Client represents a basic configuration to access a Nakadi instance. The client is used to configure
// other sub APIs of the `go-nakadi` package.
type Client struct {
	nakadiURL        string
	tokenProvider    func() (string, error)
	timeout          time.Duration
	httpClient       *http.Client
	httpStreamClient *http.Client
}

// ClientOptions contains all non mandatory parameters used to instantiate the Nakadi client.
type ClientOptions struct {
	TokenProvider     func() (string, error)
	ConnectionTimeout time.Duration
	TracingOptions    TracingOptions
}

type TracingOptions struct {
	Tracer        opentracing.Tracer
	SpanName      string
	ComponentName string
}

func (o *ClientOptions) withDefaults() *ClientOptions {
	var copyOptions ClientOptions
	if o != nil {
		copyOptions = *o
	}
	if copyOptions.ConnectionTimeout == 0 {
		copyOptions.ConnectionTimeout = defaultTimeOut
	}
	return &copyOptions
}

// New creates a new Nakadi client. New receives the URL of the Nakadi instance the client should connect to.
// In addition the second parameter options can be used to configure the behavior of the client and of all sub
// APIs in this package. The options may be nil.
func New(url string, options *ClientOptions) *Client {
	options = options.withDefaults()

	client := &Client{
		nakadiURL:        url,
		timeout:          options.ConnectionTimeout,
		tokenProvider:    options.TokenProvider,
		httpClient:       newHTTPClient(options.ConnectionTimeout, &options.TracingOptions),
		httpStreamClient: newHTTPStream(options.ConnectionTimeout, &options.TracingOptions)}

	return client
}

// httpGET fetches json encoded data with a GET request.
func (c *Client) httpGET(backOff backoff.BackOff, url string, body interface{}, msg string) error {
	var response *http.Response
	err := backoff.Retry(func() error {
		request, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return backoff.Permanent(errors.Wrapf(err, "%s: unable to prepare request", msg))
		}

		if c.tokenProvider != nil {
			token, err := c.tokenProvider()
			if err != nil {
				return backoff.Permanent(errors.Wrapf(err, "%s: unable to prepare request", msg))
			}
			request.Header.Set("Authorization", "Bearer "+token)
		}

		response, err = c.httpClient.Do(request)
		if err != nil {
			return errors.Wrap(err, msg)
		}

		if response.StatusCode >= 500 {
			buffer, err := ioutil.ReadAll(response.Body)
			if err != nil {
				return errors.Wrapf(err, "%s: unable to read response body", msg)
			}
			err = decodeResponseToError(buffer, msg)
			response.Body.Close()
			return err
		}

		return nil
	}, backOff)

	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		buffer, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return errors.Wrap(err, "unable to read response body")
		}
		return decodeResponseToError(buffer, msg)
	}

	err = json.NewDecoder(response.Body).Decode(body)
	if err != nil {
		return errors.Wrap(err, "unable to decode response body")
	}

	return nil
}

// httpPUT sends json encoded data via PUT request and returns a response.
func (c *Client) httpPUT(backOff backoff.BackOff, url string, body interface{}, msg string) (*http.Response, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, errors.Wrapf(err, "%s: unable to encode json body", msg)
	}

	var response *http.Response
	err = backoff.Retry(func() error {
		request, err := http.NewRequest("PUT", url, bytes.NewReader(encoded))
		if err != nil {
			return backoff.Permanent(errors.Wrapf(err, "%s: unable to prepare request", msg))
		}

		request.Header.Set("Content-Type", "application/json;charset=UTF-8")
		if c.tokenProvider != nil {
			token, err := c.tokenProvider()
			if err != nil {
				return backoff.Permanent(errors.Wrapf(err, "%s: unable to prepare request", msg))
			}
			request.Header.Set("Authorization", "Bearer "+token)
		}

		response, err = c.httpClient.Do(request)
		if err != nil {
			return errors.Wrap(err, msg)
		}

		if response.StatusCode >= 500 {
			buffer, err := ioutil.ReadAll(response.Body)
			if err != nil {
				return errors.Wrapf(err, "%s: unable to read response body", msg)
			}
			err = decodeResponseToError(buffer, msg)
			response.Body.Close()
			return err
		}

		return nil
	}, backOff)

	return response, err
}

// httpPOST sends json encoded data via POST request and returns a response.
func (c *Client) httpPOST(backOff backoff.BackOff, url string, body interface{}, msg string) (*http.Response, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, errors.Wrapf(err, "%s: unable to encode json body", msg)
	}

	var response *http.Response
	err = backoff.Retry(func() error {
		request, err := http.NewRequest("POST", url, bytes.NewReader(encoded))
		if err != nil {
			return backoff.Permanent(errors.Wrapf(err, "%s: unable to prepare request", msg))
		}

		request.Header.Set("Content-Type", "application/json;charset=UTF-8")
		if c.tokenProvider != nil {
			token, err := c.tokenProvider()
			if err != nil {
				return backoff.Permanent(errors.Wrapf(err, "%s: unable to prepare request", msg))
			}
			request.Header.Set("Authorization", "Bearer "+token)
		}

		response, err = c.httpClient.Do(request)
		if err != nil {
			return errors.Wrap(err, msg)
		}

		if response.StatusCode >= 500 {
			buffer, err := ioutil.ReadAll(response.Body)
			if err != nil {
				return errors.Wrapf(err, "%s: unable to read response body", msg)
			}
			err = decodeResponseToError(buffer, msg)
			response.Body.Close()
			return err
		}

		return nil
	}, backOff)

	return response, err
}

// httpDELETE sends a DELETE request. On errors httpDELETE expects a response body to contain
// an error message in the format of application/problem+json.
func (c *Client) httpDELETE(backOff backoff.BackOff, url, msg string) error {
	var response *http.Response
	err := backoff.Retry(func() error {
		request, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return backoff.Permanent(errors.Wrapf(err, "%s: unable to prepare request", msg))
		}

		if c.tokenProvider != nil {
			token, err := c.tokenProvider()
			if err != nil {
				return backoff.Permanent(errors.Wrapf(err, "%s: unable to prepare request", msg))
			}
			request.Header.Set("Authorization", "Bearer "+token)
		}

		response, err = c.httpClient.Do(request)
		if err != nil {
			return errors.Wrap(err, msg)
		}

		if response.StatusCode >= 500 {
			buffer, err := ioutil.ReadAll(response.Body)
			if err != nil {
				return errors.Wrapf(err, "%s: unable to read response body", msg)
			}
			err = decodeResponseToError(buffer, msg)
			response.Body.Close()
			return err
		}

		return nil
	}, backOff)

	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		buffer, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return errors.Wrapf(err, "%s: unable to read response body", msg)
		}
		return decodeResponseToError(buffer, msg)
	}

	return nil
}
