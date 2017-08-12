// Copyright (c) 2017, A. Stoewer <adrian.stoewer@rz.ifi.lmu.de>
// All rights reserved.

package nakadi

import (
	"testing"
	"time"

	"net/http"

	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/jarcoal/httpmock.v1"
)

func TestNew(t *testing.T) {
	t.Run("with timeout", func(t *testing.T) {
		timeout := 5 * time.Second
		client := New(defaultNakadiURL, &ClientOptions{ConnectionTimeout: timeout})

		require.NotNil(t, client)
		assert.Equal(t, client.nakadiURL, defaultNakadiURL)
		assert.Equal(t, client.timeout, timeout)
		assert.NotNil(t, client.httpClient)
		assert.Equal(t, timeout, client.httpClient.Timeout)
		assert.Nil(t, client.tokenProvider)
	})

	t.Run("with token provider", func(t *testing.T) {
		client := New(defaultNakadiURL, &ClientOptions{TokenProvider: func() (string, error) { return "token", nil }})

		require.NotNil(t, client)
		assert.Equal(t, client.nakadiURL, defaultNakadiURL)
		assert.Equal(t, client.timeout, defaultTimeOut)
		assert.NotNil(t, client.httpClient)
		assert.Equal(t, defaultTimeOut, client.httpClient.Timeout)
		assert.NotNil(t, client.tokenProvider)
	})

	t.Run("no options", func(t *testing.T) {
		url := "https://example.com/nakadi"
		client := New(url, nil)

		require.NotNil(t, client)
		assert.Equal(t, client.nakadiURL, url)
		assert.Equal(t, client.timeout, defaultTimeOut)
		assert.NotNil(t, client.httpClient)
		assert.Equal(t, defaultTimeOut, client.httpClient.Timeout)
		assert.Nil(t, client.tokenProvider)
	})
}

func TestClient_httpGET(t *testing.T) {
	body := map[string]string{}
	encoded := `{"key":"value"}`
	url := "/get-test"
	msg := "error message"

	setupClient := func(tokenProvider func() (string, error)) *Client {
		return &Client{
			tokenProvider: tokenProvider,
			httpClient:    http.DefaultClient}
	}

	t.Run("fail connection error", func(t *testing.T) {
		client := setupClient(nil)
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET", url, httpmock.NewErrorResponder(assert.AnError))

		err := client.httpGET(url, &body, msg)

		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
		assert.Regexp(t, msg, err)
	})

	t.Run("fail oauth token", func(t *testing.T) {
		client := setupClient(nil)
		client.tokenProvider = func() (string, error) { return "", assert.AnError }
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusOK, encoded))

		err := client.httpGET(url, &body, msg)

		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("success oauth token", func(t *testing.T) {
		client := setupClient(nil)
		client.tokenProvider = func() (string, error) { return "token", nil }
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, func(r *http.Request) (*http.Response, error) {
			assert.Equal(t, "Bearer token", r.Header.Get("Authorization"))
			return httpmock.NewStringResponse(http.StatusOK, encoded), nil
		})

		err := client.httpGET(url, &body, msg)

		require.NoError(t, err)
		assert.Equal(t, map[string]string{"key": "value"}, body)
	})

	t.Run("success", func(t *testing.T) {
		client := setupClient(nil)
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("GET", url, httpmock.NewStringResponder(http.StatusOK, encoded))

		err := client.httpGET(url, &body, msg)

		require.NoError(t, err)
		assert.Equal(t, map[string]string{"key": "value"}, body)
	})
}

func TestClient_httpPUT(t *testing.T) {
	expected := map[string]string{"key": "value"}
	url := "/put-test"

	setupClient := func(tokenProvider func() (string, error)) *Client {
		return &Client{
			tokenProvider: tokenProvider,
			httpClient:    http.DefaultClient}
	}

	t.Run("fail connection error", func(t *testing.T) {
		client := setupClient(nil)
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("PUT", url, httpmock.NewErrorResponder(assert.AnError))

		_, err := client.httpPUT(url, &expected)

		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail oauth token", func(t *testing.T) {
		client := setupClient(nil)
		client.tokenProvider = func() (string, error) { return "", assert.AnError }
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("PUT", url, httpmock.NewStringResponder(http.StatusOK, ""))

		_, err := client.httpPUT(url, &expected)

		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("success oauth token", func(t *testing.T) {
		client := setupClient(nil)
		client.tokenProvider = func() (string, error) { return "token", nil }
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("PUT", url, func(r *http.Request) (*http.Response, error) {
			assert.Equal(t, "Bearer token", r.Header.Get("Authorization"))
			return httpmock.NewStringResponse(http.StatusOK, ""), nil
		})

		response, err := client.httpPUT(url, &expected)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, response.StatusCode)
	})

	t.Run("success", func(t *testing.T) {
		client := setupClient(nil)
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("PUT", url, func(r *http.Request) (*http.Response, error) {
			body := map[string]string{}
			err := json.NewDecoder(r.Body).Decode(&body)
			require.NoError(t, err)
			assert.Equal(t, expected, body)
			return httpmock.NewStringResponse(http.StatusOK, ""), nil
		})

		response, err := client.httpPUT(url, &expected)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, response.StatusCode)
	})
}

func TestClient_httpPOST(t *testing.T) {
	expected := map[string]string{"key": "value"}
	url := "/post-test"

	setupClient := func(tokenProvider func() (string, error)) *Client {
		return &Client{
			tokenProvider: tokenProvider,
			httpClient:    http.DefaultClient}
	}

	t.Run("fail connection error", func(t *testing.T) {
		client := setupClient(nil)
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("POST", url, httpmock.NewErrorResponder(assert.AnError))

		_, err := client.httpPOST(url, &expected)

		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("fail oauth token", func(t *testing.T) {
		client := setupClient(nil)
		client.tokenProvider = func() (string, error) { return "", assert.AnError }
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("POST", url, httpmock.NewStringResponder(http.StatusOK, ""))

		_, err := client.httpPOST(url, &expected)

		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("success oauth token", func(t *testing.T) {
		client := setupClient(nil)
		client.tokenProvider = func() (string, error) { return "token", nil }
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("POST", url, func(r *http.Request) (*http.Response, error) {
			assert.Equal(t, "Bearer token", r.Header.Get("Authorization"))
			return httpmock.NewStringResponse(http.StatusOK, ""), nil
		})

		response, err := client.httpPOST(url, &expected)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, response.StatusCode)
	})

	t.Run("success", func(t *testing.T) {
		client := setupClient(nil)
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("POST", url, func(r *http.Request) (*http.Response, error) {
			body := map[string]string{}
			err := json.NewDecoder(r.Body).Decode(&body)
			require.NoError(t, err)
			assert.Equal(t, expected, body)
			return httpmock.NewStringResponse(http.StatusOK, ""), nil
		})

		response, err := client.httpPOST(url, &expected)

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, response.StatusCode)
	})
}

func TestClient_httpDELETE(t *testing.T) {
	msg := "error message"
	url := "/delete-test"

	setupClient := func(tokenProvider func() (string, error)) *Client {
		return &Client{
			tokenProvider: tokenProvider,
			httpClient:    http.DefaultClient}
	}

	t.Run("fail connection error", func(t *testing.T) {
		client := setupClient(nil)
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("DELETE", url, httpmock.NewErrorResponder(assert.AnError))

		err := client.httpDELETE(url, msg)

		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
		assert.Regexp(t, msg, err)
	})

	t.Run("fail oauth token", func(t *testing.T) {
		client := setupClient(nil)
		client.tokenProvider = func() (string, error) { return "", assert.AnError }
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("DELETE", url, httpmock.NewStringResponder(http.StatusOK, ""))

		err := client.httpDELETE(url, msg)

		require.Error(t, err)
		assert.Regexp(t, assert.AnError, err)
	})

	t.Run("success oauth token", func(t *testing.T) {
		client := setupClient(nil)
		client.tokenProvider = func() (string, error) { return "token", nil }
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("DELETE", url, func(r *http.Request) (*http.Response, error) {
			require.Equal(t, "Bearer token", r.Header.Get("Authorization"))
			return httpmock.NewStringResponse(http.StatusOK, ""), nil
		})

		err := client.httpDELETE(url, msg)

		assert.NoError(t, err)
	})

	t.Run("success", func(t *testing.T) {
		client := setupClient(nil)
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder("DELETE", url, httpmock.NewStringResponder(http.StatusOK, ""))

		err := client.httpDELETE(url, msg)

		assert.NoError(t, err)
	})
}
