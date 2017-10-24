package nakadi

import (
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// newHTTPClient crates an http client which is used for non streaming requests.
func newHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			Dial:                (&net.Dialer{Timeout: timeout}).Dial,
			TLSHandshakeTimeout: timeout,
		},
	}
}

// newHTTPStream creates an http client which is used for streaming purposes.
func newHTTPStream(timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			Dial:                (&net.Dialer{Timeout: timeout}).Dial,
			TLSHandshakeTimeout: timeout,
		},
	}
}

// problemJSON is used to decode error responses.
type problemJSON struct {
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
	Type   string `json:"type"`
}

type errorJSON struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// decodeResponseToError will try do decode into problemJSON then errorJSON
// and extract details from this defined formats.
// It will fallback to creating and error with message body
// Second parameter is an error message
func decodeResponseToError(buffer []byte, msg string) error {
	problem := problemJSON{}
	err := json.Unmarshal(buffer, &problem)
	if err == nil {
		return errors.Errorf("%s: %s", msg, problem.Detail)
	}
	errJSON := &errorJSON{}

	err = json.Unmarshal(buffer, &errJSON)
	if err == nil {
		return errors.Errorf("%s: %s", msg, errJSON.ErrorDescription)
	}
	return errors.Errorf("%s: %s", msg, string(buffer))
}
