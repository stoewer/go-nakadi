package nakadi

import (
	"net"
	"net/http"
	"time"
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
