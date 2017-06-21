// Copyright (c) 2017, A. Stoewer <adrian.stoewer@rz.ifi.lmu.de>
// All rights reserved.

package nakadi

import "github.com/cenkalti/backoff"

type StreamOpener interface {
	OpenStream() (Stream, error)
}

type ResilientStream struct {
	BackOff      backoff.BackOff
	StreamOpener StreamOpener
}
