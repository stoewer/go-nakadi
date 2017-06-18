// Copyright (c) 2017, A. Stoewer <adrian.stoewer@rz.ifi.lmu.de>
// All rights reserved.

package event

import (
	"encoding/json"
	"time"
)

type Metadata struct {
	EID        string    `json:"eid,omitempty"`
	EventType  string    `json:"event_type,omitempty"`
	Partition  string    `json:"partition,omitempty"`
	FlowID     string    `json:"flow_id,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
	ReceivedAt time.Time `json:"received_at,omitempty"`
}

type Undefined struct {
	Metadata Metadata `json:"metadata"`
	*json.RawMessage
}

type Business struct {
	Metadata    Metadata `json:"metadata"`
	OrderNumber string   `json:"order_number"`
	*json.RawMessage
}

type DataChange struct {
	Metadata Metadata         `json:"metadata"`
	Data     *json.RawMessage `json:"data"`
	DataOP   string           `json:"data_op"`
	DataType string           `json:"data_type"`
}
