package nakadi

import (
	"fmt"
	"time"

	"encoding/json"
	"net/http"

	"github.com/pkg/errors"
)

// EventMetadata represents the meta information which comes along with all Nakadi events. For publishing
// purposes only the fields eid and occurred_at must be present.
type EventMetadata struct {
	EID        string    `json:"eid"`
	OccurredAt time.Time `json:"occurred_at"`
	EventType  string    `json:"event_type,omitempty"`
	Partition  string    `json:"partition,omitempty"`
	ParentEIDs []string  `json:"parent_eids,omitempty"`
	FlowID     string    `json:"flow_id,omitempty"`
	ReceivedAt time.Time `json:"received_at,omitempty"`
}

// UndefinedEvent can be embedded in structs representing Nakadi events from the event category "undefined".
type UndefinedEvent struct {
	Metadata EventMetadata `json:"metadata"`
}

// BusinessEvent represents a Nakadi events from the category "business".
type BusinessEvent struct {
	Metadata    EventMetadata `json:"metadata"`
	OrderNumber string        `json:"order_number"`
}

// DataChangeEvent is a Nakadi event from the event category "data".
type DataChangeEvent struct {
	Metadata EventMetadata `json:"metadata"`
	Data     interface{}   `json:"data"`
	DataOP   string        `json:"data_op"`
	DataType string        `json:"data_type"`
}

// NewPublishAPI creates a new instance of the PublishAPI which can be used to publish Nakadi events.
// As for all sub APIs of the `go-nakadi` package NewPublishAPI receives a configured Nakadi client.
// Furthermore the name of the event type must be provided.
func NewPublishAPI(client *Client, eventType string) *PublishAPI {
	return &PublishAPI{
		client:     client,
		publishURL: fmt.Sprintf("%s/event-types/%s/events", client.nakadiURL, eventType)}
}

// PublishAPI is a sub API for publishing Nakadi events. All publish methods emit events as a single batch. If
// a publish method returns an error, the caller should check whether the error is a BatchItemsError in order to
// verify which events of a batch have been published.
type PublishAPI struct {
	client     *Client
	publishURL string
}

// PublishDataChangeEvent emits a batch of data change events.
func (p *PublishAPI) PublishDataChangeEvent(events []DataChangeEvent) error {
	return p.Publish(events)
}

// PublishBusinessEvent emits a batch of business events.
func (p *PublishAPI) PublishBusinessEvent(events []BusinessEvent) error {
	return p.Publish(events)
}

// Publish is used to emit a batch of undefined events. But can also be used to publish data change or
// business events.
func (p *PublishAPI) Publish(events interface{}) error {
	response, err := p.client.httpPOST(p.publishURL, events)
	if err != nil {
		return errors.Wrap(err, "unable to publish event")
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusMultiStatus || response.StatusCode == http.StatusUnprocessableEntity {
		batchItemError := BatchItemsError{}
		err := json.NewDecoder(response.Body).Decode(&batchItemError)
		if err != nil {
			return errors.Wrap(err, "unable to decode response body")
		}
		return batchItemError
	}

	if response.StatusCode != http.StatusOK {
		problem := problemJSON{}
		err := json.NewDecoder(response.Body).Decode(&problem)
		if err != nil {
			return errors.Wrap(err, "unable to decode response body")
		}
		return errors.Errorf("unable to request event types: %s", problem.Detail)
	}

	return nil
}

// BatchItemResponse if a batch is only published partially each batch item response contains information
// about whether a singe event was successfully published or not.
type BatchItemResponse struct {
	EID              string `json:"eid"`
	PublishingStatus string `json:"publishing_status"`
	Step             string `json:"step"`
	Detail           string `json:"detail"`
}

// BatchItemsError represents an error which contains information about the publishing status of each single
// event in a batch.
type BatchItemsError []BatchItemResponse

// Error implements the error interface for BatchItemsError.
func (err BatchItemsError) Error() string {
	return "one or many events may have not been published"
}
