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

// NewPublisher creates a new instance of the PublishAPI which can be used to publish Nakadi events.
// As for all sub APIs of the `go-nakadi` package NewPublisher receives a configured Nakadi client.
// Furthermore the name of the event type must be provided.
func NewPublisher(client *Client, eventType string) PublishAPI {
	return &httpPublishAPI{
		client:     client,
		publishURL: fmt.Sprintf("%s/event-types/%s/events", client.nakadiURL, eventType)}
}

// PublishAPI is a sub API for publishing Nakadi events. All publish methods emit events as a single batch. If
// a publish method returns an error, the caller should check whether the error is a BatchItemsError in order to
// verify which events of a batch have been published.
type PublishAPI interface {
	// PublishDataChangeEvent emits a batch of data change events.
	PublishDataChangeEvent(events []DataChangeEvent) error

	// PublishBusinessEvent emits a batch of business events.
	PublishBusinessEvent(events []BusinessEvent) error

	// Publish is used to emit a batch of undefined events. But can also be used to publish data change or
	// business events.
	Publish(events interface{}) error
}

// BatchItemsError represents an error which contains information about the publishing status of each single
// event in a batch.
type BatchItemsError []BatchItemResponse

// Error implements the error interface for BatchItemsError.
func (err BatchItemsError) Error() string {
	return "one or many events may have not been published"
}

// BatchItemResponse if a batch is only published partially each batch item response contains information
// about whether a singe event was successfully published or not.
type BatchItemResponse struct {
	EID              string `json:"eid"`
	PublishingStatus string `json:"publishing_status"`
	Step             string `json:"step"`
	Detail           string `json:"detail"`
}

// httpPublishAPI is the actual implementation of the PublishAPI
type httpPublishAPI struct {
	client     *Client
	publishURL string
}

func (p *httpPublishAPI) PublishDataChangeEvent(events []DataChangeEvent) error {
	return p.Publish(events)
}

func (p *httpPublishAPI) PublishBusinessEvent(events []BusinessEvent) error {
	return p.Publish(events)
}

func (p *httpPublishAPI) Publish(events interface{}) error {
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
