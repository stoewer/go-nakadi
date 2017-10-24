package nakadi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// EventMetadata represents the meta information which comes along with all Nakadi events. For publishing
// purposes only the fields eid and occurred_at must be present.
type EventMetadata struct {
	EID        string     `json:"eid"`
	OccurredAt time.Time  `json:"occurred_at"`
	EventType  string     `json:"event_type,omitempty"`
	Partition  string     `json:"partition,omitempty"`
	ParentEIDs []string   `json:"parent_eids,omitempty"`
	FlowID     string     `json:"flow_id,omitempty"`
	ReceivedAt *time.Time `json:"received_at,omitempty"`
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

// PublishOptions is a set of optional parameters used to configure the PublishAPI.
type PublishOptions struct {
	// Whether or not publish methods retry when publishing fails. If set to true
	// InitialRetryInterval, MaxRetryInterval, and MaxElapsedTime have no effect
	// (default: false).
	Retry bool
	// The initial (minimal) retry interval used for the exponential backoff algorithm
	// when retry is enables.
	InitialRetryInterval time.Duration
	// MaxRetryInterval the maximum retry interval. Once the exponential backoff reaches
	// this value the retry intervals remain constant.
	MaxRetryInterval time.Duration
	// MaxElapsedTime is the maximum time spent on retries when publishing events. Once
	// this value was reached the exponential backoff is halted and the events will not be
	// published.
	MaxElapsedTime time.Duration
}

func (o *PublishOptions) withDefaults() *PublishOptions {
	var copyOptions PublishOptions
	if o != nil {
		copyOptions = *o
	}
	if copyOptions.InitialRetryInterval == 0 {
		copyOptions.InitialRetryInterval = defaultInitialRetryInterval
	}
	if copyOptions.MaxRetryInterval == 0 {
		copyOptions.MaxRetryInterval = defaultMaxRetryInterval
	}
	if copyOptions.MaxElapsedTime == 0 {
		copyOptions.MaxElapsedTime = defaultMaxElapsedTime
	}
	return &copyOptions
}

// NewPublishAPI creates a new instance of the PublishAPI which can be used to publish
// Nakadi events. As for all sub APIs of the `go-nakadi` package NewPublishAPI receives a
// configured Nakadi client. Furthermore the name of the event type must be provided.
// The last parameter is a struct containing only optional parameters. The options may be
// nil.
func NewPublishAPI(client *Client, eventType string, options *PublishOptions) *PublishAPI {
	options = options.withDefaults()

	return &PublishAPI{
		client:     client,
		publishURL: fmt.Sprintf("%s/event-types/%s/events", client.nakadiURL, eventType),
		backOffConf: backOffConfiguration{
			Retry:                options.Retry,
			InitialRetryInterval: options.InitialRetryInterval,
			MaxRetryInterval:     options.MaxRetryInterval,
			MaxElapsedTime:       options.MaxElapsedTime}}
}

// PublishAPI is a sub API for publishing Nakadi events. All publish methods emit events as a single batch. If
// a publish method returns an error, the caller should check whether the error is a BatchItemsError in order to
// verify which events of a batch have been published.
type PublishAPI struct {
	client      *Client
	publishURL  string
	backOffConf backOffConfiguration
}

// PublishDataChangeEvent emits a batch of data change events. Depending on the options used when creating
// the PublishAPI this method will retry to publish the events if the were not successfully published.
func (p *PublishAPI) PublishDataChangeEvent(events []DataChangeEvent) error {
	return p.Publish(events)
}

// PublishBusinessEvent emits a batch of business events. Depending on the options used when creating
// the PublishAPI this method will retry to publish the events if the were not successfully published.
func (p *PublishAPI) PublishBusinessEvent(events []BusinessEvent) error {
	return p.Publish(events)
}

// Publish is used to emit a batch of undefined events. But can also be used to publish data change or
// business events. Depending on the options used when creating the PublishAPI this method will retry
// to publish the events if the were not successfully published.
func (p *PublishAPI) Publish(events interface{}) error {
	response, err := p.client.httpPOST(p.backOffConf.createBackOff(), p.publishURL, events)
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
		buffer, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return errors.Wrap(err, "unable to read response body")
		}
		return decodeResponseToError(buffer, "unable to request event types")
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
