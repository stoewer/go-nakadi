package nakadi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// EventMetadata represents the meta information which comes along with all Nakadi events. For publishing
// purposes only the fields eid and occurred_at must be present.
type EventMetadata struct {
	EID        string            `json:"eid"`
	OccurredAt time.Time         `json:"occurred_at"`
	EventType  string            `json:"event_type,omitempty"`
	Partition  string            `json:"partition,omitempty"`
	ParentEIDs []string          `json:"parent_eids,omitempty"`
	FlowID     string            `json:"flow_id,omitempty"`
	ReceivedAt *time.Time        `json:"received_at,omitempty"`
	SpanCtx    map[string]string `json:"span_ctx,omitempty"`
}

// UndefinedEvent can be embedded in structs representing Nakadi events from the event category "undefined".
type UndefinedEvent struct {
	Metadata EventMetadata `json:"metadata"`
}

// BusinessEvent represents a Nakadi events from the category "business".
//
// Deprecated: use a custom struct and embed UndefinedEvent instead.
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

type CompressionAlgorithm string

const (
	CompressionAlgorithmGzip CompressionAlgorithm = "gzip"
)

var (
	supportedCompressionAlgorithms = map[CompressionAlgorithm]struct{}{CompressionAlgorithmGzip: {}}
)

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
	// Whether to enable compression or not. If set to false, CompressionAlgorithm and
	// CompressionLevel have no effect. (default: false)
	EnableCompression bool
	// CompressionAlgorithm the algorithm to be used. Supported values: [gzip]. Invalid values
	// default to gzip. (default: gzip)
	CompressionAlgorithm CompressionAlgorithm
	// CompressionLevel the level of compression to be used. Supported values: [1, 9]. Invalid values
	// default to gzip.DefaultCompression. (default: gzip.DefaultCompression)
	CompressionLevel int
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
	if copyOptions.CompressionAlgorithm == "" {
		copyOptions.CompressionAlgorithm = defaultCompressionAlgorithm
	}
	if _, ok := supportedCompressionAlgorithms[copyOptions.CompressionAlgorithm]; !ok {
		copyOptions.CompressionAlgorithm = defaultCompressionAlgorithm
	}
	if copyOptions.CompressionLevel == 0 {
		switch copyOptions.CompressionAlgorithm { // default level depends on the compression algorithm used
		case CompressionAlgorithmGzip:
			copyOptions.CompressionLevel = defaultGzipCompressionLevel
		default:
			copyOptions.CompressionLevel = defaultCompressionLevel
		}
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
			MaxElapsedTime:       options.MaxElapsedTime},
		compressionConf: compressionConfiguration{
			enableCompression: options.EnableCompression,
			algorithm:         options.CompressionAlgorithm,
			level:             options.CompressionLevel,
		},
	}
}

// PublishAPI is a sub API for publishing Nakadi events. All publish methods emit events as a single batch. If
// a publish method returns an error, the caller should check whether the error is a BatchItemsError in order to
// verify which events of a batch have been published.
type PublishAPI struct {
	client          *Client
	publishURL      string
	backOffConf     backOffConfiguration
	compressionConf compressionConfiguration
}

// PublishDataChangeEvent emits a batch of data change events. Depending on the options used when creating
// the PublishAPI this method will retry to publish the events if the were not successfully published.
func (p *PublishAPI) PublishDataChangeEvent(events []DataChangeEvent) error {
	return p.Publish(events)
}

// PublishBusinessEvent emits a batch of business events. Depending on the options used when creating
// the PublishAPI this method will retry to publish the events if the were not successfully published.
//
// Deprecated: use Publish with a custom struct with embedded UndefinedEvent instead.
func (p *PublishAPI) PublishBusinessEvent(events []BusinessEvent) error {
	return p.Publish(events)
}

// Publish is used to emit a batch of undefined events. But can also be used to publish data change or
// business events. Depending on the retry options used when creating the PublishAPI this method will retry
// to publish the events if the were not successfully published. Depending on the compression options used,
// the method will compress the payload before publishing.
func (p *PublishAPI) Publish(events interface{}) error {
	const errMsg = "unable to request event types"
	var response *http.Response
	var err error

	if p.compressionConf.enableCompression {
		response, err = p.client.httpPOSTWithCompression(p.backOffConf.create(), p.publishURL, events, errMsg, p.compressionConf)
	} else {
		response, err = p.client.httpPOST(p.backOffConf.create(), p.publishURL, events, errMsg)
	}

	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusMultiStatus || response.StatusCode == http.StatusUnprocessableEntity {
		batchItemError := BatchItemsError{}
		err := json.NewDecoder(response.Body).Decode(&batchItemError)
		if err != nil {
			return errors.Wrapf(err, "%s: unable to decode response body", errMsg)
		}
		return batchItemError
	}

	if response.StatusCode != http.StatusOK {
		buffer, err := io.ReadAll(response.Body)
		if err != nil {
			return errors.Wrapf(err, "%s: unable to read response body", errMsg)
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
	if err == nil {
		return ""
	}
	return "one or many events may have not been published"
}

// Format implements fmt.Formatter for BatchItemsError
func (err BatchItemsError) Format(s fmt.State, verb rune) {
	if err == nil {
		_, _ = io.WriteString(s, "nil")
		return
	}
	switch verb {
	case 'v':
		if s.Flag('+') {
			var messages []string
			for k, v := range err {
				messages = append(messages, fmt.Sprintf("[%d]: %+v", k, v))
			}

			builder := bytes.NewBuffer(make([]byte, 0))
			switch len(err) {
			case 0:
				builder.WriteString("an unknown error occurred while publishing event")
			case 1:
				builder.WriteString("an error occurred while publishing event: ")
			default:
				builder.WriteString("errors occurred while publishing events: ")
			}

			builder.WriteString(strings.Join(messages, ", "))
			_, _ = io.WriteString(s, builder.String())

			return
		}
		fallthrough
	case 's', 'q':
		_, _ = io.WriteString(s, err.Error())
	}
}
