package nakadi

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// An EventType defines a kind of event that can be processed on a Nakadi service.
type EventType struct {
	Name                 string                  `json:"name"`
	OwningApplication    string                  `json:"owning_application"`
	Category             string                  `json:"category"`
	EnrichmentStrategies []string                `json:"enrichment_strategies,omitempty"`
	PartitionStrategy    string                  `json:"partition_strategy,omitempty"`
	CompatibilityMode    string                  `json:"compatibility_mode,omitempty"`
	Schema               *EventTypeSchema        `json:"schema"`
	PartitionKeyFields   []string                `json:"partition_key_fields"`
	DefaultStatistics    *EventTypeStatistics    `json:"default_statistics,omitempty"`
	Options              *EventTypeOptions       `json:"options,omitempty"`
	Authorization        *EventTypeAuthorization `json:"authorization,omitempty"`
	CreatedAt            time.Time               `json:"created_at,omitempty"`
	UpdatedAt            time.Time               `json:"updated_at,omitempty"`
}

// EventTypeSchema is a non optional description of the schema on an event type.
type EventTypeSchema struct {
	Version   string    `json:"version,omitempty"`
	Type      string    `json:"type"`
	Schema    string    `json:"schema"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}

// EventTypeStatistics describe operational statistics for an event type. This statistics are
// used by Nakadi to optimize the throughput events from a certain kind. They are provided on
// event type creation.
type EventTypeStatistics struct {
	MessagesPerMinute int `json:"messages_per_minute"`
	MessageSize       int `json:"message_size"`
	ReadParallelism   int `json:"read_parallelism"`
	WriteParallelism  int `json:"write_parallelism"`
}

// EventTypeOptions provide additional parameters for tuning Nakadi.
type EventTypeOptions struct {
	RetentionTime int64 `json:"retention_time"`
}

// EventTypeAuthorization represents authorization settings for an event type.
type EventTypeAuthorization struct {
	Admins  []AuthorizationAttribute `json:"admins"`
	Readers []AuthorizationAttribute `json:"readers"`
	Writers []AuthorizationAttribute `json:"writers"`
}

// EventOptions is a set of optional parameters used to configure the EventAPI.
type EventOptions struct {
	// Whether or not methods of the EventAPI retry when a request fails. If
	// set to true InitialRetryInterval, MaxRetryInterval, and MaxElapsedTime have
	// no effect (default: false).
	Retry bool
	// The initial (minimal) retry interval used for the exponential backoff algorithm
	// when retry is enables.
	InitialRetryInterval time.Duration
	// MaxRetryInterval the maximum retry interval. Once the exponential backoff reaches
	// this value the retry intervals remain constant.
	MaxRetryInterval time.Duration
	// MaxElapsedTime is the maximum time spent on retries when when performing a request.
	// Once this value was reached the exponential backoff is halted and the request will
	// fail with an error.
	MaxElapsedTime time.Duration
}

func (o *EventOptions) withDefaults() *EventOptions {
	var copyOptions EventOptions
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

// NewEventAPI creates a new instance of a EventAPI implementation which can be used to
// manage event types on a specific Nakadi service. The last parameter is a struct containing only
// optional parameters. The options may be nil.
func NewEventAPI(client *Client, options *EventOptions) *EventAPI {
	options = options.withDefaults()

	return &EventAPI{
		client: client,
		backOffConf: backOffConfiguration{
			Retry:                options.Retry,
			InitialRetryInterval: options.InitialRetryInterval,
			MaxRetryInterval:     options.MaxRetryInterval,
			MaxElapsedTime:       options.MaxElapsedTime}}
}

// EventAPI is a sub API that allows to inspect and manage event types on a Nakadi instance.
type EventAPI struct {
	client      *Client
	backOffConf backOffConfiguration
}

// List returns all registered event types.
func (e *EventAPI) List() ([]*EventType, error) {
	eventTypes := []*EventType{}
	err := e.client.httpGET(e.backOffConf.create(), e.eventBaseURL(), &eventTypes, "unable to request event types")
	if err != nil {
		return nil, err
	}
	return eventTypes, nil
}

// Get returns an event type based on its name.
func (e *EventAPI) Get(name string) (*EventType, error) {
	eventType := &EventType{}
	err := e.client.httpGET(e.backOffConf.create(), e.eventURL(name), eventType, "unable to request event types")
	if err != nil {
		return nil, err
	}
	return eventType, nil
}

// Create saves a new event type.
func (e *EventAPI) Create(eventType *EventType) error {
	const errMsg = "unable to create event type"

	response, err := e.client.httpPOST(e.backOffConf.create(), e.eventBaseURL(), eventType, errMsg)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		buffer, err := io.ReadAll(response.Body)
		if err != nil {
			return errors.Wrapf(err, "%s: unable to read response body", errMsg)
		}
		return decodeResponseToError(buffer, errMsg)
	}

	return nil
}

// Update updates an existing event type.
func (e *EventAPI) Update(eventType *EventType) error {
	const errMsg = "unable to update event type"

	response, err := e.client.httpPUT(e.backOffConf.create(), e.eventURL(eventType.Name), eventType, errMsg)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		buffer, err := io.ReadAll(response.Body)
		if err != nil {
			return errors.Wrapf(err, "%s: unable to read response body", errMsg)
		}
		return decodeResponseToError(buffer, "unable to update event type")
	}

	return nil
}

// Delete removes an event type.
func (e *EventAPI) Delete(name string) error {
	return e.client.httpDELETE(e.backOffConf.create(), e.eventURL(name), "unable to delete event type")
}

func (e *EventAPI) eventURL(name string) string {
	return fmt.Sprintf("%s/event-types/%s", e.client.nakadiURL, name)
}

func (e *EventAPI) eventBaseURL() string {
	return fmt.Sprintf("%s/event-types", e.client.nakadiURL)
}
