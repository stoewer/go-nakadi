package nakadi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// EventAPI is an interface which allows to inspect and manage event types on
// a Nakadi instance.
type EventAPI interface {
	// List returns all registered event types.
	List() ([]*EventType, error)

	// Get returns an event type based on its name.
	Get(name string) (*EventType, error)

	// Save creates or updates the provided event type.
	Save(eventType *EventType) (*EventType, error)

	// Delete removes an event type.
	Delete(name string) error
}

// An EventType defines a kind of event that can be processed on a Nakadi service.
type EventType struct {
	Name                 string               `json:"name"`
	OwningApplication    string               `json:"owning_application"`
	Category             string               `json:"category"`
	EnrichmentStrategies []string             `json:"enrichment_strategies,omitempty"`
	PartitionStrategy    string               `json:"partition_strategy,omitempty"`
	CompatibilityMode    string               `json:"compatibility_mode,omitempty"`
	Schema               *EventTypeSchema     `json:"schema"`
	PartitionKeyFields   []string             `json:"partition_key_fields"`
	DefaultStatistics    *EventTypeStatistics `json:"default_statistics,omitempty"`
	Options              *EventTypeOptions    `json:"options,omitempty"`
	CreatedAt            time.Time            `json:"created_at,omitempty"`
	UpdatedAt            time.Time            `json:"updated_at,omitempty"`
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

// NewEvents creates a new instance of a EventAPI implementation which can be used to
// manage event types on a specific Nakadi service.
func NewEvents(client *Client) EventAPI {
	return &httpEventAPI{client: client}
}

// httpEventAPI is the actual implementation of EventAPI
type httpEventAPI struct {
	client *Client
}

func (e *httpEventAPI) List() ([]*EventType, error) {
	eventTypes := []*EventType{}
	err := e.client.httpGET(e.eventBaseURL(), &eventTypes, "unable to request event types")
	if err != nil {
		return nil, err
	}
	return eventTypes, nil
}

func (e *httpEventAPI) Get(name string) (*EventType, error) {
	eventType := &EventType{}
	err := e.client.httpGET(e.eventURL(name), eventType, "unable to request event types")
	if err != nil {
		return nil, err
	}
	return eventType, nil
}

func (e *httpEventAPI) Save(eventType *EventType) (*EventType, error) {
	response, err := e.client.httpPUT(e.eventURL(eventType.Name), eventType)
	if err != nil {
		return nil, errors.Wrap(err, "unable to save event type")
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		problem := problemJSON{}
		err := json.NewDecoder(response.Body).Decode(&problem)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode response body")
		}
		return nil, errors.Errorf("unable to save event type: %s", problem.Detail)
	}

	eventType = &EventType{}
	err = json.NewDecoder(response.Body).Decode(eventType)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decode response body")
	}

	return eventType, nil
}

func (e *httpEventAPI) Delete(name string) error {
	return e.client.httpDELETE(e.eventURL(name), "unable to delete event type")
}

func (e *httpEventAPI) eventURL(name string) string {
	return fmt.Sprintf("%s/event-types/%s", e.client.nakadiURL, name)
}

func (e *httpEventAPI) eventBaseURL() string {
	return fmt.Sprintf("%s/event-types", e.client.nakadiURL)
}