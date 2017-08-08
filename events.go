package nakadi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// EventAPI is an interface which allows to inspect and manage event types on
// a Nakadi service.
type EventAPI interface {
	List() ([]*EventType, error)
	Get(name string) (*EventType, error)
	Save(eventType *EventType) (*EventType, error)
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
	return &httpEventTypeManager{client: client}
}

type httpEventTypeManager struct {
	client *Client
}

func (etm *httpEventTypeManager) List() ([]*EventType, error) {
	eventTypes := []*EventType{}
	err := etm.client.httpGET(etm.eventListURL(), &eventTypes, "unable to request event types")
	if err != nil {
		return nil, err
	}
	return eventTypes, nil
}

func (etm *httpEventTypeManager) Get(name string) (*EventType, error) {
	eventType := EventType{}
	err := etm.client.httpGET(etm.eventURL(name), &eventType, "unable to request event types")
	if err != nil {
		return nil, err
	}
	return &eventType, nil
}

func (etm *httpEventTypeManager) Save(eventType *EventType) (*EventType, error) {
	response, err := etm.client.httpPUT(etm.eventURL(eventType.Name), eventType)

	if response.StatusCode != http.StatusOK {
		problem := problemJSON{}
		err := json.NewDecoder(response.Body).Decode(&problem)
		if err != nil {
			errors.Wrap(err, "unable to decode response body")
		}
		return nil, errors.Errorf("unable to request event types: %s", problem.Detail)
	}

	eventType = &EventType{}
	err = json.NewDecoder(response.Body).Decode(eventType)
	if err != nil {
		errors.Wrap(err, "unable to decode response body")
	}

	return eventType, nil
}

func (etm *httpEventTypeManager) Delete(name string) error {
	return etm.client.httpDELETE(etm.eventURL(name), "unable to delete event type")
}

func (etm *httpEventTypeManager) eventURL(name string) string {
	return fmt.Sprintf("%s/event-types/%s", etm.client.nakadiURL, name)
}

func (etm *httpEventTypeManager) eventListURL() string {
	return fmt.Sprintf("%s/event-types", etm.client.nakadiURL)
}
