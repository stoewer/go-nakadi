package nakadi

import (
	"fmt"
	"time"

	"encoding/json"
	"net/http"

	"github.com/pkg/errors"
)

type EventMetadata struct {
	EID        string    `json:"eid"`
	OccurredAt time.Time `json:"occurred_at"`
	EventType  string    `json:"event_type,omitempty"`
	Partition  string    `json:"partition,omitempty"`
	ParentEIDs []string  `josn:"parent_eids,omitempty"`
	FlowID     string    `json:"flow_id,omitempty"`
	ReceivedAt time.Time `json:"received_at,omitempty"`
}

type UndefinedEvent struct {
	Metadata EventMetadata `json:"metadata"`
}

type BusinessEvent struct {
	Metadata    EventMetadata `json:"metadata"`
	OrderNumber string        `json:"order_number"`
}

type DataChangeEvent struct {
	Metadata EventMetadata `json:"metadata"`
	Data     interface{}   `json:"data"`
	DataOP   string        `json:"data_op"`
	DataType string        `json:"data_type"`
}

func NewPublisher(client *Client, eventType string) PublishAPI {
	return &httpPublishAPI{
		client:     client,
		publishURL: fmt.Sprintf("%s/event-types/%s/events", client.nakadiURL, eventType)}
}

type PublishAPI interface {
	PublishDataChangeEvent(events []DataChangeEvent) error
	PublishBusinessEvent(events []BusinessEvent) error
	Publish(events interface{}) error
}

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

type BatchItemsError []BatchItemResponse

func (err BatchItemsError) Error() string {
	return "one or many events may have not been published"
}

type BatchItemResponse struct {
	EID              string `json:"eid"`
	PublishingStatus string `json:"publishing_status"`
	Step             string `json:"step"`
	Detail           string `json:"detail"`
}
