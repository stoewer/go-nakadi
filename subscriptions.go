package nakadi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// Subscription represents a subscription as used by the Nakadi high level API.
type Subscription struct {
	ID                string    `json:"id,omitempty"`
	OwningApplication string    `json:"owning_application"`
	EventTypes        []string  `json:"event_types"`
	ConsumerGroup     string    `json:"consumer_group,omitempty"`
	ReadFrom          string    `json:"read_from,omitempty"`
	CreatedAt         time.Time `json:"created_at,omitempty"`
}

// NewSubscriptionAPI crates a new instance of the SubscriptionAPI. As for all sub APIs of the `go-nakadi` package
// NewSubscriptionAPI receives a configured Nakadi client.
func NewSubscriptionAPI(client *Client) *SubscriptionAPI {
	return &SubscriptionAPI{
		client: client}
}

// SubscriptionAPI is a sub API that is used to manage subscriptions.
type SubscriptionAPI struct {
	client *Client
}

// List returns all available subscriptions.
func (s *SubscriptionAPI) List() ([]*Subscription, error) {
	subscriptions := struct {
		Items []*Subscription `json:"items"`
	}{}
	err := s.client.httpGET(s.subBaseURL(), &subscriptions, "unable to request subscriptions")
	if err != nil {
		return nil, err
	}
	return subscriptions.Items, nil
}

// Get obtains a single subscription identified by its ID.
func (s *SubscriptionAPI) Get(id string) (*Subscription, error) {
	subscription := &Subscription{}
	err := s.client.httpGET(s.subURL(id), subscription, "unable to request subscription")
	if err != nil {
		return nil, err
	}
	return subscription, err
}

// Create initializes a new subscription. If the subscription already exists the pre existing subscription
// is returned.
func (s *SubscriptionAPI) Create(subscription *Subscription) (*Subscription, error) {
	response, err := s.client.httpPOST(s.subBaseURL(), subscription)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create subscription")
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		problem := problemJSON{}
		err := json.NewDecoder(response.Body).Decode(&problem)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode response body")
		}
		return nil, errors.Errorf("unable to create subscription: %s", problem.Detail)
	}

	subscription = &Subscription{}
	err = json.NewDecoder(response.Body).Decode(subscription)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decode response body")
	}

	return subscription, nil
}

// Delete removes an existing subscription.
func (s *SubscriptionAPI) Delete(id string) error {
	return s.client.httpDELETE(s.subURL(id), "unable to delete subscription")
}

func (s *SubscriptionAPI) subURL(id string) string {
	return fmt.Sprintf("%s/subscriptions/%s", s.client.nakadiURL, id)
}

func (s *SubscriptionAPI) subBaseURL() string {
	return fmt.Sprintf("%s/subscriptions", s.client.nakadiURL)
}
