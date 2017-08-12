package nakadi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

type Subscription struct {
	ID                string    `json:"id,omitempty"`
	OwningApplication string    `json:"owning_application"`
	EventTypes        []string  `json:"event_types"`
	ConsumerGroup     string    `json:"consumer_group,omitempty"`
	ReadFrom          string    `json:"read_from,omitempty"`
	CreatedAt         time.Time `json:"created_at,omitempty"`
}

func NewSubscriptions(client *Client) SubscriptionAPI {
	return &httpSubscriptionAPI{
		client: client}
}

type SubscriptionAPI interface {
	List() ([]*Subscription, error)
	Get(id string) (*Subscription, error)
	Create(*Subscription) (*Subscription, error)
	Delete(id string) error
}

type httpSubscriptionAPI struct {
	client *Client
}

func (s *httpSubscriptionAPI) List() ([]*Subscription, error) {
	subscriptions := []*Subscription{}
	err := s.client.httpGET(s.subBaseURL(), &subscriptions, "unable to request subscriptions")
	if err != nil {
		return nil, err
	}
	return subscriptions, nil
}

func (s *httpSubscriptionAPI) Get(id string) (*Subscription, error) {
	subscription := &Subscription{}
	err := s.client.httpGET(s.subURL(id), subscription, "unable to request subscription")
	if err != nil {
		return nil, err
	}
	return subscription, err
}

func (s *httpSubscriptionAPI) Create(subscription *Subscription) (*Subscription, error) {
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

func (s *httpSubscriptionAPI) Delete(id string) error {
	return s.client.httpDELETE(s.subURL(id), "unable to delete subscription")
}

func (s *httpSubscriptionAPI) subURL(id string) string {
	return fmt.Sprintf("%s/subscriptions/%s", s.client.nakadiURL, id)
}

func (s *httpSubscriptionAPI) subBaseURL() string {
	return fmt.Sprintf("%s/subscriptions", s.client.nakadiURL)
}
