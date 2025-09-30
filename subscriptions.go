package nakadi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// AuthorizationAttribute represents a record for SubscriptionAuthorization and EventTypeAuthorization which is used by the Nakadi high level API.
type AuthorizationAttribute struct {
	DataType string `json:"data_type"`
	Value    string `json:"value"`
}

// SubscriptionAuthorization represents a subscription auth as used by the Nakadi high level API.
type SubscriptionAuthorization struct {
	Admins  []AuthorizationAttribute `json:"admins"`
	Readers []AuthorizationAttribute `json:"readers"`
}

// Subscription represents a subscription as used by the Nakadi high level API.
type Subscription struct {
	ID                string                     `json:"id,omitempty"`
	OwningApplication string                     `json:"owning_application"`
	EventTypes        []string                   `json:"event_types"`
	ConsumerGroup     string                     `json:"consumer_group,omitempty"`
	ReadFrom          string                     `json:"read_from,omitempty"`
	CreatedAt         time.Time                  `json:"created_at,omitempty"`
	Authorization     *SubscriptionAuthorization `json:"authorization,omitempty"`
}

// SubscriptionOptions is a set of optional parameters used to configure the SubscriptionAPI.
type SubscriptionOptions struct {
	// Whether methods of the SubscriptionAPI retry when a request fails. If
	// set to true InitialRetryInterval, MaxRetryInterval, and MaxElapsedTime have
	// no effect (default: false).
	Retry bool
	// The initial (minimal) retry interval used for the exponential backoff algorithm
	// when retry is enables.
	InitialRetryInterval time.Duration
	// MaxRetryInterval the maximum retry interval. Once the exponential backoff reaches
	// this value the retry intervals remain constant.
	MaxRetryInterval time.Duration
	// MaxElapsedTime is the maximum time spent on retries when performing a request.
	// Once this value was reached the exponential backoff is halted and the request will
	// fail with an error.
	MaxElapsedTime time.Duration
}

func (o *SubscriptionOptions) withDefaults() *SubscriptionOptions {
	var copyOptions SubscriptionOptions
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

// NewSubscriptionAPI crates a new instance of the SubscriptionAPI. As for all sub APIs of the `go-nakadi` package
// NewSubscriptionAPI receives a configured Nakadi client. The last parameter is a struct containing only optional \
// parameters. The options may be nil.
func NewSubscriptionAPI(client *Client, options *SubscriptionOptions) *SubscriptionAPI {
	options = options.withDefaults()

	return &SubscriptionAPI{
		client: client,
		backOffConf: backOffConfiguration{
			Retry:                options.Retry,
			InitialRetryInterval: options.InitialRetryInterval,
			MaxRetryInterval:     options.MaxRetryInterval,
			MaxElapsedTime:       options.MaxElapsedTime}}
}

// SubscriptionAPI is a sub API that is used to manage subscriptions.
type SubscriptionAPI struct {
	client      *Client
	backOffConf backOffConfiguration
}

// List returns all available subscriptions.
func (s *SubscriptionAPI) List() ([]*Subscription, error) {
	subscriptions := struct {
		Items []*Subscription `json:"items"`
	}{}
	err := s.client.httpGET(s.backOffConf.create(), s.subBaseURL(), &subscriptions, "unable to request subscriptions")
	if err != nil {
		return nil, err
	}
	return subscriptions.Items, nil
}

// Get obtains a single subscription identified by its ID.
func (s *SubscriptionAPI) Get(id string) (*Subscription, error) {
	subscription := &Subscription{}
	err := s.client.httpGET(s.backOffConf.create(), s.subURL(id), subscription, "unable to request subscription")
	if err != nil {
		return nil, err
	}
	return subscription, err
}

// Create initializes a new subscription. If the subscription already exists the pre-existing subscription
// is returned.
func (s *SubscriptionAPI) Create(subscription *Subscription) (*Subscription, error) {
	const errMsg = "unable to create subscription"

	response, err := s.client.httpPOST(s.backOffConf.create(), s.subBaseURL(), subscription, errMsg)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		buffer, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, errors.Wrapf(err, "%s: unable to read response body", errMsg)
		}
		return nil, decodeResponseToError(buffer, errMsg)
	}

	subscription = &Subscription{}
	err = json.NewDecoder(response.Body).Decode(subscription)
	if err != nil {
		return nil, errors.Wrapf(err, "%s: unable to decode response body", errMsg)
	}

	return subscription, nil
}

// Delete removes an existing subscription.
func (s *SubscriptionAPI) Delete(id string) error {
	return s.client.httpDELETE(s.backOffConf.create(), s.subURL(id), "unable to delete subscription")
}

// SubscriptionStats represents detailed statistics for the subscription
type SubscriptionStats struct {
	EventType  string            `json:"event_type"`
	Partitions []*PartitionStats `json:"partitions"`
}

// PartitionStats represents statistic information for the particular partition
type PartitionStats struct {
	Partition        string `json:"partition"`
	State            string `json:"state"`
	UnconsumedEvents int    `json:"unconsumed_events"`
	StreamID         string `json:"stream_id"`
}

type statsResponse struct {
	Items []*SubscriptionStats `json:"items"`
}

// GetStats returns statistic information for subscription
func (s *SubscriptionAPI) GetStats(id string) ([]*SubscriptionStats, error) {
	stats := &statsResponse{}
	if err := s.client.httpGET(s.backOffConf.create(), s.subURL(id)+"/stats", stats, "unable to get stats for subscription"); err != nil {
		return nil, err
	}
	return stats.Items, nil
}

func (s *SubscriptionAPI) subURL(id string) string {
	return fmt.Sprintf("%s/subscriptions/%s", s.client.nakadiURL, id)
}

func (s *SubscriptionAPI) subBaseURL() string {
	return fmt.Sprintf("%s/subscriptions", s.client.nakadiURL)
}
