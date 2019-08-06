package nakadi_test

import (
	"fmt"
	"log"
	"time"

	"github.com/stoewer/go-nakadi"
)

func Example_complete() {
	//  create a new client
	client := nakadi.New("http://localhost:8080", &nakadi.ClientOptions{ConnectionTimeout: 500 * time.Millisecond})

	// create an event api create a new event type

	eventAPI := nakadi.NewEventAPI(client, &nakadi.EventOptions{Retry: true})
	eventType := &nakadi.EventType{
		Name:                 "test-type",
		OwningApplication:    "test-app",
		Category:             "data",
		EnrichmentStrategies: []string{"metadata_enrichment"},
		PartitionStrategy:    "random",
		Schema: &nakadi.EventTypeSchema{
			Type:   "json_schema",
			Schema: `{"properties":{"test":{"type":"string"}}}`,
		},
	}
	err := eventAPI.Create(eventType)
	if err != nil {
		log.Fatal(err)
	}

	// create a new subscription API and a new subscription

	subAPI := nakadi.NewSubscriptionAPI(client, &nakadi.SubscriptionOptions{Retry: true})
	auth := &nakadi.SubscriptionAuthorization{
		Admins:  []nakadi.AuthorizationAttribute{{DataType: "service", Value: "test-service"}},
		Readers: []nakadi.AuthorizationAttribute{{DataType: "service", Value: "test-service"}},
	}
	sub := &nakadi.Subscription{
		OwningApplication: "another-app",
		EventTypes:        []string{"test-type"},
		ReadFrom:          "begin",
		Authorization:     auth,
	}
	sub, err = subAPI.Create(sub)
	if err != nil {
		log.Fatal(err)
	}

	// create a publish api and publish events
	pubAPI := nakadi.NewPublishAPI(client, eventType.Name, nil)
	event := nakadi.DataChangeEvent{
		Metadata: nakadi.EventMetadata{
			EID:        "9aabcd94-7ebd-11e7-898b-97df92934aa5",
			OccurredAt: time.Now(),
		},
		Data:     map[string]string{"test": "some value"},
		DataOP:   "U",
		DataType: "test",
	}
	err = pubAPI.PublishDataChangeEvent([]nakadi.DataChangeEvent{event})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("event published")

	// create a new stream and read one event
	stream := nakadi.NewStream(client, sub.ID, nil)
	cursor, _, err := stream.NextEvents()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("1 event received")

	stream.CommitCursor(cursor)
	stream.Close()

	subAPI.Delete(sub.ID)
	eventAPI.Delete(eventType.Name)

	// Output: event published
	// 1 event received
}
