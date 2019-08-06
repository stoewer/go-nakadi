// +build integration

package nakadi

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationProcessor(t *testing.T) {
	events := []DataChangeEvent{}
	helperLoadTestData(t, "events-data-create-stream.json", &events)
	eventType := &EventType{}
	helperLoadTestData(t, "event-type-create.json", eventType)

	auth := &SubscriptionAuthorization{
		Admins:  []AuthorizationAttribute{{DataType: "service", Value: "test-service"}},
		Readers: []AuthorizationAttribute{{DataType: "service", Value: "test-service"}},
	}
	newSub := &Subscription{OwningApplication: "test-app", EventTypes: []string{eventType.Name}, ReadFrom: "begin", Authorization: auth}
	subscription := helperCreateSubscriptions(t, eventType, newSub)[0]
	defer helperDeleteSubscriptions(t, eventType, subscription)

	client := New(defaultNakadiURL, &ClientOptions{ConnectionTimeout: time.Second})
	publishAPI := NewPublishAPI(client, eventType.Name, &PublishOptions{Retry: true})

	// publish events
	for _, e := range events {
		err := publishAPI.PublishDataChangeEvent([]DataChangeEvent{e})
		require.NoError(t, err)
	}

	// process events
	eventCh := make(chan DataChangeEvent, 1)

	processor := NewProcessor(client, subscription.ID, &ProcessorOptions{BatchLimit: 2, EventsPerMinute: 60})
	processor.Start(func(i int, id string, rawEvents []byte) error {
		events := []DataChangeEvent{}
		err := json.Unmarshal(rawEvents, &events)
		assert.NoError(t, err)
		if err != nil {
			return err
		}
		for _, e := range events {
			eventCh <- e
		}
		return nil
	})

	// compare events
	for i := 0; i < len(events); i++ {
		e := <-eventCh
		assert.Equal(t, events[i].Metadata.EID, e.Metadata.EID)
		assert.Equal(t, events[i].Metadata.EventType, e.Metadata.EventType)
		assert.Equal(t, events[i].DataType, e.DataType)
		assert.Equal(t, events[i].DataOP, e.DataOP)
		assert.Equal(t, events[i].Data, e.Data)
	}

	err := processor.Stop()
	assert.NoError(t, err)
}
