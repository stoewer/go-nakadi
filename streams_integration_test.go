// +build integration

package nakadi

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationStreamAPI(t *testing.T) {
	events := []DataChangeEvent{}
	helperLoadTestData(t, "events-data-create-stream.json", &events)
	eventType := &EventType{}
	helperLoadTestData(t, "event-type-create.json", eventType)

	newSub := &Subscription{OwningApplication: "test-app", EventTypes: []string{eventType.Name}, ReadFrom: "begin"}
	subscription := helperCreateSubscriptions(t, eventType, newSub)[0]
	defer helperDeleteSubscriptions(t, eventType, subscription)

	client := New(defaultNakadiURL, &ClientOptions{ConnectionTimeout: time.Second})
	publishAPI := NewPublishAPI(client, eventType.Name, &PublishOptions{Retry: true})

	// publish events
	for _, e := range events {
		err := publishAPI.PublishDataChangeEvent([]DataChangeEvent{e})
		require.NoError(t, err)
	}

	// stream events
	streamAPI := NewStream(client, subscription.ID, &StreamOptions{BatchLimit: 2, CommitRetry: true})
	received := []DataChangeEvent{}
	for len(received) < len(events) {
		cursor, rawEvents, err := streamAPI.NextEvents()
		require.NoError(t, err)

		temp := []DataChangeEvent{}
		err = json.Unmarshal(rawEvents, &temp)
		require.NoError(t, err)
		assert.Len(t, temp, 2)
		received = append(received, temp...)

		err = streamAPI.CommitCursor(cursor)
		require.NoError(t, err)
	}

	// compare events
	require.Equal(t, len(events), len(received))
	for i, e := range events {
		assert.Equal(t, e.Metadata.EID, received[i].Metadata.EID)
		assert.Equal(t, e.Metadata.EventType, received[i].Metadata.EventType)
		assert.Equal(t, e.DataType, received[i].DataType)
		assert.Equal(t, e.DataOP, received[i].DataOP)
		assert.Equal(t, e.Data, received[i].Data)
	}

	err := streamAPI.Close()
	assert.NoError(t, err)
}
