// +build integration

package nakadi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationEventAPI_Get(t *testing.T) {
	expected := &EventType{}
	helperLoadTestData(t, "event-type-create.json", expected)
	helperCreateEventTypes(t, expected)
	defer helperDeleteEventTypes(t, expected)

	client := New(defaultNakadiURL, &ClientOptions{ConnectionTimeout: time.Second})
	eventAPI := NewEventAPI(client)

	t.Run("fail not found", func(t *testing.T) {
		_, err := eventAPI.Get("does-not-exist")

		require.Error(t, err)
		assert.Regexp(t, "does not exist", err)
	})

	t.Run("success", func(t *testing.T) {
		eventType, err := eventAPI.Get(expected.Name)

		require.NoError(t, err)
		assert.Equal(t, expected.Name, eventType.Name)
		assert.Equal(t, expected.OwningApplication, eventType.OwningApplication)
		assert.Equal(t, expected.Category, eventType.Category)
		assert.Equal(t, expected.EnrichmentStrategies, eventType.EnrichmentStrategies)
		assert.Equal(t, expected.PartitionStrategy, eventType.PartitionStrategy)
		assert.Equal(t, expected.Schema.Schema, eventType.Schema.Schema)
		assert.Equal(t, expected.PartitionKeyFields, eventType.PartitionKeyFields)
	})
}

func TestIntegrationEventAPI_List(t *testing.T) {
	expected := []*EventType{}
	helperLoadTestData(t, "event-types-create.json", &expected)
	helperCreateEventTypes(t, expected...)
	defer helperDeleteEventTypes(t, expected...)

	client := New(defaultNakadiURL, &ClientOptions{ConnectionTimeout: time.Second})
	eventAPI := NewEventAPI(client)

	eventTypes, err := eventAPI.List()
	require.NoError(t, err)
	assert.Len(t, eventTypes, 2)
}

func TestIntegrationEventAPI_Create(t *testing.T) {
	eventType := &EventType{}
	helperLoadTestData(t, "event-type-create.json", eventType)

	client := New(defaultNakadiURL, &ClientOptions{ConnectionTimeout: time.Second})
	eventAPI := NewEventAPI(client)

	t.Run("fail invalid event type", func(t *testing.T) {
		err := eventAPI.Create(&EventType{Name: "foo"})

		require.Error(t, err)
		assert.Regexp(t, "unable to create event type", err)
	})

	t.Run("success", func(t *testing.T) {
		err := eventAPI.Create(eventType)

		require.NoError(t, err)
		helperDeleteEventTypes(t, eventType)
	})
}

func TestIntegrationEventAPI_Update(t *testing.T) {
	eventType := &EventType{}
	helperLoadTestData(t, "event-type-create.json", eventType)
	helperCreateEventTypes(t, eventType)
	defer helperDeleteEventTypes(t, eventType)

	client := New(defaultNakadiURL, &ClientOptions{ConnectionTimeout: time.Second})
	eventAPI := NewEventAPI(client)

	t.Run("fail not found", func(t *testing.T) {
		copyEventType := *eventType
		copyEventType.Name = "random"
		err := eventAPI.Update(&copyEventType)

		require.Error(t, err)
		assert.Regexp(t, "does not exist", err)
	})

	t.Run("fail incompatible change", func(t *testing.T) {
		copyEventType := *eventType
		copyEventType.PartitionStrategy = "random"
		err := eventAPI.Update(&copyEventType)

		require.Error(t, err)
		assert.Regexp(t, "changing partition_strategy is only allowed if the original strategy was 'random'", err)
	})

	t.Run("success", func(t *testing.T) {
		schema := `{"properties":{"test":{"type":"string"},"test2":{"type":"string"}},"additionalProperties":true}`
		eventType.Schema.Schema = schema
		err := eventAPI.Update(eventType)

		require.NoError(t, err)
	})
}

func TestIntegrationEventAPI_Delete(t *testing.T) {
	expected := &EventType{}
	helperLoadTestData(t, "event-type-create.json", expected)
	helperCreateEventTypes(t, expected)

	client := New(defaultNakadiURL, &ClientOptions{ConnectionTimeout: time.Second})
	eventAPI := NewEventAPI(client)

	t.Run("fail not found", func(t *testing.T) {
		err := eventAPI.Delete("does-not-exist")

		require.Error(t, err)
		assert.Regexp(t, "does not exist", err)
	})

	t.Run("success", func(t *testing.T) {
		err := eventAPI.Delete(expected.Name)

		require.NoError(t, err)
	})
}

func helperCreateEventTypes(t *testing.T, eventTypes ...*EventType) {
	for _, eventType := range eventTypes {
		serialized, err := json.Marshal(eventType)
		require.NoError(t, err)
		_, err = http.DefaultClient.Post(defaultNakadiURL+"/event-types", "application/json", bytes.NewReader(serialized))
		require.NoError(t, err)
	}
}

func helperDeleteEventTypes(t *testing.T, eventTypes ...*EventType) {
	for _, e := range eventTypes {
		request, err := http.NewRequest("DELETE", defaultNakadiURL+"/event-types/"+e.Name, nil)
		require.NoError(t, err)
		_, err = http.DefaultClient.Do(request)
		require.NoError(t, err)
	}
}
