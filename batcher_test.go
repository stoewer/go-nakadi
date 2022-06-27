package nakadi

import (
	"fmt"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"testing"
	"time"
)

// Check that publishing batcher is getting closed after usage
func TestBatchPublisher_Close(t *testing.T) {
	t.Run("Test closing", func(t *testing.T) {
		batcher := NewBatchPublisher(nil, &BatchPublisherOptions{})
		batcher.Close()
	})
}

func TestBatchPublisher_Publish(t *testing.T) {
	t.Run("Test that batching by max batch size is working", func(t *testing.T) {
		const maxBatchSize = 4
		batcher, mockAPI := setupTestBatchPublisher(24*time.Hour, maxBatchSize)
		defer batcher.Close()

		dataToPublish := make(map[string]struct{})
		for index := 0; index < maxBatchSize*2; index++ {
			dataToPublish[fmt.Sprintf("Some data %d", index)] = struct{}{}
		}
		mockAPI.On("Publish", mock.Anything).Twice().Return(nil)

		finish := make(chan bool, 2*maxBatchSize)
		for item := range dataToPublish {
			go func(itemToPublish string) {
				err := batcher.Publish(itemToPublish)
				assert.NoError(t, err)
				finish <- true
			}(item)
		}
		for i := 0; i < len(dataToPublish); i++ {
			<-finish
		}

		mockAPI.AssertNumberOfCalls(t, "Publish", 2)
		assert.Equal(t, dataToPublish, mockAPI.dataPublished)
		assert.Equal(t, []int{maxBatchSize, maxBatchSize}, mockAPI.batchSizes)
	})
	t.Run("Test that error is propagated to calling functions", func(t *testing.T) {
		const maxBatchSize = 4

		batcher, mockAPI := setupTestBatchPublisher(24*time.Hour, maxBatchSize)
		defer batcher.Close()

		dataToPublish := make(map[string]struct{}, maxBatchSize*2)
		for index := 0; index < maxBatchSize*2; index++ {
			dataToPublish[fmt.Sprintf("Some data %d", index)] = struct{}{}
		}

		mockAPI.On("Publish", mock.Anything).Once().Return(assert.AnError)
		mockAPI.On("Publish", mock.Anything).Once().Return(nil)
		errored := make(chan string, maxBatchSize*2)
		succeeded := make(chan string, maxBatchSize*2)
		for item := range dataToPublish {
			go func(itemToPublish string) {
				err := batcher.Publish(itemToPublish)
				if err != nil {
					errored <- itemToPublish
				} else {
					succeeded <- itemToPublish
				}
			}(item)
		}
		dataDiscarded := make(map[string]struct{})
		dataPublished := make(map[string]struct{})
		for i := 0; i < maxBatchSize; i++ {
			dataDiscarded[<-errored] = struct{}{}
			dataPublished[<-succeeded] = struct{}{}
		}

		mockAPI.AssertNumberOfCalls(t, "Publish", 2)
		assert.Equal(t, dataDiscarded, mockAPI.dataDiscarded)
		assert.Equal(t, dataPublished, mockAPI.dataPublished)
	})

	t.Run("Test that batching by time works", func(t *testing.T) {
		const maxBatchSize = 4
		const aggregationPeriod = time.Millisecond * 100
		batcher, mockAPI := setupTestBatchPublisher(aggregationPeriod, maxBatchSize)
		defer batcher.Close()

		dataToPublish := make(map[string]struct{}, maxBatchSize*2)
		for i := 0; i < maxBatchSize*2; i++ {
			dataToPublish[fmt.Sprintf("Some data %d", i)] = struct{}{}
		}

		mockAPI.On("Publish", mock.Anything).Times(3).Return(nil)

		finish := make(chan bool, 2*maxBatchSize)
		counter := 0
		for item := range dataToPublish {
			go func(itemToPublish string) {
				err := batcher.Publish(itemToPublish)
				assert.NoError(t, err)
				finish <- true
			}(item)
			if counter == (maxBatchSize - 2) {
				time.Sleep(aggregationPeriod + time.Millisecond*100)
			}
			counter += 1
		}
		for i := 0; i < maxBatchSize*2; i++ {
			<-finish
		}
		mockAPI.AssertNumberOfCalls(t, "Publish", 3)
		assert.Equal(t, []int{maxBatchSize - 1, maxBatchSize, 1}, mockAPI.batchSizes)
	})

	t.Run("Test that slices publishing is propagated without waiting", func(t *testing.T) {
		batcher, mockAPI := setupTestBatchPublisher(time.Hour*24, 2)
		itemsToPublish := [][]string{
			{"batch one"},
			{"batch two"},
		}
		mockAPI.On("Publish", itemsToPublish[0]).Once().Return(nil)
		mockAPI.On("Publish", itemsToPublish[1]).Once().Return(nil)

		for _, items := range itemsToPublish {
			err := batcher.Publish(items)
			assert.NoError(t, err)
		}

		mockAPI.AssertNumberOfCalls(t, "Publish", 2)
	})
}

type mockPublishAPI struct {
	mock.Mock
	batchSizes    []int
	dataPublished map[string]struct{}
	dataDiscarded map[string]struct{}
}

func (m *mockPublishAPI) Publish(events interface{}) error {
	e := m.Called(events).Get(0)

	autoCollected, ok := events.([]interface{})
	if !ok {
		if e == nil {
			return nil
		}
		return e.(error)
	}
	m.batchSizes = append(m.batchSizes, len(autoCollected))

	if e == nil {
		for _, item := range autoCollected {
			m.dataPublished[item.(string)] = struct{}{}
		}
		return nil
	}
	for _, item := range autoCollected {
		m.dataDiscarded[item.(string)] = struct{}{}
	}
	return e.(error)
}

func setupTestBatchPublisher(batchCollectionTimeout time.Duration, maxBatchSize int) (*BatchPublisher, *mockPublishAPI) {
	api := &mockPublishAPI{
		batchSizes:    make([]int, 0),
		dataPublished: make(map[string]struct{}),
		dataDiscarded: make(map[string]struct{}),
	}
	result := BatchPublisher{
		publishAPI:             api,
		maxBatchSize:           maxBatchSize,
		batchCollectionTimeout: batchCollectionTimeout,
		eventsChannel:          make(chan *eventToPublish, 1000),
		dispatchFinished:       make(chan int),
	}
	go result.dispatchThread()
	return &result, api
}
