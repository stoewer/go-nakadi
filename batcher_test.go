package nakadi

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type mockPublishApi struct {
	responses []error
	requests  [][]interface{}
}

func (mock *mockPublishApi) Publish(events interface{}) error {
	switch events.(type) {
	case []interface{}:
		mock.requests = append(mock.requests, events.([]interface{}))
	default:
		panic("only slices are expected")
	}
	resp := mock.responses[0]
	mock.responses = mock.responses[1:]
	return resp
}

func newMockPublishApi(responses []error) *mockPublishApi {
	return &mockPublishApi{
		responses: responses,
		requests:  make([][]interface{}, 0),
	}
}

// Check that publishing batcher is getting closed after usage
func TestPublishingBatcher_Close(t *testing.T) {
	t.Run("Test closing", func(t *testing.T) {
		batcher := NewPublishingBatcher(nil, time.Second, 100)
		batcher.Close()
	})
}

func TestPublishingBatcher_Publish(t *testing.T) {
	t.Run("Test that batching by max batch size is working", func(t *testing.T) {
		const maxBatchSize = 4
		mockApi := newMockPublishApi([]error{nil, nil})
		batcher := NewPublishingBatcher(mockApi, 24*time.Hour, maxBatchSize)
		defer batcher.Close()

		finish := make(chan bool, 2*maxBatchSize)
		for i := 0; i < 2*maxBatchSize; i++ {
			go func() {
				err := batcher.Publish("some data")
				assert.NoError(t, err)
				finish <- true
			}()
		}
		for i := 0; i < maxBatchSize*2; i++ {
			<-finish
		}

		assert.Equal(t, 2, len(mockApi.requests))
		assert.Equal(t, maxBatchSize, len(mockApi.requests[0]))
		assert.Equal(t, maxBatchSize, len(mockApi.requests[1]))
	})
	t.Run("Test that error is propagated to calling functions", func(t *testing.T) {
		const maxBatchSize = 4
		mockApi := newMockPublishApi([]error{fmt.Errorf("YAYA"), nil})
		batcher := NewPublishingBatcher(mockApi, 24*time.Hour, maxBatchSize)
		defer batcher.Close()

		finish := make(chan bool, 2*maxBatchSize)
		for i := 0; i < 2*maxBatchSize; i++ {
			go func(idx int) {
				err := batcher.Publish("Some data")
				if idx < maxBatchSize {
					assert.Error(t, err, "Expect to have error on iteration %v", idx)
				} else {
					assert.NoError(t, err, "Expect not no have error on iteration %v", idx)
				}
				finish <- true
			}(i)
			time.Sleep(time.Millisecond * 100)
		}
		for i := 0; i < maxBatchSize*2; i++ {
			<-finish
		}

		assert.Equal(t, 2, len(mockApi.requests))
		assert.Equal(t, maxBatchSize, len(mockApi.requests[0]))
		assert.Equal(t, maxBatchSize, len(mockApi.requests[1]))
	})

	t.Run("Test that batching by time works", func(t *testing.T) {
		const maxBatchSize = 4
		const aggregationPeriod = time.Millisecond * 100
		mockApi := newMockPublishApi([]error{nil, nil, nil})
		batcher := NewPublishingBatcher(mockApi, aggregationPeriod, maxBatchSize)
		defer batcher.Close()

		finish := make(chan bool, 2*maxBatchSize)
		for i := 0; i < maxBatchSize*2; i++ {
			go func(idx int) {
				err := batcher.Publish("Some data")
				assert.NoError(t, err)
				finish <- true
			}(i)
			if i == (maxBatchSize - 2) {
				time.Sleep(aggregationPeriod + time.Millisecond*100)
			}
		}
		for i := 0; i < maxBatchSize*2; i++ {
			<-finish
		}
		assert.Equal(t, 3, len(mockApi.requests))
		assert.Equal(t, maxBatchSize-1, len(mockApi.requests[0]))
		assert.Equal(t, maxBatchSize, len(mockApi.requests[1]))
		assert.Equal(t, 1, len(mockApi.requests[2]))
	})

	t.Run("Test that actual data is passed to publish api", func(t *testing.T) {
		mockApi := newMockPublishApi([]error{nil, nil, nil})
		batcher := NewPublishingBatcher(mockApi, time.Hour, 2)
		defer batcher.Close()

		finish := make(chan bool, 2)
		for i := 0; i < 2; i++ {
			go func(idx int) {
				err := batcher.Publish(fmt.Sprintf("Some data %v", i))
				assert.NoError(t, err)
				finish <- true
			}(i)
			time.Sleep(100 * time.Millisecond)
		}
		assert.Equal(t, 1, len(mockApi.requests))
		assert.Equal(t, 2, len(mockApi.requests[0]))
		assert.Equal(t, "Some data 0", mockApi.requests[0][0])
		assert.Equal(t, "Some data 1", mockApi.requests[0][1])
	})
}
