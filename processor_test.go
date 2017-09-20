package nakadi

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	testSubscriptionID = "9eacd55a-9d6c-11e7-86f0-53366f10d8d4"
	testClient         = New(defaultNakadiURL, nil)
	testStreamOptions  = &StreamOptions{}
)

func TestProcessorOptions_withDefaults(t *testing.T) {
	tests := []struct {
		Options  *ProcessorOptions
		Expected *ProcessorOptions
	}{
		{
			Options: nil,
			Expected: &ProcessorOptions{
				BatchLimit:           1,
				StreamCount:          1,
				EventsPerMinute:      0,
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				CommitMaxElapsedTime: defaultMaxElapsedTime,
			},
		},
		{
			Options: &ProcessorOptions{BatchLimit: 15},
			Expected: &ProcessorOptions{
				BatchLimit:           15,
				StreamCount:          1,
				EventsPerMinute:      0,
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				CommitMaxElapsedTime: defaultMaxElapsedTime,
			},
		},
		{
			Options: &ProcessorOptions{StreamCount: 4},
			Expected: &ProcessorOptions{
				BatchLimit:           1,
				StreamCount:          4,
				EventsPerMinute:      0,
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				CommitMaxElapsedTime: defaultMaxElapsedTime,
			},
		},
		{
			Options: &ProcessorOptions{EventsPerMinute: 10},
			Expected: &ProcessorOptions{
				BatchLimit:           1,
				StreamCount:          1,
				EventsPerMinute:      10,
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				CommitMaxElapsedTime: defaultMaxElapsedTime,
			},
		},
		{
			Options: &ProcessorOptions{InitialRetryInterval: 123 * time.Millisecond},
			Expected: &ProcessorOptions{
				BatchLimit:           1,
				StreamCount:          1,
				EventsPerMinute:      0,
				InitialRetryInterval: 123 * time.Millisecond,
				MaxRetryInterval:     defaultMaxRetryInterval,
				CommitMaxElapsedTime: defaultMaxElapsedTime,
			},
		},
		{
			Options: &ProcessorOptions{MaxRetryInterval: 123 * time.Second},
			Expected: &ProcessorOptions{
				BatchLimit:           1,
				StreamCount:          1,
				EventsPerMinute:      0,
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     123 * time.Second,
				CommitMaxElapsedTime: defaultMaxElapsedTime,
			},
		},
		{
			Options: &ProcessorOptions{CommitMaxElapsedTime: 123 * time.Second},
			Expected: &ProcessorOptions{
				BatchLimit:           1,
				StreamCount:          1,
				EventsPerMinute:      0,
				InitialRetryInterval: defaultInitialRetryInterval,
				MaxRetryInterval:     defaultMaxRetryInterval,
				CommitMaxElapsedTime: 123 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		withDefaults := tt.Options.withDefaults()
		assert.Equal(t, tt.Expected.BatchLimit, withDefaults.BatchLimit)
		assert.Equal(t, tt.Expected.StreamCount, withDefaults.StreamCount)
		assert.Equal(t, tt.Expected.EventsPerMinute, withDefaults.EventsPerMinute)
		assert.Equal(t, tt.Expected.InitialRetryInterval, withDefaults.InitialRetryInterval)
		assert.Equal(t, tt.Expected.MaxRetryInterval, withDefaults.MaxRetryInterval)
		assert.Equal(t, tt.Expected.CommitMaxElapsedTime, withDefaults.CommitMaxElapsedTime)
		assert.NotNil(t, withDefaults.NotifyErr)
		assert.NotNil(t, withDefaults.NotifyOK)
	}
}

func TestNewProcessor(t *testing.T) {
	client := New(defaultNakadiURL, nil)
	processor := NewProcessor(client, testSubscriptionID, &ProcessorOptions{EventsPerMinute: 6})

	require.NotNil(t, processor)
	assert.Equal(t, client, processor.client)
	assert.Equal(t, testSubscriptionID, processor.subscriptionID)
	assert.Len(t, processor.streamOptions, 1)
	assert.Len(t, processor.streams, 0)
	assert.Equal(t, 10*time.Second, processor.timePerBatchPerStream)
	assert.NotNil(t, processor.ctx)
	assert.NotNil(t, processor.cancel)
}

func TestProcessor_Start(t *testing.T) {
	t.Run("fail running stream", func(t *testing.T) {
		_, s, processor := setupMockProcessor()
		processor.streams = []streamAPI{s}

		err := processor.Start(func(i int, batch []byte) error { return nil })
		require.Error(t, err)
		assert.Regexp(t, "processor was already started", err)
	})

	t.Run("fail reading batch", func(t *testing.T) {
		newStream, streamAPI, processor := setupMockProcessor()

		newStream.On("NewStream", testClient, testSubscriptionID, testStreamOptions).
			Return(streamAPI)
		streamAPI.On("NextEvents").
			Return(Cursor{}, nil, assert.AnError)

		processor.Start(func(i int, batch []byte) error {
			assert.Fail(t, "operator should not be called")
			return nil
		})

		<-newStream.wait
		newStream.AssertCalled(t, "NewStream", testClient, testSubscriptionID, testStreamOptions)

		<-streamAPI.wait
		streamAPI.AssertCalled(t, "NextEvents")
	})

	t.Run("fail during operation", func(t *testing.T) {
		batchCh := make(chan []byte, 1)
		newStream, streamAPI, processor := setupMockProcessor()

		newStream.On("NewStream", testClient, testSubscriptionID, testStreamOptions).
			Return(streamAPI)
		streamAPI.On("Close").
			Return(nil)
		streamAPI.On("NextEvents").
			Return(Cursor{}, []byte("batch no 1"), nil)

		processor.Start(func(i int, batch []byte) error {
			batchCh <- batch
			return assert.AnError
		})

		<-newStream.wait
		newStream.AssertCalled(t, "NewStream", testClient, testSubscriptionID, testStreamOptions)

		<-streamAPI.wait
		streamAPI.AssertCalled(t, "NextEvents")

		batch := <-batchCh
		require.Equal(t, []byte("batch no 1"), batch)

		<-streamAPI.wait
		streamAPI.AssertCalled(t, "Close")

		<-newStream.wait
		newStream.AssertCalled(t, "NewStream", testClient, testSubscriptionID, testStreamOptions)
	})

	t.Run("successful processing", func(t *testing.T) {
		batchCh := make(chan []byte, 1)
		newStream, streamAPI, processor := setupMockProcessor()

		newStream.On("NewStream", testClient, testSubscriptionID, testStreamOptions).
			Return(streamAPI)
		streamAPI.On("CommitCursor", Cursor{}).
			Return(nil)
		streamAPI.On("NextEvents").
			Return(Cursor{}, []byte("batch no 1"), nil)

		processor.Start(func(i int, batch []byte) error {
			batchCh <- batch
			return nil
		})

		<-newStream.wait
		newStream.AssertCalled(t, "NewStream", testClient, testSubscriptionID, testStreamOptions)

		<-streamAPI.wait
		streamAPI.AssertCalled(t, "NextEvents")

		batch := <-batchCh
		require.Equal(t, []byte("batch no 1"), batch)

		<-streamAPI.wait
		streamAPI.AssertCalled(t, "CommitCursor", Cursor{})

		<-streamAPI.wait
		streamAPI.AssertCalled(t, "NextEvents")

		batch = <-batchCh
		require.Equal(t, []byte("batch no 1"), batch)

		<-streamAPI.wait
		streamAPI.AssertCalled(t, "CommitCursor", Cursor{})
	})
}

func TestProcessor_Stop(t *testing.T) {
	t.Run("fail not running", func(t *testing.T) {
		_, _, processor := setupMockProcessor()

		err := processor.Stop()
		require.Error(t, err)
		assert.Regexp(t, "processor is not running", err)
	})

	t.Run("close with error", func(t *testing.T) {
		newStream, streamAPI, processor := setupMockProcessor()

		newStream.On("NewStream", testClient, testSubscriptionID, testStreamOptions).
			Return(streamAPI)
		streamAPI.On("NextEvents").
			Return(Cursor{}, []byte("batch no 1"), nil)
		streamAPI.On("CommitCursor", Cursor{}).
			Return(nil)
		streamAPI.On("Close").
			Return(assert.AnError)

		processor.Start(func(i int, batch []byte) error {
			return nil
		})

		<-newStream.wait
		newStream.AssertCalled(t, "NewStream", testClient, testSubscriptionID, testStreamOptions)

		processor.Stop()

		<-streamAPI.waitClose
		streamAPI.AssertCalled(t, "Close")
	})

	t.Run("success", func(t *testing.T) {
		newStream, streamAPI, processor := setupMockProcessor()

		newStream.On("NewStream", testClient, testSubscriptionID, testStreamOptions).
			Return(streamAPI)
		streamAPI.On("NextEvents").
			Return(Cursor{}, []byte("batch no 1"), nil)
		streamAPI.On("CommitCursor", Cursor{}).
			Return(nil)
		streamAPI.On("Close").
			Return(nil)

		processor.Start(func(i int, batch []byte) error {
			return nil
		})

		<-newStream.wait
		newStream.AssertCalled(t, "NewStream", testClient, testSubscriptionID, testStreamOptions)

		<-streamAPI.wait
		processor.Stop()

		<-streamAPI.waitClose
		streamAPI.AssertCalled(t, "Close")
	})
}

func setupMockProcessor() (*mockNewStream, *mockStreamAPI, *Processor) {
	mockNewStream := &mockNewStream{wait: make(chan struct{}, 1)}
	mockStreamAPI := &mockStreamAPI{wait: make(chan struct{}, 1), waitClose: make(chan struct{}, 1)}

	ctx, cancel := context.WithCancel(context.Background())
	processor := &Processor{
		client:                testClient,
		subscriptionID:        testSubscriptionID,
		ctx:                   ctx,
		cancel:                cancel,
		newStream:             mockNewStream.NewStream,
		timePerBatchPerStream: 100 * time.Millisecond,
		streamOptions:         []StreamOptions{*testStreamOptions}}

	return mockNewStream, mockStreamAPI, processor
}

type mockNewStream struct {
	mock.Mock
	wait chan struct{}
}

func (m *mockNewStream) NewStream(client *Client, subscriptionID string, options *StreamOptions) streamAPI {
	args := m.Called(client, subscriptionID, options)
	m.wait <- struct{}{}
	return args.Get(0).(streamAPI)
}

type mockStreamAPI struct {
	mock.Mock
	wait      chan struct{}
	waitClose chan struct{}
}

func (m *mockStreamAPI) NextEvents() (Cursor, []byte, error) {
	args := m.Called()
	m.wait <- struct{}{}
	if args.Error(2) != nil {
		return Cursor{}, nil, args.Error(2)
	}
	return args.Get(0).(Cursor), args.Get(1).([]byte), nil
}

func (m *mockStreamAPI) CommitCursor(cursor Cursor) error {
	args := m.Called(cursor)
	m.wait <- struct{}{}
	return args.Error(0)
}

func (m *mockStreamAPI) Close() error {
	args := m.Called()
	m.waitClose <- struct{}{}
	return args.Error(0)
}
