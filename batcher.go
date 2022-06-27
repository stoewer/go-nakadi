package nakadi

import (
	"reflect"
	"time"
)

// publishAPI defines interface that is used for publishing. Used because of unit tests
type publishAPI interface {
	Publish(events interface{}) error
}

// BatchPublishAPI allows publishing of events in a batched manner. The batcher collects single events into batches,
// respecting batch collection timeout and max batch size. Instead of creating many separate requests to nakadi it will
// aggregate single evewnts and publish them in batches.
type BatchPublishAPI struct {
	publishAPI             publishAPI
	batchCollectionTimeout time.Duration
	maxBatchSize           int
	eventsChannel          chan *eventToPublish
	dispatchFinished       chan int
}

// BatchOptions specifies parameters that should be used to collect events to batches
type BatchOptions struct {
	// Maximum amount of time that event will spend in intermediate queue before being published.
	BatchCollectionTimeout time.Duration
	// Maximum batch size - it is guaranteed that not more than MaxBatchSize events will be sent within one batch
	MaxBatchSize int
	// Size of the intermediate queue in which events are stored before being published.
	// If the queue is full, publishing call will be blocked, waiting for batch to be assembled
	BatchQueueSize int
}

func (o *BatchOptions) withDefaults() *BatchOptions {
	var copyOptions BatchOptions
	if o != nil {
		copyOptions = *o
	}
	if copyOptions.BatchCollectionTimeout == 0 {
		copyOptions.BatchCollectionTimeout = time.Second
	}
	if copyOptions.MaxBatchSize == 0 {
		copyOptions.MaxBatchSize = 10
	}
	if copyOptions.BatchQueueSize == 0 {
		copyOptions.BatchQueueSize = 1000
	}
	return &copyOptions
}

// NewBatchPublishAPI creates a proxy for batching from a client, publishOptions and batchOptions.
func NewBatchPublishAPI(
	client *Client,
	eventType string,
	publishOptions *PublishOptions,
	batchOptions *BatchOptions,
) *BatchPublishAPI {
	publishOptions = publishOptions.withDefaults()
	api := NewPublishAPI(client, eventType, publishOptions)

	batchOptions = batchOptions.withDefaults()
	result := BatchPublishAPI{
		publishAPI:             api,
		batchCollectionTimeout: batchOptions.BatchCollectionTimeout,
		maxBatchSize:           batchOptions.MaxBatchSize,
		eventsChannel:          make(chan *eventToPublish, batchOptions.BatchQueueSize),
		dispatchFinished:       make(chan int),
	}
	go result.dispatchThread()
	return &result
}

// Publish will publish requested data through PublishApi. In case if it is a single event (not a slice), it will be
// added to a batch and published as a part of a batch.
func (p *BatchPublishAPI) Publish(event interface{}) error {
	if reflect.TypeOf(event).Kind() == reflect.Slice {
		return p.publishAPI.Publish(event)
	}
	eventProxy := eventToPublish{
		requestedAt:   time.Now(),
		event:         event,
		publishResult: make(chan error, 1),
	}
	defer close(eventProxy.publishResult)

	p.eventsChannel <- &eventProxy
	return <-eventProxy.publishResult
}

type eventToPublish struct {
	requestedAt   time.Time
	event         interface{}
	publishResult chan error
}

// Close stops batching goroutine and waits for it to confirm stop process
func (p *BatchPublishAPI) Close() {
	close(p.eventsChannel)
	<-p.dispatchFinished
	close(p.dispatchFinished)
}

func (p *BatchPublishAPI) publishBatchToNakadi(events []*eventToPublish) {
	itemsToPublish := make([]interface{}, len(events))
	for idx, evt := range events {
		itemsToPublish[idx] = evt.event
	}
	err := p.publishAPI.Publish(itemsToPublish)
	for _, evt := range events {
		evt.publishResult <- err
	}
}

func (p *BatchPublishAPI) dispatchThread() {
	defer func() { p.dispatchFinished <- 1 }()
	batch := make([]*eventToPublish, 0, 1)
	var finishBatchCollectionAt *time.Time
	flush := func() {
		if len(batch) > 0 {
			p.publishBatchToNakadi(batch)
			batch = make([]*eventToPublish, 0, 1)
		}
		finishBatchCollectionAt = nil
	}
	for {
		if finishBatchCollectionAt == nil {
			event, ok := <-p.eventsChannel
			if !ok {
				break
			}
			batch = append(batch, event)
			finishAt := event.requestedAt.Add(p.batchCollectionTimeout)
			finishBatchCollectionAt = &finishAt
		} else {
			if len(batch) >= p.maxBatchSize || time.Now().After(*finishBatchCollectionAt) {
				flush()
			} else {
				select {
				case <-time.After(time.Until(*finishBatchCollectionAt)):
					flush()
				case evt, ok := <-p.eventsChannel:
					if ok {
						batch = append(batch, evt)
						break
					}
					flush()
				}
			}
		}
	}
}
