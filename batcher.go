package nakadi

import (
	"reflect"
	"time"
)

// publishAPI defines interface that is used for publishing. Used because of unit tests
type publishAPI interface {
	Publish(events interface{}) error
}

// BatchPublisher is a proxy that allows publishing events in a batched manner. In case if events are published in
// parallel (not in a form of slices, but in a form of events), then batcher will collect them into batches, respecting
// batch collection timeout and max batch size and instead of creating many separate requests to nakadi it will create
// only several of them with aggregated data.
type BatchPublisher struct {
	publishAPI       publishAPI
	options          BatchPublisherOptions
	eventsChannel    chan *eventToPublish
	dispatchFinished chan int
}

// BatchPublisherOptions specifies parameters that should be used to collect events to batches
type BatchPublisherOptions struct {
	// Maximum amount of time that event will spend in intermediate queue before being published.
	BatchCollectionTimeout time.Duration
	// Maximum batch size - it is guaranteed that not more than MaxBatchSize events will be sent within one batch
	MaxBatchSize int
}

// NewPublishingBatcher creates a proxy for batching based on api and batching parameters
func NewPublishingBatcher(api *PublishAPI, options BatchPublisherOptions) *BatchPublisher {
	result := BatchPublisher{
		publishAPI:       api,
		options:          options,
		eventsChannel:    make(chan *eventToPublish, 1000),
		dispatchFinished: make(chan int),
	}
	go result.dispatchThread()
	return &result
}

// Publish will publish requested data through PublishApi. In case if it is a single event (not a slice), it will be
// added to a batch and published as a part of a batch.
func (p *BatchPublisher) Publish(event interface{}) error {
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
func (p *BatchPublisher) Close() {
	close(p.eventsChannel)
	<-p.dispatchFinished
	close(p.dispatchFinished)
}

func (p *BatchPublisher) publishBatchToNakadi(events []*eventToPublish) {
	itemsToPublish := make([]interface{}, len(events))
	for idx, evt := range events {
		itemsToPublish[idx] = evt.event
	}
	err := p.publishAPI.Publish(itemsToPublish)
	for _, evt := range events {
		evt.publishResult <- err
	}
}

func (p *BatchPublisher) dispatchThread() {
	defer func() { p.dispatchFinished <- 1 }()
	batch := make([]*eventToPublish, 0, 1)
	var finishBatchCollectionAt *time.Time
	for {
		if finishBatchCollectionAt == nil {
			event, ok := <-p.eventsChannel
			if !ok {
				break
			}
			batch = append(batch, event)
			finishAt := event.requestedAt.Add(p.options.BatchCollectionTimeout)
			finishBatchCollectionAt = &finishAt
		} else {
			flush := false
			if len(batch) >= p.options.MaxBatchSize || time.Now().After(*finishBatchCollectionAt) {
				flush = true
			} else {
				select {
				case <-time.After(time.Until(*finishBatchCollectionAt)):
					flush = true
					break
				case evt, ok := <-p.eventsChannel:
					if !ok {
						flush = true
					} else {
						batch = append(batch, evt)
					}
					break
				}
			}
			if flush {
				go p.publishBatchToNakadi(batch)
				batch = make([]*eventToPublish, 0, 1)
				finishBatchCollectionAt = nil
			}
		}
	}
}
