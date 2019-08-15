package nakadi

import (
	"reflect"
	"time"
)

// IPublishAPI defines interface that is used for publishing. Used mostly because of unit tests
type IPublishAPI interface {
	Publish(events interface{}) error
}

// PublishingBatcher is a proxy that allows publishing events in a batched manner. In case if events are published in
// parallel (not in a form of slices, but in a form of events), then batcher will collect them into batches, respecting
// batch collection timeout and max batch size and instead of creating many separate requests to nakadi it will create
// only several of them with aggregated data.
type PublishingBatcher struct {
	PublishAPI             IPublishAPI
	BatchCollectionTimeout time.Duration
	MaxBatchSize           int
	EventsChannel          chan *eventToPublish
	DispatchFinished       chan int
}

// NewPublishingBatcher creates a proxy for batching based on api and batching parameters
func NewPublishingBatcher(api IPublishAPI, batchCollectionTimeout time.Duration, maxBatchSize int) *PublishingBatcher {
	result := PublishingBatcher{
		PublishAPI:             api,
		BatchCollectionTimeout: batchCollectionTimeout,
		MaxBatchSize:           maxBatchSize,
		EventsChannel:          make(chan *eventToPublish, 1000),
		DispatchFinished:       make(chan int),
	}
	go result.dispatchThread()
	return &result
}

// Publish will publish requested data through PublishApi. In case if it is a single event (not a slice), it will be
// added to a batch and published as a part of a batch.
func (p *PublishingBatcher) Publish(event interface{}) error {
	if reflect.TypeOf(event).Kind() == reflect.Slice {
		return p.PublishAPI.Publish(event)
	}
	eventProxy := eventToPublish{
		requestedAt:   time.Now(),
		event:         event,
		publishResult: make(chan error, 1),
	}
	defer close(eventProxy.publishResult)

	p.EventsChannel <- &eventProxy
	return <-eventProxy.publishResult
}

type eventToPublish struct {
	requestedAt   time.Time
	event         interface{}
	publishResult chan error
}

// Close stops batching goroutine and waits for it to confirm stop process
func (p *PublishingBatcher) Close() {
	close(p.EventsChannel)
	<-p.DispatchFinished
	close(p.DispatchFinished)
}

func (p *PublishingBatcher) publishBatchToNakadi(events []*eventToPublish) {
	itemsToPublish := make([]interface{}, len(events))
	for idx, evt := range events {
		itemsToPublish[idx] = evt.event
	}
	err := p.PublishAPI.Publish(itemsToPublish)
	for _, evt := range events {
		evt.publishResult <- err
	}
}

func (p *PublishingBatcher) dispatchThread() {
	defer func() { p.DispatchFinished <- 1 }()
	batch := make([]*eventToPublish, 0, 1)
	var finishBatchCollectionAt *time.Time
	for {
		if finishBatchCollectionAt == nil {
			event, ok := <-p.EventsChannel
			if !ok {
				break
			}
			batch = append(batch, event)
			finishAt := event.requestedAt.Add(p.BatchCollectionTimeout)
			finishBatchCollectionAt = &finishAt
		} else {
			flush := false
			if len(batch) >= p.MaxBatchSize {
				flush = true
			} else {
				select {
				case <-time.After(finishBatchCollectionAt.Sub(time.Now())):
					flush = true
					break
				case evt, ok := <-p.EventsChannel:
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
