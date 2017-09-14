package nakadi

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type ProcessorOptions struct {
	BatchLimit           int
	StreamCount          int
	InitialRetryInterval time.Duration
	MaxRetryInterval     time.Duration
	CommitMaxElapsedTime time.Duration
	NotifyErr            func(int, error, time.Duration)
	NotifyOK             func(int)
}

func (o *ProcessorOptions) withDefaults() *ProcessorOptions {
	var copyOptions ProcessorOptions
	if o != nil {
		copyOptions = *o
	}
	if copyOptions.StreamCount == 0 {
		copyOptions.StreamCount = 1
	}
	if copyOptions.InitialRetryInterval == 0 {
		copyOptions.InitialRetryInterval = defaultInitialRetryInterval
	}
	if copyOptions.MaxRetryInterval == 0 {
		copyOptions.MaxRetryInterval = defaultMaxRetryInterval
	}
	if copyOptions.CommitMaxElapsedTime == 0 {
		copyOptions.CommitMaxElapsedTime = defaultMaxElapsedTime
	}
	if copyOptions.NotifyErr == nil {
		copyOptions.NotifyErr = func(_ int, _ error, _ time.Duration) {}
	}
	if copyOptions.NotifyOK == nil {
		copyOptions.NotifyOK = func(_ int) {}
	}
	return &copyOptions
}

// streamAPI is a contract that is used internally in order to be able to mock StreamAPI
type streamAPI interface {
	NextEvents() (Cursor, []byte, error)
	CommitCursor(cursor Cursor) error
	Close() error
}

func NewProcessor(client *Client, subscriptionID string, options *ProcessorOptions) *Processor {
	options = options.withDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	processor := &Processor{
		client:         client,
		subscriptionID: subscriptionID,
		ctx:            ctx,
		cancel:         cancel,
		newStream: func(client *Client, id string, options *StreamOptions) streamAPI {
			return NewStream(client, id, options)
		}}

	for i := 0; i < options.StreamCount; i++ {
		streamOptions := StreamOptions{
			BatchLimit:           options.BatchLimit,
			InitialRetryInterval: options.InitialRetryInterval,
			MaxRetryInterval:     options.MaxRetryInterval,
			CommitMaxElapsedTime: options.CommitMaxElapsedTime,
			NotifyErr:            func(err error, duration time.Duration) { options.NotifyErr(i, err, duration) },
			NotifyOK:             func() { options.NotifyOK(i) }}

		processor.streamOptions = append(processor.streamOptions, streamOptions)
	}

	return processor
}

type Processor struct {
	sync.Mutex
	client         *Client
	subscriptionID string
	streamOptions  []StreamOptions
	newStream      func(*Client, string, *StreamOptions) streamAPI
	streams        []streamAPI
	ctx            context.Context
	cancel         context.CancelFunc
}

func (p *Processor) Start(callback func(int, []byte) error) error {
	p.Lock()
	defer p.Unlock()

	if len(p.streams) > 0 {
		return errors.New("processor was already started")
	}

	for i, options := range p.streamOptions {
		stream := p.newStream(p.client, p.subscriptionID, &options)
		p.streams = append(p.streams, stream)

		go func() {
			for {
				select {
				case <-p.ctx.Done():
					return
				default:
					cursor, events, err := stream.NextEvents()
					if err != nil {
						continue
					}

					err = callback(i, events)
					if err != nil {
						p.Lock()
						p.streams[i].Close()
						p.streams[i] = p.newStream(p.client, p.subscriptionID, &options)
						p.Unlock()
						continue
					}

					stream.CommitCursor(cursor)
				}
			}
		}()
	}

	return nil
}

func (p *Processor) Stop() error {
	p.Lock()
	defer p.Unlock()

	if len(p.streams) == 0 {
		return errors.New("processor is not running")
	}

	p.cancel()

	var allErrors []error
	for _, s := range p.streams {
		err := s.Close()
		if err != nil {
			allErrors = append(allErrors, err)
		}
	}

	p.streams = nil

	if len(allErrors) > 0 {
		return errors.Errorf("%d streams had errors while closing the stream", len(allErrors))
	}

	return nil
}
