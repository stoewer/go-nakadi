package nakadi

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// ProcessorOptions contains optional parameters that are used to create a Processor.
type ProcessorOptions struct {
	// The maximum number of Events in each chunk (and therefore per partition) of the stream (default: 1)
	BatchLimit uint
	// The number of parallel streams the Processor will use to consume events (default: 1)
	StreamCount uint
	// Limits the number of events that the processor will handle per minute. This value represents an
	// upper bound, if some streams are not healthy e.g. if StreamCount exceeds the number of partitions,
	// or the actual batch size is lower than BatchLimit the actual number of processed events can be
	// much lower. 0 is interpreted as no limit at all (default: no limit)
	EventsPerMinute uint
	// The initial (minimal) retry interval used for the exponential backoff. This value is applied for
	// stream initialization as well as for cursor commits.
	InitialRetryInterval time.Duration
	// MaxRetryInterval the maximum retry interval. Once the exponential backoff reaches this value
	// the retry intervals remain constant. This value is applied for stream initialization as well as
	// for cursor commits.
	MaxRetryInterval time.Duration
	// CommitMaxElapsedTime is the maximum time spent on retries when committing a cursor. Once this value
	// was reached the exponential backoff is halted and the cursor will not be committed.
	CommitMaxElapsedTime time.Duration
	// NotifyErr is called when an error occurs that leads to a retry. This notify function can be used to
	// detect unhealthy streams. The first parameter indicates the stream No that encountered the error.
	NotifyErr func(uint, error, time.Duration)
	// NotifyOK is called whenever a successful operation was completed. This notify function can be used
	// to detect that a stream is healthy again. The first parameter indicates the stream No that just
	// regained health.
	NotifyOK func(uint)
}

func (o *ProcessorOptions) withDefaults() *ProcessorOptions {
	var copyOptions ProcessorOptions
	if o != nil {
		copyOptions = *o
	}
	if copyOptions.BatchLimit == 0 {
		copyOptions.BatchLimit = 1
	}
	if copyOptions.StreamCount == 0 {
		copyOptions.StreamCount = 1
	}
	if copyOptions.EventsPerMinute == 0 {
		copyOptions.EventsPerMinute = 0
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
		copyOptions.NotifyErr = func(_ uint, _ error, _ time.Duration) {}
	}
	if copyOptions.NotifyOK == nil {
		copyOptions.NotifyOK = func(_ uint) {}
	}
	return &copyOptions
}

// streamAPI is a contract that is used internally in order to be able to mock StreamAPI
type streamAPI interface {
	NextEvents() (Cursor, []byte, error)
	CommitCursor(cursor Cursor) error
	Close() error
}

// NewProcessor creates a new processor for a given subscription ID.  The constructor receives a
// configured Nakadi client as first parameter. Furthermore a valid subscription ID must be
// provided. The last parameter is a struct containing only optional parameters. The options may be
// nil, in this case the processor falls back to the defaults defined in the ProcessorOptions.
func NewProcessor(client *Client, subscriptionID string, options *ProcessorOptions) *Processor {
	options = options.withDefaults()

	var timePerBatchPerStream int64
	if options.EventsPerMinute > 0 {
		timePerBatchPerStream = int64(time.Minute) / int64(options.EventsPerMinute) * int64(options.StreamCount) * int64(options.BatchLimit)
	}

	ctx, cancel := context.WithCancel(context.Background())
	processor := &Processor{
		client:         client,
		subscriptionID: subscriptionID,
		ctx:            ctx,
		cancel:         cancel,
		newStream: func(client *Client, id string, options *StreamOptions) streamAPI {
			return NewStream(client, id, options)
		},
		timePerBatchPerStream: time.Duration(timePerBatchPerStream)}

	for i := uint(0); i < options.StreamCount; i++ {
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

// A Processor for nakadi events. The Processor is a high level API for consuming events from
// Nakadi. It can process event batches from multiple partitions (streams) and can be configured
// with a certain event rate, that limits the number of processed events per minute. The cursors of
// event batches that were successfully processed are automatically committed.
type Processor struct {
	sync.Mutex
	client                *Client
	subscriptionID        string
	streamOptions         []StreamOptions
	newStream             func(*Client, string, *StreamOptions) streamAPI
	timePerBatchPerStream time.Duration
	streams               []streamAPI
	ctx                   context.Context
	cancel                context.CancelFunc
}

// Start begins event processing. All event batches received from the underlying streams are passed to
// the operation function. If the operation function terminates without error the respective cursor will
// be automatically committed to Nakadi. If the operations terminates with an error, the underlying stream
// will be halted and a new stream will continue to pass event batches to the operation function.
//
// Event processing will go on indefinitely unless the processor is stopped via its Stop method. Star will
// return an error if the processor is already running.
func (p *Processor) Start(operation func(int, []byte) error) error {
	p.Lock()
	defer p.Unlock()

	if len(p.streams) > 0 {
		return errors.New("processor was already started")
	}

	p.streams = make([]streamAPI, len(p.streamOptions))

	for streamNo, options := range p.streamOptions {
		go p.startSingleStream(operation, streamNo, options)
	}

	return nil
}

// startSingleStream starts a single stream with a given stream number / position. After the stream has been
// started it consumes events. In cases of errors the stream is closed and a new stream will be opened.
func (p *Processor) startSingleStream(operation func(int, []byte) error, streamNo int, options StreamOptions) {
	p.streams[streamNo] = p.newStream(p.client, p.subscriptionID, &options)

	if p.timePerBatchPerStream > 0 {
		initialWait := rand.Int63n(int64(p.timePerBatchPerStream))
		select {
		case <-p.ctx.Done():
			return
		case <-time.After(time.Duration(initialWait)):
			// nothing
		}
	}

	for {
		start := time.Now()

		select {
		case <-p.ctx.Done():
			return
		default:
			cursor, events, err := p.streams[streamNo].NextEvents()
			if err != nil {
				continue
			}

			err = operation(streamNo, events)
			if err != nil {
				p.Lock()
				p.streams[streamNo].Close()
				p.streams[streamNo] = p.newStream(p.client, p.subscriptionID, &options)
				p.Unlock()
				continue
			}

			p.streams[streamNo].CommitCursor(cursor)
		}

		elapsed := time.Since(start)
		if elapsed < p.timePerBatchPerStream {
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(p.timePerBatchPerStream - elapsed):
				// nothing
			}
		}
	}
}

// Stop halts all steams and terminates event processing. Stop will return with an error if the processor
// is not running.
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
