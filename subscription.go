package nakadi

import "context"

type SubscriptionAPI interface {
	NextEvents(ctx context.Context) (*Cursor, []byte, error)
	CommitCursor(cursor *Cursor) error
	Close() error
}

type SubscriptionOptions struct {
	ConsumerGroup   string
	BatchLimit      int
	ReadParallelism int
}

func NewSubscription(client *Client, application string, eventType string, options *SubscriptionOptions) (*Subscription, error) {
	return nil, nil
}
