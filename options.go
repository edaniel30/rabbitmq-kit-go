package rabbitmq

import "time"

// QueueConfig defines the structure for automatically creating queues and exchanges
type QueueConfig struct {
	Name        string
	Exchange    string
	RoutingKeys []string
}

type Options struct {
	URL            string
	ReconnectDelay time.Duration
	PrefetchCount  int
	Queues         []QueueConfig // New configuration list
}

type Option func(*Options)

func WithReconnectDelay(d time.Duration) Option {
	return func(o *Options) { o.ReconnectDelay = d }
}

func WithPrefetch(count int) Option {
	return func(o *Options) { o.PrefetchCount = count }
}

// WithQueues allows passing queue configuration from the main project
func WithQueues(queues []QueueConfig) Option {
	return func(o *Options) { o.Queues = queues }
}

func defaultOptions() Options {
	return Options{
		ReconnectDelay: 5 * time.Second,
		PrefetchCount:  10,
		Queues:         []QueueConfig{}, // Empty by default
	}
}
