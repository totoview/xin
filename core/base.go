package core

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// Runnable provides basic lifecycle management for active components
// that run their own goroutines.
type Runnable interface {
	// Init prepares for running. Use ctx to check for timeout and cancellation.
	// Use errc to send error and wg to synchronize completion.
	Init(ctx context.Context, errc chan<- error, wg *sync.WaitGroup)

	// Start starts all internal goroutines. Use ctx to check for timeout and cancellation.
	// Use errc to send error and wg to synchronize completion.
	Start(ctx context.Context, errc chan<- error, wg *sync.WaitGroup)

	// Stop stops all internal goroutines. Use ctx to check for timeout and cancellation.
	// Use errc to send error and wg to synchronize completion.
	Stop(ctx context.Context, errc chan<- error, wg *sync.WaitGroup)
}

// PushProducer pushes requests actively into subscribers's channels
type PushProducer interface {
	// Subscribe creates a new subscription to receive requests on reqc
	Subscribe(reqc chan<- interface{}) error
}

// PullProducer produces requests passively by <i>Recv</i>.
type PullProducer interface {
	// Recv returns up to <i>max</i> requests
	Recv(max int, tmout time.Duration) []interface{}
}

// BasicPushProducer implements simple PushProducer
type BasicPushProducer struct {
	// list of subscriptions
	Subs []chan<- interface{}
}

// Subscribe adds reqc as new subscriber
func (p *BasicPushProducer) Subscribe(reqc chan<- interface{}) error {
	for _, c := range p.Subs {
		if c == reqc {
			return errors.Errorf("Existing subscriber")
		}
	}
	p.Subs = append(p.Subs, reqc)
	return nil
}

// Store persists messages.
type Store interface {
	// Save persists a slice of messages
	Save([]interface{}) error
}

// Service defines basic lifecycle methods.
type Service interface {
	// Init performs service initialization.
	Init(tmoutMillis int64) error
	// Start runs service.
	Start(tmoutMillis int64) error
	// Stop shuts down serivce.
	Stop(tmoutMillis int64) error
}
