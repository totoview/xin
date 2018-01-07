package core

import (
	"context"
	"sync"
)

// Node is the basic component of pipeline. Nodes run asynchronously and
// cooperate with each other through requests.
type Node interface {
	Runnable
}

// PullNode produces requests passively through <i>Recv</i>.
type PullNode interface {
	Node
	PullProducer
}

// PushNode produces requests actively through channels.
type PushNode interface {
	Node
	PushProducer
}

// SourceNode has no upstream nodes and is the starting point of a pipeline.
// It is responsible for keeping track of request state to avoid
// duplicate processing after restarts.
type SourceNode interface {
	Node
}

// PullSourceNode produces requests to downstream nodes passively. It is
// suitable for cases where the actual data source has the ability to buffer
// and persist incoming requests (such as Apache Kafka). Then PullSourceNode
// can be used to consume requests at a speed that naturally matches pipeline
// throughput without implementing additional throttling mechanism.
type PullSourceNode interface {
	Node
	PullProducer
}

// PushSourceNode produces requests actively. This could easily overload downstream
// nodes without proper throttling. Use this only if request management is not
// available upstream and must be implemented locally.
type PushSourceNode interface {
	Node
	PushProducer
}

// OffsetTrackingNode keeps track of request offsets. It implements the Runnable interface.
type OffsetTrackingNode struct {
	init     int64        // initial offset
	listener func(int64)  // invoked whenever offset is updated
	update   chan []int64 // offset update channel
	tracker  *OffsetTracker

	stop chan struct{}
	done sync.WaitGroup
}

// UpdateChannel returns n's offset update channel
func (n *OffsetTrackingNode) UpdateChannel() chan<- []int64 {
	return n.update
}

// NewOffsetTrackingNode creates a new OffsetTrackingNode instance with initial offset
// set to init and registeres listener as offset change callback.
func NewOffsetTrackingNode(init int64, listener func(int64)) *OffsetTrackingNode {
	return &OffsetTrackingNode{init: init, listener: listener, update: make(chan []int64)}
}

// Init initialises n
func (n *OffsetTrackingNode) Init(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	n.stop = make(chan struct{})
	n.tracker = NewOffsetTracker(n.init)
}

// Start runs n
func (n *OffsetTrackingNode) Start(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	n.done.Add(1)
	go func() {
		defer n.done.Done()
		for {
			select {
			case updates := <-n.update:
				prev := n.tracker.Offset()
				offset := n.tracker.AddAll(updates)
				if offset != prev && n.listener != nil {
					n.listener(offset)
				}
			case <-n.stop:
				return
			}
		}
	}()
}

// Stop stops n
func (n *OffsetTrackingNode) Stop(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	n.stop <- struct{}{}
	n.done.Wait()
}
