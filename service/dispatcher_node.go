package service

import (
	"context"
	"sync"
	"time"

	"github.com/totoview/xin/core"
	"go.uber.org/zap"
)

// DispatcherNode broadcasts messages to all subscribers.
type DispatcherNode struct {
	core.BasicPushProducer
	logger      *zap.Logger
	src         core.PullSourceNode // msg source
	maxRecv     int                 // max msgs to get from src.Recv each time
	recvTimeout time.Duration       // timeout for src.Recv
	RateControl *core.Muffler       // rate control
	stop        chan struct{}
	done        chan struct{}
}

// NewDispatcherNode constructs a new message dispatcher.
func NewDispatcherNode(config *core.ConsumerConfig, src core.PullSourceNode, recvTimeout time.Duration, logger *zap.Logger) (*DispatcherNode, error) {
	return &DispatcherNode{
		src:         src,
		maxRecv:     config.BatchSize,
		recvTimeout: recvTimeout,
		RateControl: core.NewMuffler(int64(config.MaxPending)),
		logger:      logger,
	}, nil
}

//////////////////////////////////////////////////////////////////////////////////////////
// core.Runnable

func (c *DispatcherNode) Init(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
}

func (c *DispatcherNode) Start(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	c.stop, c.done = make(chan struct{}), make(chan struct{})
	go func() {
		for {
			select {
			case <-c.stop:
				c.done <- struct{}{}
				return
			case <-c.RateControl.Ready:
			}
			if msgs := c.src.Recv(c.maxRecv, c.recvTimeout); msgs != nil && len(msgs) > 0 {
				c.RateControl.AddPending(int64(len(msgs)))
				for _, sub := range c.Subs {
					sub <- msgs
				}
			}
		}
	}()
}

func (c *DispatcherNode) Stop(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	c.stop <- struct{}{}
	<-c.done
}
