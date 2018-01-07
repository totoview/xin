package query

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/totoview/xin/core"
	"github.com/totoview/xin/kafka"
	pb "github.com/totoview/xin/pb/store"
	"go.uber.org/zap"
)

// Process events from message store.
type eventProcNode struct {
	logger *zap.Logger

	msgs         chan interface{}
	offsetUpdate chan<- []int64
	rateControl  *core.Muffler

	stop chan struct{}
	done chan struct{}
}

// NewEventProcNode constructs a new message store update Node.
func NewEventProcNode(msgSource core.PushProducer, offsetUpdate chan<- []int64,
	rateControl *core.Muffler, logger *zap.Logger) (core.Node, error) {
	var err error
	un := &eventProcNode{
		logger:       logger,
		offsetUpdate: offsetUpdate,
		msgs:         make(chan interface{}),
		rateControl:  rateControl,
	}
	if err = msgSource.Subscribe(un.msgs); err != nil {
		return nil, err
	}
	return un, err
}

//////////////////////////////////////////////////////////////////////////////////////////////
// core.Runnable

func (un *eventProcNode) Init(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
}

func (un *eventProcNode) update(msgs []interface{}) {
	var (
		cnt     = len(msgs)
		offsets = make([]int64, cnt)
	)

	for i, m := range msgs {
		msg := m.(*kafka.IncomingMsg)
		offsets[i] = msg.Offset

		event := msg.Data.(*pb.StoreEvent)
		switch event.Type {
		case pb.StoreEvent_MsgCreated:
			if ce := un.logger.Check(zap.DebugLevel, "New event"); ce != nil {
				data, _ := json.Marshal(event.MsgCreated)
				ce.Write(zap.String("event", string(data)))
			}
		}
	}

	un.offsetUpdate <- offsets
	un.rateControl.RemovePending(int64(cnt))
}

func (un *eventProcNode) Start(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	un.stop, un.done = make(chan struct{}), make(chan struct{})

	// receive messages from source
	go func() {
		for {
			select {
			case <-un.stop:
				un.done <- struct{}{}
				return
			case msgs := <-un.msgs:
				// will this block ? do we need a goroutine for this ?
				un.update(msgs.([]interface{}))
			}
		}
	}()
}

func (un *eventProcNode) Stop(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	close(un.stop)
	<-un.done
}
