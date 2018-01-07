package mgw

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/totoview/xin/core"
	"github.com/totoview/xin/kafka"
	base "github.com/totoview/xin/pb"
	pb "github.com/totoview/xin/pb/mgw"
	"go.uber.org/zap"
)

// Generate updates for message store.
type storeUpdateNode struct {
	logger *zap.Logger

	msgs         chan interface{}
	updateTopic  string
	updates      chan<- []*kafka.ProducerMsg
	offsetUpdate chan<- []int64
	rateControl  *core.Muffler

	stop chan struct{}
	done chan struct{}
}

// NewStoreUpdateNode constructs a new message store update Node.
func NewStoreUpdateNode(msgSource core.PushProducer, updateTopic string, updates chan<- []*kafka.ProducerMsg, offsetUpdate chan<- []int64,
	rateControl *core.Muffler, logger *zap.Logger) (core.Node, error) {
	var err error
	un := &storeUpdateNode{
		logger:       logger,
		offsetUpdate: offsetUpdate,
		updateTopic:  updateTopic,
		updates:      updates,
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

func (un *storeUpdateNode) Init(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
}

func (un *storeUpdateNode) update(msgs []interface{}) {
	var (
		cnt     = len(msgs)
		offsets = make([]int64, cnt)
		updates = make([]*kafka.ProducerMsg, cnt)
	)

	for i, m := range msgs {
		msg := m.(*kafka.IncomingMsg)
		offsets[i] = msg.Offset

		req := msg.Data.(*pb.MgwRequest)
		switch req.Type {
		case pb.MgwRequest_MsgReq:
			var (
				from     = req.MsgReq.From
				to       = req.MsgReq.To[0]
				report   = &base.MsgDeliveryReport{From: from, To: to, Stat: &base.MsgDeliveryStat{Code: base.MsgDeliveryStat_Accepted}}
				event    = pb.NewEventWithMsgDelivery(&pb.MsgDelivery{Req: req.MsgReq, Report: []*base.MsgDeliveryReport{report}})
				key, _   = proto.Marshal(from)
				value, _ = proto.Marshal(&event)
			)

			if ce := un.logger.Check(zap.DebugLevel, "New event"); ce != nil {
				data, _ := json.Marshal(event)
				ce.Write(zap.String("event", string(data)))
			}

			updates[i] = &kafka.ProducerMsg{Topic: un.updateTopic, Key: key, Value: value}
		}
	}

	un.updates <- updates
	un.offsetUpdate <- offsets
	un.rateControl.RemovePending(int64(cnt))
}

func (un *storeUpdateNode) Start(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
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

func (un *storeUpdateNode) Stop(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	close(un.stop)
	<-un.done
}
