package es

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/olivere/elastic"
	"github.com/totoview/xin/core"
	"github.com/totoview/xin/kafka"
	pb "github.com/totoview/xin/pb/mgw"
	pbstore "github.com/totoview/xin/pb/store"
	"go.uber.org/zap"
)

func toDocID(req *pb.MsgReq) string {
	return fmt.Sprintf("%v_%v_%v", req.App, req.Id, req.Msg.TsNanos)
}

func toDoc(msg map[string]interface{}) map[string]interface{} {
	// FIXME
	return msg
}

type EsStore struct {
	logger *zap.Logger
	config *core.ElasticSearchConfig
	client *elastic.Client

	total        uint64
	offsetUpdate chan<- []int64
	eventTopic   string
	events       chan<- []*kafka.ProducerMsg
	rateControl  *core.Muffler
}

func (es *EsStore) Save(msgs []interface{}) error {

	var (
		cnt     = len(msgs)
		offsets = make([]int64, cnt)
		bulk    = es.client.Bulk()
		start   = time.Now()
		events  = make([]*kafka.ProducerMsg, 0)
	)

	for i, m := range msgs {

		msg := m.(*kafka.IncomingMsg)
		evt := msg.Data.(*pb.MgwEvent)

		offsets[i] = msg.Offset

		switch evt.Type {
		default:
			es.logger.DPanic(fmt.Sprintf("FIXME: handle unknown event type: %s", evt.Type))

		case pb.MgwEvent_MsgDelivery:
			// use message data timestamp
			ts := time.Unix(0, int64(evt.MsgDelivery.Req.Msg.TsNanos)).UTC()
			req := elastic.NewBulkUpdateRequest().
				Index(fmt.Sprintf("xin_%d-%02d", ts.Year(), ts.Month())).
				Type("messages").
				Id(toDocID(evt.MsgDelivery.Req)).
				Doc(evt.MsgDelivery.Req).
				DocAsUpsert(true).
				DetectNoop(true)

			if ce := es.logger.Check(zap.DebugLevel, "ES bulk"); ce != nil {
				ce.Write(zap.String("update", req.String()))
			}

			bulk.Add(req)
		}
	}

	rsp, err := bulk.Do(context.Background())

	if err == nil {

		// check response of each item
		for i, item := range rsp.Items {

			v, _ := json.Marshal(item)
			fmt.Printf("[%d] %s\n", i, string(v))

			update, ok := item["update"]
			if ok {
				switch update.Status / 100 {
				default:
					es.logger.Error("FIXME: handle unexpected bulk status code", zap.Int("status", update.Status))
				case 2:
					switch update.Result {
					default:
						es.logger.Error("FIXME: handle unexpected bulk update result", zap.String("result", update.Result))
					case "noop":
						// duplicate item, nothing to do
					case "created":
						var (
							// FIXME: do we always have MsgDelivery here ?
							msg   = msgs[i].(*kafka.IncomingMsg).Data.(*pb.MgwEvent).MsgDelivery
							from  = msg.Req.From
							event = pbstore.NewEventWithMsgCreated(&pbstore.MsgCreated{
								Index: update.Index,
								Id:    update.Id,
								Ver:   update.Version,
								Type:  update.Type,
							})
							key, _   = proto.Marshal(from)
							value, _ = proto.Marshal(&event)
						)

						if ce := es.logger.Check(zap.DebugLevel, "New event"); ce != nil {
							data, _ := json.Marshal(event)
							ce.Write(zap.String("event", string(data)))
						}

						events = append(events, &kafka.ProducerMsg{Topic: es.eventTopic, Key: key, Value: value})
					}
				}
			} else {
				es.logger.Error("Unexpected bulk response item")
			}
		}

		es.total += uint64(cnt)

		es.logger.Info("Save messages",
			zap.Int("count", len(msgs)),
			zap.Duration("duration", time.Now().Sub(start)),
			zap.Uint64("total", es.total))

		if len(events) > 0 {
			es.events <- events
		}

		es.offsetUpdate <- offsets
		es.rateControl.RemovePending(int64(cnt))
	} else {
		es.logger.Error("Bulk update failed", zap.Error(err))
	}

	return err
}

// NewStore creates a ElasticSearch based message store
func NewStore(eventTopic string, config *core.ElasticSearchConfig,
	offsetUpdate chan<- []int64, events chan<- []*kafka.ProducerMsg,
	rateControl *core.Muffler, logger *zap.Logger) (*EsStore, error) {
	client, err := elastic.NewClient(elastic.SetURL(config.Addr))
	if err != nil {
		return nil, err
	}
	return &EsStore{
		logger:       logger.With(zap.String("comp", "esStore")),
		config:       config,
		client:       client,
		offsetUpdate: offsetUpdate,
		rateControl:  rateControl,
		eventTopic:   eventTopic,
		events:       events,
	}, nil
}
