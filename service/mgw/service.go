package mgw

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/boltdb/bolt"
	"github.com/gogo/protobuf/proto"
	"github.com/spf13/viper"
	"github.com/totoview/xin/core"
	"github.com/totoview/xin/kafka"
	pb "github.com/totoview/xin/pb/mgw"
	"github.com/totoview/xin/service"
	"go.uber.org/zap"
)

// helpers

type kafkaMsgDecoder struct{}

func (d *kafkaMsgDecoder) Decode(m *sarama.ConsumerMessage) (interface{}, error) {
	var msg pb.MgwRequest
	if err := proto.Unmarshal(m.Value, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

type decodingErrorHandler struct {
	logger       *zap.Logger
	offsetUpdate chan<- []int64
}

func (h *decodingErrorHandler) OnDecodingError(err error, offset int64, key []byte, value []byte) {
	h.logger.Error("Failed to decode Kafka msg", zap.String("key", hex.Dump(key)), zap.String("value", hex.Dump(value)))
	h.offsetUpdate <- []int64{offset}
}

// Service implements the message gateway service.
type Service struct {
	service.KafkaConsumer
	updateConfig *core.ProducerConfig // config for sending update messages
}

// NewFromConfig makes a new message gateway serivce based on config.
func NewFromConfig(config *viper.Viper, logger *zap.Logger) (core.Service, error) {
	if err := core.VerifyConfig(config, Schema); err != nil {
		return nil, err
	}
	var (
		sessionConfig, _ = core.GetSessionConfig(config.Sub("session"))
		sourceConfig, _  = core.GetConsumerConfig(config.Sub("source"))
		updateConfig, _  = core.GetProducerConfig(config.Sub("update"))
	)

	db, err := bolt.Open(sessionConfig.Store.Bolt.Path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	return NewService(sourceConfig, updateConfig, db, logger)
}

// NewService makes a new mgw instance for Kafka message source.
func NewService(sourceConfig *core.ConsumerConfig, updateConfig *core.ProducerConfig, db *bolt.DB, logger *zap.Logger) (*Service, error) {
	s := &Service{updateConfig: updateConfig}
	if err := s.InitKafkaConsumer(sourceConfig, db, logger, s); err != nil {
		return nil, err
	}
	return s, nil
}

// NewKafkaConsumerPipline implements the KafkaConsumerPipelineBuilder interface.
func (s *Service) NewKafkaConsumerPipline(index int, initOffsets kafka.InitialOffsets, part kafka.Partition) (*core.Pipeline, error) {
	p := core.NewPipeline(fmt.Sprintf("%s:%d", s.ConsumerConfig.Kafka.Topic, part.ID), s.Logger)

	// add offset tracking service
	offsetTracker := core.NewOffsetTrackingNode(initOffsets.FirstMsgOffset, func(offset int64) {
		s.Offsets[index] = offset
	})
	p.AddService(offsetTracker)

	// add event producer service
	eventProducer, err := kafka.NewProducerNode(s.updateConfig, s.DB, s.Logger)
	if err != nil {
		return nil, err
	}
	p.AddService(eventProducer)

	// add source
	kafkaSrcNode, err := kafka.NewSourceNode(s.ConsumerConfig, part.ID, initOffsets.ConsumerOffset,
		&kafkaMsgDecoder{}, &decodingErrorHandler{logger: s.Logger, offsetUpdate: offsetTracker.UpdateChannel()}, s.Logger)
	if err != nil {
		return nil, err
	}
	p.AddSourceNode(kafkaSrcNode)

	// add dispatcher
	dispatcherNode, err := service.NewDispatcherNode(s.ConsumerConfig, kafkaSrcNode, 50*time.Millisecond, s.Logger)
	if err != nil {
		return nil, err
	}
	p.AddChildNode(dispatcherNode, kafkaSrcNode)

	storeUpdateNode, err := NewStoreUpdateNode(dispatcherNode, s.updateConfig.Kafka.Topic, eventProducer.Input(), offsetTracker.UpdateChannel(), dispatcherNode.RateControl, s.Logger)
	if err != nil {
		return nil, err
	}
	p.AddChildNode(storeUpdateNode, dispatcherNode)

	return p, nil
}
