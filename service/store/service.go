package store

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gogo/protobuf/proto"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/boltdb/bolt"
	"github.com/totoview/xin/core"
	"github.com/totoview/xin/es"
	"github.com/totoview/xin/kafka"
	pb "github.com/totoview/xin/pb/mgw"
	"github.com/totoview/xin/service"
)

// helpers

type kafkaMsgDecoder struct{}

func (d *kafkaMsgDecoder) Decode(m *sarama.ConsumerMessage) (interface{}, error) {
	var msg pb.MgwEvent
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

// Service implements the message store service. It consumes incoming messages
// from Kafka and stores them in ElasticSearch. It then publishes update events
// to Kafka for downstream services.
//
// This is an internal service without any public interfaces.
type Service struct {
	service.KafkaConsumer                          // message source
	storeConfig           *core.MessageStoreConfig // message storage
	updateConfig          *core.ProducerConfig     // updates
	DB                    *bolt.DB                 // session persistence
}

// NewFromConfig makes a new Store instance for Kafka message source based on config.
func NewFromConfig(config *viper.Viper, logger *zap.Logger) (*Service, error) {
	if err := core.VerifyConfig(config, Schema); err != nil {
		return nil, err
	}
	var (
		sessionConfig, _ = core.GetSessionConfig(config.Sub("session"))
		sourceConfig, _  = core.GetConsumerConfig(config.Sub("source"))
		storeConfig, _   = core.GetMessageStoreConfig(config.Sub("store"))
		updateConfig, _  = core.GetProducerConfig(config.Sub("update"))
	)

	db, err := bolt.Open(sessionConfig.Store.Bolt.Path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	return New(sourceConfig, storeConfig, updateConfig, db, logger)
}

// New makes a new Store instance for Kafka message source.
func New(sourceConfig *core.ConsumerConfig, storeConfig *core.MessageStoreConfig, updateConfig *core.ProducerConfig,
	db *bolt.DB, logger *zap.Logger) (*Service, error) {

	s := &Service{storeConfig: storeConfig, updateConfig: updateConfig, DB: db}
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

	// add store
	msgStore, err := es.NewStore(s.updateConfig.Kafka.Topic, s.storeConfig.ES, offsetTracker.UpdateChannel(), eventProducer.Input(), dispatcherNode.RateControl, s.Logger)
	if err != nil {
		return nil, err
	}

	storeNode, err := NewStoreNode(dispatcherNode, msgStore, s.Logger)
	if err != nil {
		return nil, err
	}
	p.AddChildNode(storeNode, dispatcherNode)

	return p, nil
}
