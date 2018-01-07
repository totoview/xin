package service

import (
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/totoview/xin/core"
	"github.com/totoview/xin/kafka"
	"go.uber.org/zap"
)

func pipelineID(topic string, partition int32) string {
	return fmt.Sprintf("__pipeline_%s_%d__", topic, partition)
}

func eventID(topic string) string {
	return fmt.Sprintf("__event_id_%s__", topic)
}

// KafkaConsumerPipelineBuilder defines the interface for a solid KafkaConsumer implementation
type KafkaConsumerPipelineBuilder interface {
	NewKafkaConsumerPipline(index int, initOffsets kafka.InitialOffsets, part kafka.Partition) (*core.Pipeline, error)
}

// KafkaConsumer provides the base implementation of services that process messages from a Kafka topic.
type KafkaConsumer struct {
	Logger         *zap.Logger
	ConsumerConfig *core.ConsumerConfig
	Processor      KafkaTopicProcessor
	Offsets        []int64           // partition offsets
	SessionStore   *core.BoltKVStore // session store
	DB             *bolt.DB          // backing db of the session store
}

// InitKafkaConsumer initializes KafkaConsumer for an actual implementation.
func (s *KafkaConsumer) InitKafkaConsumer(config *core.ConsumerConfig, db *bolt.DB,
	logger *zap.Logger, imp KafkaConsumerPipelineBuilder) error {

	sessionStore, err := core.NewBoltKVStore(fmt.Sprintf("_kafka_consumer_kv_store_%s_", config.Kafka.Topic), db)
	if err != nil {
		return err
	}

	s.DB, s.ConsumerConfig = db, config
	s.Logger, s.SessionStore = logger, sessionStore

	processor, err := NewKafkaTopicProcessor(config.Kafka.Brokers, config.Kafka.Topic, func(index int, part kafka.Partition) (*core.Pipeline, error) {
		savedOffset, err := core.KVGetInt64(sessionStore, pipelineID(config.Kafka.Topic, part.ID))
		if err != nil {
			if err == core.ErrNotFound {
				// no existing offset
				savedOffset = -1
			} else {
				return nil, err
			}
		}
		initOffsets, err := kafka.GetInitialOffsets(savedOffset, part)
		if err != nil {
			return nil, err
		}
		s.Logger.Debug("New topic processor",
			zap.String("topic", s.ConsumerConfig.Kafka.Topic),
			zap.Int32("partition", part.ID),
			zap.Int64("savedOffset", savedOffset),
			zap.Int64("firstMsgOffset", initOffsets.FirstMsgOffset),
			zap.Int64("comsuerOffset", initOffsets.ConsumerOffset))

		p, err := imp.NewKafkaConsumerPipline(index, initOffsets, part)
		if err != nil {
			return nil, err
		}
		s.Offsets = append(s.Offsets, initOffsets.FirstMsgOffset)
		return p, nil
	})

	if err != nil {
		return err
	}
	s.Processor = processor
	return nil
}

// SaveOffsets saves Kafka partition offsets.
func (s *KafkaConsumer) SaveOffsets() {
	for i, p := range s.Processor {
		// save offsets
		s.Logger.Debug("Save topic processor offset",
			zap.String("topic", s.ConsumerConfig.Kafka.Topic),
			zap.Int32("partition", p.Partition.ID),
			zap.Int64("savedOffset", s.Offsets[i]))

		core.KVPutInt64(s.SessionStore, pipelineID(s.ConsumerConfig.Kafka.Topic, p.Partition.ID), s.Offsets[i])
	}
}

/////////////////////////////////////////////////////////////////////////////////////
// core.Service

// Init initialises s
func (s *KafkaConsumer) Init(tmoutMillis int64) error {
	err := s.Processor.Init(tmoutMillis)
	if err == nil {
		s.Logger.Debug("Init")
	} else {
		s.Logger.Error("Failed to init", zap.Error(err))
	}
	return err
}

// Start runs s.
func (s *KafkaConsumer) Start(tmoutMillis int64) error {
	err := s.Processor.Start(tmoutMillis)
	if err == nil {
		s.Logger.Info("Start")
	} else {
		s.Logger.Error("Failed to start", zap.Error(err))
	}
	return err
}

// Stop stops s.
func (s *KafkaConsumer) Stop(tmoutMillis int64) error {
	s.SaveOffsets()
	err := s.Processor.Stop(tmoutMillis)
	if err == nil {
		s.Logger.Info("Stop")
	} else {
		s.Logger.Error("Failed to stop", zap.Error(err))
	}
	return err
}
