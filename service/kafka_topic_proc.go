package service

import (
	"github.com/totoview/xin/core"
	"github.com/totoview/xin/kafka"
)

// KafkaPartitionProcessor processes messages from a Kafka partition
type KafkaPartitionProcessor struct {
	Partition kafka.Partition
	Pipeline  *core.Pipeline
}

// KafkaTopicProcessor processes messages from a Kafka topic.
type KafkaTopicProcessor []*KafkaPartitionProcessor
type KafkaPipelineFactory func(index int, partition kafka.Partition) (*core.Pipeline, error)

// NewKafkaTopicProcessor creates a new Kafka topic processor.
func NewKafkaTopicProcessor(brokers, topic string, cb KafkaPipelineFactory) (KafkaTopicProcessor, error) {
	partitions, err := kafka.ListPartitions(brokers, topic)
	if err != nil {
		return nil, err
	}

	proc := make(KafkaTopicProcessor, len(partitions))

	for i, part := range partitions {
		p, err := cb(i, part)
		if err != nil {
			return nil, err
		}
		proc[i] = &KafkaPartitionProcessor{Partition: part, Pipeline: p}
	}
	return proc, nil
}

//////////////////////////////////////////////////////////////////////////////////
// core.Service

// Init initialises s
func (s *KafkaTopicProcessor) Init(tmoutMillis int64) error {
	var err error
	for _, p := range *s {
		err = core.WrapErr(err, p.Pipeline.Init(tmoutMillis))
	}
	return err
}

// Start runs s
func (s *KafkaTopicProcessor) Start(tmoutMillis int64) error {
	var err error
	for _, p := range *s {
		err = core.WrapErr(err, p.Pipeline.Start(tmoutMillis))
	}
	return err
}

// Stop stops s.
func (s *KafkaTopicProcessor) Stop(tmoutMillis int64) error {
	var err error
	for _, p := range *s {
		err = core.WrapErr(err, p.Pipeline.Stop(tmoutMillis))
	}
	return err
}
