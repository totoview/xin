package kafka

import (
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// Partition contains id and offsets of a Kafka partition.
type Partition struct {
	ID           int32
	OldestOffset int64 // oldest offset available on the broker for this partition
	NewestOffset int64 // offset assigned to the next message produced to this partition
}

// ListPartitions returns the list of partitions for topic.
func ListPartitions(brokers, topic string) ([]Partition, error) {
	client, err := sarama.NewClient(strings.Split(brokers, ","), nil)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, err
	}

	pi := make([]Partition, len(partitions))
	for i, part := range partitions {
		new, err := client.GetOffset(topic, part, sarama.OffsetNewest)
		if err != nil {
			return nil, err
		}
		old, err := client.GetOffset(topic, part, sarama.OffsetOldest)
		if err != nil {
			return nil, err
		}
		pi[i] = Partition{ID: part, OldestOffset: old, NewestOffset: new}
	}

	return pi, nil
}

// InitialOffsets are offsets required for offset tracking and message consumption
// when receiving messages from a Kafka partition.
type InitialOffsets struct {
	FirstMsgOffset int64 // offset of the next message consumed from the partition
	ConsumerOffset int64 // starting offset to be passed to consumer to consume messages from the partition
}

// GetInitialOffsets returns the initial offsets required by a Kafka consumer pipeline
// given the last saved offset savedOffset and partition information returned
// by Kafka part. If there's no existing saved offset, savedOffset will be negative.
func GetInitialOffsets(savedOffset int64, part Partition) (InitialOffsets, error) {
	if part.NewestOffset <= 0 {
		// no messages, ignore saved offset
		return InitialOffsets{
			FirstMsgOffset: 0,
			ConsumerOffset: sarama.OffsetOldest,
		}, nil
	}

	if savedOffset < 0 {
		// no existing saved offset, start from the beginning
		return InitialOffsets{
			FirstMsgOffset: part.OldestOffset,
			ConsumerOffset: sarama.OffsetOldest,
		}, nil
	}

	if savedOffset > 0 && savedOffset < part.OldestOffset {
		// we are somehow lagging behind, this could be serious, report error
		return InitialOffsets{}, errors.Errorf("Saved offset (%v) greater than oldest offset (%v)", savedOffset, part.OldestOffset)
	}

	// if the saved offset is just 1 behind or the same as the newest partition offset, all
	// the messages have already be consumed, in this case, we can't use the newest
	// offset itself as the starting offset as the message for that offset doesn't
	// exist, have to use the special value
	if savedOffset+1 >= part.NewestOffset {
		return InitialOffsets{
			FirstMsgOffset: part.NewestOffset,
			ConsumerOffset: sarama.OffsetNewest,
		}, nil
	}

	return InitialOffsets{
		FirstMsgOffset: savedOffset + 1,
		ConsumerOffset: savedOffset + 1,
	}, nil
}
