package kafka

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/totoview/xin/core"
	"go.uber.org/zap"
)

type MsgDecoder interface {
	Decode(msg *sarama.ConsumerMessage) (interface{}, error)
}

type DecodingErrorHandler interface {
	OnDecodingError(err error, offset int64, key []byte, value []byte)
}

// IncomingMsg is a message received from Kafka.
type IncomingMsg struct {
	Offset int64
	Data   interface{}
}

type sourceNode struct {
	logger *zap.Logger

	brokers   string // broker list
	topic     string // topic
	partition int32  // partition
	offset    int64  // offset

	bufferSize int              // incoming message buffer capacity
	incoming   chan interface{} // incoming message buffer
	decoder    MsgDecoder
	errHandler DecodingErrorHandler

	stop chan struct{}  // stop all partition consumers
	done sync.WaitGroup // all goroutines has stopped
}

func (k *sourceNode) Recv(max int, tmout time.Duration) []interface{} {
	ticker := time.NewTicker(tmout)
	defer ticker.Stop()

	msgs := []interface{}{}
	for i := 0; i < max; i++ {
		select {
		case _m := <-k.incoming:
			if _m != nil {
				m := _m.(*sarama.ConsumerMessage)
				data, err := k.decoder.Decode(m)
				if err == nil {
					msgs = append(msgs, &IncomingMsg{Offset: m.Offset, Data: data})
				} else {
					k.errHandler.OnDecodingError(err, m.Offset, m.Key, m.Value)
				}
			}
		case <-ticker.C:
			return msgs
		}
	}
	return msgs
}

// NewSourceNode creates a Kafka based PullSourceNode to receive messages from parition of topic,
// starting at offset. bufferSize controls how many messages are buffered before a consumer calls Recv.
func NewSourceNode(config *core.ConsumerConfig, partition int32, offset int64,
	decoder MsgDecoder, errHandler DecodingErrorHandler, logger *zap.Logger) (core.PullSourceNode, error) {
	if strings.Compare(config.Type, "kafka") != 0 {
		return nil, errors.Errorf("Invalid config for type %s", config.Type)
	}
	return &sourceNode{
		brokers:    config.Kafka.Brokers,
		topic:      config.Kafka.Topic,
		partition:  partition,
		offset:     offset,
		bufferSize: config.BatchSize * 2,
		decoder:    decoder,
		errHandler: errHandler,
		logger:     logger.With(zap.String("comp", "kafkaSrcN"))}, nil
}

///////////////////////////////////////////////////////////////////////////////////////
// core.Runnable

func (k *sourceNode) Init(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	k.incoming, k.stop = make(chan interface{}, k.bufferSize), make(chan struct{})
}

func (k *sourceNode) Start(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(strings.Split(k.brokers, ","), config)
	if err != nil {
		errc <- err
		return
	}

	pc, err := consumer.ConsumePartition(k.topic, k.partition, k.offset)
	if err != nil {
		errc <- err
		return
	}

	k.done.Add(1)
	go func() {
		defer k.done.Done()
		defer pc.AsyncClose()

		select {
		case err := <-pc.Errors():
			if err != nil {
				// FIXME
				k.logger.DPanic("Error from partition consumer", zap.Error(err))
			}
		case <-k.stop:
		}
	}()

	k.done.Add(1)
	go func() {
		defer consumer.Close()
		defer k.done.Done()

		for msg := range pc.Messages() {
			k.incoming <- msg
		}
	}()
}

func (k *sourceNode) Stop(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	if k.stop != nil {
		close(k.incoming)
		close(k.stop)
		k.done.Wait()
	}
}
