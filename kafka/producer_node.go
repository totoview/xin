package kafka

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/boltdb/bolt"
	"github.com/totoview/xin/core"
	"go.uber.org/zap"
)

var (
	queueMaxPending = 200000
	queueBatchSize  = 100
	sleepAfterError = time.Millisecond * 100
	maxErrDelay     = time.Second * 1
	shutdownDelay   = time.Millisecond * 1000
)

// ProducerNode sends messages to Kafka.
type ProducerNode struct {
	logger *zap.Logger
	config *core.ProducerConfig

	totalIn  uint64
	totalOut uint64
	totalErr uint64

	queue    *ProducerQueue
	incoming chan []*ProducerMsg
	stop     chan struct{}
	done     chan struct{}
}

// NewProducerNode creates a node to send messages to Kafka.
func NewProducerNode(config *core.ProducerConfig, db *bolt.DB, logger *zap.Logger) (*ProducerNode, error) {
	q, err := NewProducerQueue("producerQueue", queueMaxPending, db, logger)
	if err != nil {
		return nil, err
	}
	return &ProducerNode{config: config, incoming: make(chan []*ProducerMsg), queue: q, logger: logger.With(zap.String("comp", "producerN"))}, nil
}

// Input returns the channel to send messages to this node.
func (k *ProducerNode) Input() chan<- []*ProducerMsg {
	return k.incoming
}

///////////////////////////////////////////////////////////////////////////////////////
// core.Runnable

func (k *ProducerNode) Init(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := k.queue.Init(); err != nil {
		errc <- err
	}
}

func (k *ProducerNode) Start(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	k.stop, k.done = make(chan struct{}), make(chan struct{})

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(strings.Split(k.config.Kafka.Brokers, ","), config)
	if err != nil {
		errc <- err
		return
	}

	go func() {
		defer producer.AsyncClose()

		var (
			delay     time.Duration
			toSend    []*ProducerMsg
			toConfirm = make(map[uint64]*ProducerMsg)
			errmode   = false
		)

		onStop := func() {
			// add unconfirmed messages back to queue
			for _, msg := range toConfirm {
				k.queue.AddFailed(msg)
			}
		}

		onError := func(err *sarama.ProducerError) {
			k.totalErr++
			errmode = true
			if err.Msg != nil {
				msg := err.Msg.Metadata.(*ProducerMsg)
				k.queue.AddFailed(msg)
				delete(toConfirm, msg.ID)
			}
		}

		onSuccess := func(msg *sarama.ProducerMessage) {
			errmode, delay = false, 0
			delete(toConfirm, msg.Metadata.(*ProducerMsg).ID)
			k.totalOut++
		}

		onDelay := func() {
			delay += sleepAfterError
			if delay > maxErrDelay {
				delay = maxErrDelay
			}
			<-time.After(delay)
		}

	mainLoop:
		for {

			toSend = toSend[:0]

			// get messages to send. need to keep trying until we have a message to send.
			// note that we alse need to drain message from producer's error and success channels
			// otherwise we may deadlock when trying to send message in the next for loop.

			for len(toSend) == 0 {
				select {
				case <-k.stop:
					onStop()
					break mainLoop
				case reqs := <-k.incoming:
					k.totalIn += uint64(len(reqs))
					k.queue.Add(reqs...)
				case <-k.queue.Ready:
					if !errmode {
						toSend, _ = k.queue.Get(queueBatchSize)
					} else {
						onDelay()
						// retry with the first message only
						toSend, _ = k.queue.Get(1)
					}
				case err := <-producer.Errors():
					onError(err)
				case msg := <-producer.Successes():
					onSuccess(msg)
				}
			}

			// send messages
			for i, msg := range toSend {
				// loop until message is sent
				for msg != nil {
					// add delay if in error mode
					if errmode {
						onDelay()
					}

					select {
					case <-k.stop:
						// add pending messages back to queue
						k.queue.AddFailed(toSend[i:]...)
						onStop()
						break mainLoop
					case producer.Input() <- &sarama.ProducerMessage{Topic: msg.Topic, Key: sarama.StringEncoder(msg.Key), Value: sarama.ByteEncoder(msg.Value), Metadata: msg}:
						toConfirm[msg.ID] = msg
						msg = nil
					case err := <-producer.Errors():
						onError(err)
					case msg := <-producer.Successes():
						onSuccess(msg)
					case reqs := <-k.incoming:
						k.totalIn += uint64(len(reqs))
						k.queue.Add(reqs...)
					}
				}
			}
		}

		// wait a little longer for potential errors
		shutdown := make(chan struct{})
		go func() {
			<-time.After(shutdownDelay)
			close(shutdown)
		}()

		for {
			select {
			case <-shutdown:
				k.done <- struct{}{}
				return
			case err := <-producer.Errors():
				if err.Msg != nil {
					k.queue.AddFailed(err.Msg.Metadata.(*ProducerMsg))
				}
			}
		}
	}()
	k.logger.Debug("Start producer node", zap.Uint64("saved", k.queue.Count()))
}

func (k *ProducerNode) Stop(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	if k.stop != nil {
		k.stop <- struct{}{}
		<-k.done
	}
	err := k.queue.Close()
	if err == nil {
		k.logger.Debug("Stop producer node",
			zap.Uint64("totalIn", k.totalIn),
			zap.Uint64("totalOut", k.totalOut),
			zap.Uint64("totalErr", k.totalErr),
			zap.Uint64("saved", k.queue.Count()))
	} else {
		k.logger.Error("Failed to stop producer node",
			zap.Error(err),
			zap.Uint64("totalIn", k.totalIn),
			zap.Uint64("totalOut", k.totalOut),
			zap.Uint64("totalErr", k.totalErr),
			zap.Uint64("saved", k.queue.Count()))
	}
	if err != nil {
		errc <- err
	}
}
