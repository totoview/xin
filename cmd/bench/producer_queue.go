package bench

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/boltdb/bolt"
	"github.com/spf13/cobra"
	"github.com/totoview/xin/kafka"
	"go.uber.org/zap"
)

var (
	totalKafkaMsgs   uint32
	kafkaBrokers     string
	kafkaTopic       string
	tickerCount      int32
	maxPending       int32
	kafkaDequeueSize int32
)

func init() {
	ProducerQueueCmd.Flags().Uint32VarP(&totalKafkaMsgs, "total", "", 100000, "total messages")
	ProducerQueueCmd.Flags().StringVarP(&kafkaBrokers, "brokers", "", "127.0.0.1:9092", "Kafka brokers")
	ProducerQueueCmd.Flags().StringVarP(&kafkaTopic, "topic", "", "xin-store-test", "Kafka topic")
	ProducerQueueCmd.Flags().Int32VarP(&tickerCount, "tickers", "", 2, "number of tickers")
	ProducerQueueCmd.Flags().Int32VarP(&maxPending, "maxpending", "", 200000, "max pending messages in queue")
	ProducerQueueCmd.Flags().Int32VarP(&kafkaDequeueSize, "dequeuesize", "", 200, "number of messages to get from queue each time")
}

func round(d time.Duration) time.Duration {
	if d > 1*time.Second {
		return d.Round(10 * time.Millisecond)
	}
	return d.Round(10 * time.Microsecond)
}

var ProducerQueueCmd = &cobra.Command{
	Use:   "producerqueue",
	Short: "Benchmark ProducerQueue",
	Long: `Test the performance of ProducerQueue using a setup similar to the one used by kafka.ProducerNode.
A number of tickers are used to generate messages at the rate of one message per microsecond. Each ticker runs
in a dedicated goroutine and all messages are sent to a shared channel. A producer goroutine receives messages
from the channel and put them in a ProducerQueue. At the same time, it dequeues message from the queue and send
them to Kafka. A set of consumers receives messages from Kafka with one consumer per Kafka partition. Each
consumer runs in a separate goroutine.`,
	PreRun: func(cmd *cobra.Command, args []string) {},
	Run: func(cmd *cobra.Command, args []string) {

		tmpfile, err := ioutil.TempFile("", "pqtest")
		if err != nil {
			fmt.Printf("Failed to create db file: %s\n", err.Error())
			return
		}
		fname := tmpfile.Name()
		tmpfile.Close()
		defer os.Remove(fname)

		db, err := bolt.Open(fname, 0600, nil)
		if err != nil {
			fmt.Printf("Failed to open db: %s\n", err.Error())
			return
		}
		defer db.Close()
		queue, err := kafka.NewProducerQueue("test", int(maxPending), db, zap.NewNop())
		if err != nil {
			fmt.Printf("Failed to create producer queue: %s\n", err.Error())
			return
		}
		if err := queue.Init(); err != nil {
			fmt.Printf("Failed to init queue: %s\n", err.Error())
			return
		}

		stop := make(chan struct{})
		var wg sync.WaitGroup

		producerConfig := sarama.NewConfig()
		producerConfig.Producer.RequiredAcks = sarama.WaitForLocal
		producerConfig.Producer.Return.Errors = true
		producerConfig.Producer.Return.Successes = true

		producer, err := sarama.NewAsyncProducer(strings.Split(kafkaBrokers, ","), producerConfig)
		if err != nil {
			panic(err)
		}

		consumerConfig := sarama.NewConfig()
		consumerConfig.Consumer.Return.Errors = true
		consumer, err := sarama.NewConsumer(strings.Split(kafkaBrokers, ","), consumerConfig)
		if err != nil {
			panic(err)
		}

		partitions, err := kafka.ListPartitions(kafkaBrokers, kafkaTopic)
		if err != nil {
			panic(err)
		}

		var checkPoint uint32
		for checkPoint = 10; checkPoint*100 < totalKafkaMsgs; checkPoint *= 10 {
		}
		for ; checkPoint*20 < totalKafkaMsgs; checkPoint *= 2 {
		}

		// stgart the consumers
		var wgConsumer sync.WaitGroup
		recved := uint32(0)

		start := time.Now()
		for _, part := range partitions {
			wg.Add(1)
			wgConsumer.Add(1)
			go func(part kafka.Partition) {

				cnt := 0
				var delay time.Duration

				fmt.Printf("Consumer for partition %d started (offset: %d-%d)\n", part.ID, part.OldestOffset, part.NewestOffset)

				pc, _ := consumer.ConsumePartition(kafkaTopic, part.ID, sarama.OffsetNewest)

				defer wg.Done()
				defer pc.AsyncClose()

				wgConsumer.Done()
				for {
					select {
					case <-stop:
						fmt.Printf("Consumer for partition %d stopped (recved=%d, totalLatency=%v, avgLatency=%v)\n",
							part.ID, cnt, round(delay), round(time.Duration(delay.Nanoseconds()/int64(cnt))*time.Nanosecond))
						return
					case err := <-pc.Errors():
						panic(err)
					case msg := <-pc.Messages():
						nanos := binary.LittleEndian.Uint64(msg.Value)
						delay += time.Now().Sub(time.Unix(0, int64(nanos)))
						cnt++
						n := atomic.AddUint32(&recved, 1)
						if n%checkPoint == 0 {
							fmt.Printf("- %d msgs recv in %v: %.2f/s\n", n, round(time.Now().Sub(start)), float64(totalKafkaMsgs)/time.Now().Sub(start).Seconds())
						}
						if n >= totalKafkaMsgs {
							fmt.Printf("\n%d msgs recv in %v: %.2f/s\n\n", n, round(time.Now().Sub(start)), float64(totalKafkaMsgs)/time.Now().Sub(start).Seconds())
							close(stop)
						}
					}
				}
			}(part)
		}

		// wait for consumers to finish starting before starting producer
		// otherwise we may miss messages
		wgConsumer.Wait()

		cmsgs := make(chan *kafka.ProducerMsg, tickerCount*10)
		cnt, tickInt, tickStart := uint32(0), 1*time.Microsecond, time.Now()

		for i := int32(0); i < tickerCount; i++ {
			wg.Add(1)
			go func(tickerID int32) {
				defer wg.Done()
				ticker := time.NewTicker(tickInt)
				fmt.Printf("Ticker %d started, generating 1 msg every %v\n", tickerID, tickInt)

				for {
					select {
					case <-stop:
						fmt.Printf("Ticker %d stopped\n", tickerID)
						return
					case <-ticker.C:
						if atomic.LoadUint32(&cnt) < totalKafkaMsgs {
							n := atomic.AddUint32(&cnt, 1)
							if n%checkPoint == 0 {
								fmt.Printf("@ %d msgs gene in %v: %.2f/s\n", n, round(time.Now().Sub(tickStart)), float64(n)/time.Now().Sub(tickStart).Seconds())
							}

							if n >= totalKafkaMsgs {
								fmt.Printf("%d msgs generated in %v (%.2f/s)\n", totalKafkaMsgs, round(time.Now().Sub(tickStart)), float64(totalKafkaMsgs)/time.Now().Sub(tickStart).Seconds())
								ticker.Stop()
							}

							value := make([]byte, 8)
							binary.LittleEndian.PutUint64(value, uint64(time.Now().UnixNano()))
							key := make([]byte, 4)
							binary.LittleEndian.PutUint32(key, n)
							cmsgs <- &kafka.ProducerMsg{Topic: kafkaTopic, Key: key, Value: value}
						}
					}
				}
			}(i)
		}

		wg.Add(1)
		go func() {
			defer producer.AsyncClose()
			defer wg.Done()

			fmt.Printf("Producer started\n")

			var (
				msgs      []*kafka.ProducerMsg
				sent      = uint32(0)
				confirmed = uint32(0)
				start     = time.Now()
				requested = make(map[uint64]*kafka.ProducerMsg)
			)

			onStop := func() {
				close(cmsgs)
				for _, msg := range requested {
					queue.AddFailed(msg)
				}
				fmt.Println("Producer stopped")
			}

			onError := func(err *sarama.ProducerError) {
				if err.Msg != nil {
					msg := err.Msg.Metadata.(*kafka.ProducerMsg)
					delete(requested, msg.ID)
					queue.AddFailed(msg)
				}
			}

			onSuccess := func(msg *sarama.ProducerMessage) {
				delete(requested, msg.Metadata.(*kafka.ProducerMsg).ID)
				confirmed++
				if confirmed%checkPoint == 0 {
					fmt.Printf("+ %d msgs sent in %v: %.2f/s\n", confirmed, round(time.Now().Sub(start)), float64(confirmed)/time.Now().Sub(start).Seconds())
				}
			}

			for {

				msgs = msgs[:0]

				// keep trying until we have a message to send. note that we need to
				// drain message from producer's error and success channels as well
				// otherwise we may deadlock when trying to send message in the next for loop.
				for len(msgs) == 0 {
					select {
					case <-stop:
						onStop()
						return
					case msg := <-cmsgs:
						queue.Add(msg)
						msgs, _ = queue.Get(int(kafkaDequeueSize))
					case <-queue.Ready:
					case err := <-producer.Errors():
						onError(err)
						panic(err)
					case msg := <-producer.Successes():
						onSuccess(msg)
					}
				}

				// need to keep draining until msg is sent
				for _, msg := range msgs {
					for msg != nil {
						select {
						case <-stop:
							onStop()
							return
						case producer.Input() <- &sarama.ProducerMessage{Topic: msg.Topic, Key: sarama.ByteEncoder(msg.Key), Value: sarama.ByteEncoder(msg.Value), Metadata: msg}:
							requested[msg.ID] = msg
							msg = nil
							sent++
							if sent%checkPoint == 0 {
								fmt.Printf("* %d msgs proc in %v: %.2f/s\n", sent, round(time.Now().Sub(start)), float64(sent)/time.Now().Sub(start).Seconds())
							}
						case err := <-producer.Errors():
							onError(err)
							panic(err)
						case msg := <-producer.Successes():
							onSuccess(msg)
						}
					}
				}
			}
		}()

		go func() {
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt)
			<-c
			pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			close(stop)
			queue.Close()
		}()

		wg.Wait()
	},
}
