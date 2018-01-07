package kafka

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/boltdb/bolt"
	"github.com/totoview/xin/core"
	"go.uber.org/zap"
)

const (
	currentIDKey = "__producer_queue_current_id__"
)

type msgCodec struct{}

func (c *msgCodec) Encode(msg interface{}) ([]byte, []byte) {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, msg.(*ProducerMsg).ID)
	return key, msg.(*ProducerMsg).Marshal()
}

func (c *msgCodec) Decode(key, value []byte) (interface{}, error) {
	msg := &ProducerMsg{ID: binary.BigEndian.Uint64(key)}
	msg.Unmarshal(value)
	return msg, nil
}

// Unfortunately, BoltQueue works with []interface{} and the
// conversion between []interface{} and []*ProducerMsg is O(n). The overhead
// of this should be relatively small when there are many boltdb operations.

func fromGenericSlice(s []interface{}) []*ProducerMsg {
	msgs := make([]*ProducerMsg, len(s))
	for i, m := range s {
		msgs[i] = m.(*ProducerMsg)
	}
	return msgs
}

func toGenericSlice(msgs []*ProducerMsg) []interface{} {
	s := make([]interface{}, len(msgs))
	for i, m := range msgs {
		s[i] = m
	}
	return s
}

// ProducerQueue buffers messages to be sent to Kafka, including those that have failed
// and require retries. It persists messages to bolt when there are too many messages.
// It also saves pending messages to bolt on Close() and restores them on Init(). It uses
// ProducerMsg.id to keep track of message enqueue order and tries to dequeue messages
// in the same order.
//
// The queue is not thread-safe. It must not be shared by multiple goroutines.
type ProducerQueue struct {
	name   string
	logger *zap.Logger

	id         uint64
	total      uint64
	boltQueue  *core.BoltQueue
	MaxPending int // max pending messages in memory
	pending    []*ProducerMsg
	failed     *msgHeap

	isReady bool
	Ready   chan struct{}
}

// NewProducerQueue creates a new instance of ProducerQueue backed by db.
func NewProducerQueue(name string, maxPending int, db *bolt.DB, logger *zap.Logger) (*ProducerQueue, error) {
	bq, err := core.NewBoltQueue(db, fmt.Sprintf("__producer_queue_%s__", name), &msgCodec{}, 5000)
	if err != nil {
		return nil, err
	}
	logger = logger.With(zap.String("comp", "producerQ"), zap.String("name", name))
	q := &ProducerQueue{name: name, MaxPending: maxPending, boltQueue: bq, logger: logger, id: 1, failed: &msgHeap{}}
	heap.Init(q.failed)
	return q, nil
}

// Count returns total number of messages in queue
func (q *ProducerQueue) Count() uint64 {
	return q.total
}

// FailedCount returns number of in-memory failed messages.
func (q *ProducerQueue) FailedCount() int {
	return q.failed.Len()
}

// PendingCount returns number of in-memory pending messages.
func (q *ProducerQueue) PendingCount() int {
	return len(q.pending)
}

// OndiskCount returns number of pending messages saved on-disk.
func (q *ProducerQueue) OndiskCount() uint64 {
	return q.boltQueue.Count()
}

// IsReady returns true if and only if the queue is not empty
func (q *ProducerQueue) IsReady() bool {
	return q.isReady
}

// Block blocks the ready channel, used for graceful shutdown
func (q *ProducerQueue) Block() {
	if q.isReady {
		q.isReady = false
		q.Ready = make(chan struct{})
	}
}

// Unblock marks the queue as ready for consumers.
func (q *ProducerQueue) Unblock() {
	if !q.isReady {
		q.isReady = true
		close(q.Ready)
	}
}

// Get removes up to cnt messages from queue. The returned messages are either all failed
// messages or all new messages, allowing application of different handling strategies to them.
// If the second return value is true, the returned messages are regular messages; failed
// messages otherwise.
func (q *ProducerQueue) Get(cnt int) ([]*ProducerMsg, bool) {
	msgs, isNew := []*ProducerMsg{}, true
	if q.failed.Len() > 0 {
		isNew = false
		for i := 0; i < cnt && q.failed.Len() > 0; i++ {
			msgs = append(msgs, heap.Pop(q.failed).(*ProducerMsg))
		}
	} else {
		if len(q.pending) == 0 {
			s, _ := q.boltQueue.Get(int(q.MaxPending) + cnt)
			q.pending = fromGenericSlice(s)
		}
		if len(q.pending) > cnt {
			msgs = q.pending[:cnt]
			q.pending = q.pending[cnt:]
		} else {
			msgs = q.pending
			q.pending = make([]*ProducerMsg, 0)
		}
	}
	q.total -= uint64(len(msgs))
	if q.total == 0 {
		q.Block()
	}
	return msgs, isNew
}

// Add inserts new messages into queue.
func (q *ProducerQueue) Add(msgs ...*ProducerMsg) {
	q.total += uint64(len(msgs))
	// new messages, set id
	for _, msg := range msgs {
		msg.ID = q.id
		q.id++
	}

	var toSave []*ProducerMsg

	// so long as we have ondisk message, we must save to disk
	// to preserve order
	if q.OndiskCount() > 0 {
		toSave = msgs
	} else {
		q.pending = append(q.pending, msgs...)
		if len(q.pending) > q.MaxPending {
			toSave = q.pending[q.MaxPending:]
			q.pending = q.pending[0:q.MaxPending]
		}
	}
	if len(toSave) > 0 {
		q.boltQueue.Add(toGenericSlice(toSave))
	}
	q.Unblock()
}

// AddFailed adds messages that have failed delivery for retry.
func (q *ProducerQueue) AddFailed(msgs ...*ProducerMsg) {
	q.total += uint64(len(msgs))
	// failed messages already have id set
	for _, msg := range msgs {
		heap.Push(q.failed, msg)
	}
	q.Unblock()
}

// Init restores pending messages from bolt.
func (q *ProducerQueue) Init() error {
	q.Ready, q.isReady = make(chan struct{}), false
	err := q.boltQueue.Init()
	if err != nil {
		return err
	}
	id, err := q.boltQueue.GetMeta(currentIDKey)
	if err == nil && len(id) > 0 {
		q.id, _ = strconv.ParseUint(string(id), 10, 64)
	}

	s, err := q.boltQueue.Get(int(q.MaxPending))
	q.pending = fromGenericSlice(s)
	// if everything in memory, restart id from 1
	if q.OndiskCount() == 0 {
		for i, msg := range q.pending {
			msg.ID = uint64(i + 1)
		}
		q.id = uint64(len(q.pending) + 1)
	}
	if len(q.pending) > 0 {
		q.isReady = true
		close(q.Ready)
	}
	q.total = q.OndiskCount() + uint64(len(q.pending))

	if err == nil {
		q.logger.Debug("Init producer queue",
			zap.Uint64("id", q.id),
			zap.Int("pending", len(q.pending)),
			zap.Uint64("ondisk", q.OndiskCount()))
	} else {
		q.logger.Error("Failed to init producer queue",
			zap.Error(err),
			zap.Uint64("id", q.id),
			zap.Int("pending", len(q.pending)),
			zap.Uint64("ondisk", q.OndiskCount()))
	}
	return err
}

// Close saves pending messages to bolt.
func (q *ProducerQueue) Close() error {
	q.logger.Debug("Close producer queue",
		zap.Uint64("id", q.id),
		zap.Int("pending", len(q.pending)),
		zap.Int("failed", q.failed.Len()),
		zap.Uint64("ondisk", q.OndiskCount()))

	if len(q.pending) > 0 {
		q.boltQueue.Add(toGenericSlice(q.pending))
	}
	if q.failed.Len() > 0 {
		msgs := make([]interface{}, q.failed.Len())
		for i, msg := range *q.failed {
			msgs[i] = msg
		}
		q.boltQueue.Add(msgs)
	}
	q.boltQueue.AddMeta(currentIDKey, []byte(strconv.FormatUint(q.id, 10)))
	return q.boltQueue.Close()
}
