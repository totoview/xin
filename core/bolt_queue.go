package core

import (
	"fmt"

	"github.com/boltdb/bolt"
)

// bucket name for meta data
func meta(bucket string) string { return fmt.Sprintf("%s.meta", bucket) }

// BoltQueueCodec handles application data serialisation for BoltQueue.
type BoltQueueCodec interface {
	// Encode serialises msg into key and data
	Encode(msg interface{}) (key []byte, data []byte)
	// Decode deserialises the original message from key and data
	Decode(key []byte, data []byte) (interface{}, error)
}

// BoltQueue implements a persistent message queue based on Bolt.
type BoltQueue struct {
	db        *bolt.DB       // the backing db
	bucket    string         // bucket name
	cacheSize int            // number of messages to keep in cache
	cache     []interface{}  // cache to improve batching performance
	total     uint64         // number of messages, including those in cache
	codec     BoltQueueCodec // message codec

	// stats
	TotalAdd uint64 // total number of messages added
	TotalGet uint64 // total number of messages removed
}

// NewBoltQueue creates a new instance of BoltQueue.
func NewBoltQueue(db *bolt.DB, bucket string, codec BoltQueueCodec, cacheSize int) (*BoltQueue, error) {
	if err := db.Update(func(tx *bolt.Tx) error {
		// bucket for messages
		if _, err := tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
			return err
		}
		// bucket for meta data
		_, err := tx.CreateBucketIfNotExists([]byte(meta(bucket)))
		return err
	}); err != nil {
		return nil, err
	}
	return &BoltQueue{db: db, bucket: bucket, cacheSize: cacheSize, codec: codec}, nil
}

// Count returns number of messages in queue.
func (q *BoltQueue) Count() uint64 {
	return q.total
}

// Add adds messages to queue.
func (q *BoltQueue) Add(msgs []interface{}) error {
	q.total += uint64(len(msgs))
	q.TotalAdd += uint64(len(msgs))
	q.cache = append(q.cache, msgs...)
	if len(q.cache) > q.cacheSize {
		return q.flush()
	}
	return nil
}

// Get removes messages from queue.
func (q *BoltQueue) Get(cnt int) ([]interface{}, error) {
	var msgs []interface{}
	if q.total == 0 {
		return msgs, nil
	}
	if err := q.flush(); err != nil {
		return nil, err
	}
	total := 0
	if err := q.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(q.bucket))
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			c.Delete()

			msg, err := q.codec.Decode(k, v)
			if err != nil {
				return err
			}
			msgs = append(msgs, msg)
			q.total--
			total++
			if total >= cnt {
				break
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	q.TotalGet += uint64(len(msgs))
	return msgs, nil
}

// AddMeta adds meta data
func (q *BoltQueue) AddMeta(key string, value []byte) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(meta(q.bucket)))
		return b.Put([]byte(key), value)
	})
}

// GetMeta gets meta data
func (q *BoltQueue) GetMeta(key string) (value []byte, err error) {
	err = q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(meta(q.bucket)))
		value = b.Get([]byte(key))
		return nil
	})
	return value, err
}

// persist messages in cache
func (q *BoltQueue) flush() (err error) {
	if len(q.cache) == 0 {
		return nil
	}
	err = q.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(q.bucket))
		for _, msg := range q.cache {
			k, v := q.codec.Encode(msg)
			if err := b.Put(k, v); err != nil {
				return err
			}
		}
		return nil
	})
	q.cache = q.cache[:0]
	return err
}

// Init restores pending messages from bolt.
func (q *BoltQueue) Init() error {
	return q.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(q.bucket))
		q.total = uint64(b.Stats().KeyN)
		return nil
	})
}

// Close prepares the queue for restart.
func (q *BoltQueue) Close() error {
	return q.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(q.bucket))
		for _, msg := range q.cache {
			k, v := q.codec.Encode(msg)
			if err := b.Put(k, v); err != nil {
				return err
			}
		}
		return nil
	})
}
