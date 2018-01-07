package kafka_test

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/go-kit/kit/log"
	"github.com/totoview/xin/source/kafka"
)

var nextKey = int64(0)

func newMessage() *kafka.ProducerMsg {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(nextKey))
	m := &kafka.ProducerMsg{Topic: "test", Key: key, Value: []byte{0, 1, 2}}
	nextKey++
	return m
}

func addMessages(q *kafka.ProducerQueue, cnt int) {
	for i := 0; i < cnt; i++ {
		q.Add(newMessage())
	}
}

func TestProducerQueue(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Errorf("failed to create db file: %s", err.Error())
	}
	fname := tmpfile.Name()
	tmpfile.Close()

	db, err := bolt.Open(fname, 0600, nil)
	if err != nil {
		t.Errorf("failed to open db: %s", err.Error())
	}
	defer func() {
		db.Close()
		os.Remove(fname)
	}()

	maxPending, batchSize := 4, 2
	q, err := kafka.NewProducerQueue(maxPending, db, log.NewNopLogger())
	if err != nil {
		t.Errorf("failed to create queue: %s", err.Error())
	}

	if err := q.Init(); err != nil {
		t.Errorf("failed to init queue: %s", err.Error())
	}

	if q.Count() != 0 || q.FailedCount() != 0 || q.PendingCount() != 0 || q.OndiskCount() != 0 || q.IsReady() {
		t.Errorf("invalid init state")
	}

	// make pending full
	addMessages(q, 4)
	if q.Count() != 4 || q.FailedCount() != 0 || q.PendingCount() != 4 || q.OndiskCount() != 0 || !q.IsReady() {
		t.Errorf("invalid state")
	}

	// these 8 should flow to db
	addMessages(q, 8)
	if q.Count() != 12 || q.FailedCount() != 0 || q.PendingCount() != 4 || q.OndiskCount() != 8 || !q.IsReady() {
		t.Errorf("invalid state")
	}

	// should get regular messages
	msgs, isNew := q.Get(batchSize)
	if q.Count() != 10 || q.PendingCount() != 2 || !isNew || len(msgs) != batchSize || msgs[0].ID != 1 || msgs[1].ID != 2 {
		t.Errorf("invalid state")
	}

	// get another 2, pending should be empty
	msgs, isNew = q.Get(batchSize)
	if q.Count() != 8 || q.PendingCount() != 0 || !isNew || len(msgs) != batchSize || msgs[0].ID != 3 || msgs[1].ID != 4 {
		t.Errorf("invalid state")
	}

	// add the last 2 as failed
	q.AddFailed(msgs...)
	if q.Count() != 10 || q.PendingCount() != 0 || q.OndiskCount() != 8 || q.FailedCount() != 2 || !q.IsReady() {
		t.Errorf("invalid state")
	}

	// these should get failed messages, one at a time
	msgs, isNew = q.Get(1)
	if q.Count() != 9 || q.PendingCount() != 0 || q.FailedCount() != 1 || isNew || len(msgs) != 1 || msgs[0].ID != 3 {
		t.Errorf("invalid state")
	}
	msgs, isNew = q.Get(1)
	if q.Count() != 8 || q.PendingCount() != 0 || q.FailedCount() != 0 || isNew || len(msgs) != 1 || msgs[0].ID != 4 {
		t.Errorf("invalid state")
	}

	// this will load batchSize+maxPending from disk, then return the first 2
	msgs, isNew = q.Get(batchSize)
	if q.Count() != 6 || q.PendingCount() != 4 || q.OndiskCount() != 2 || !isNew || len(msgs) != batchSize || msgs[0].ID != 5 || msgs[1].ID != 6 {
		t.Errorf("invalid state")
	}

	// close and reopen, should save the remaining 6 to db
	if err := q.Close(); err != nil {
		t.Errorf("failed to close: %s", err.Error())
	}

	maxPending, batchSize = 3, 3
	q, err = kafka.NewProducerQueue(maxPending, db, log.NewNopLogger())
	if err != nil {
		t.Errorf("failed to create queue: %s", err.Error())
	}

	if err := q.Init(); err != nil {
		t.Errorf("failed to init queue: %s", err.Error())
	}

	if q.Count() != 6 || q.FailedCount() != 0 || q.PendingCount() != 3 || q.OndiskCount() != 3 || !q.IsReady() {
		t.Errorf("invalid init state")
	}

	msg := newMessage()
	q.Add(msg)

	// this should be added to on-disk, it's the 13th message added
	if q.Count() != 7 || q.FailedCount() != 0 || q.PendingCount() != 3 || q.OndiskCount() != 4 || !q.IsReady() || msg.ID != 13 {
		t.Errorf("invalid init state")
	}

	// get the 3 from memory
	msgs, isNew = q.Get(batchSize)
	if q.Count() != 4 || q.PendingCount() != 0 || q.OndiskCount() != 4 || !isNew || len(msgs) != 3 || msgs[0].ID != 7 || msgs[1].ID != 8 || msgs[2].ID != 9 {
		t.Errorf("invalid state")
	}

	// get another 3, this load all the remaining 4 into memory
	msgs, isNew = q.Get(batchSize)
	if q.Count() != 1 || q.PendingCount() != 1 || q.OndiskCount() != 0 || !isNew || len(msgs) != 3 || msgs[0].ID != 10 || msgs[1].ID != 11 || msgs[2].ID != 12 {
		t.Errorf("invalid state")
	}

	// close and reopen, should save the remaining 1 to db
	if err := q.Close(); err != nil {
		t.Errorf("failed to close: %s", err.Error())
	}

	q, err = kafka.NewProducerQueue(maxPending, db, log.NewNopLogger())
	if err != nil {
		t.Errorf("failed to create queue: %s", err.Error())
	}

	if err := q.Init(); err != nil {
		t.Errorf("failed to init queue: %s", err.Error())
	}

	if q.Count() != 1 || q.FailedCount() != 0 || q.PendingCount() != 1 || q.OndiskCount() != 0 || !q.IsReady() {
		t.Errorf("invalid init state")
	}

	// get the last message, its ID should have been reset since we loaded all messages into memory on Init
	msgs, isNew = q.Get(batchSize)
	if q.Count() != 0 || q.PendingCount() != 0 || q.OndiskCount() != 0 || !isNew || len(msgs) != 1 {
		t.Errorf("invalid state")
	}

	if msgs[0].ID != 1 || q.IsReady() {
		t.Errorf("invalid state")
	}
}
