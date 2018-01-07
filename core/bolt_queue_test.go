package core_test

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/totoview/xin/core"
)

type testMsg struct {
	ID   uint64 `json:"id"`
	Text string `json:"text"`
}

type codec struct{}

func (c *codec) Encode(msg interface{}) (key []byte, value []byte) {
	key = make([]byte, 8)
	binary.BigEndian.PutUint64(key, msg.(*testMsg).ID)
	value, _ = json.Marshal(msg)
	return key, value
}

func (c *codec) Decode(key, value []byte) (interface{}, error) {
	msg := &testMsg{}
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func TestBoltQueue(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "testBoltQueue")
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

	q, err := core.NewBoltQueue(db, "testBoltQueue", &codec{}, 100)
	if err != nil {
		t.Errorf("failed to create queue: %s", err.Error())
	}

	if err := q.Init(); err != nil {
		t.Errorf("failed to init queue: %s", err.Error())
	}

	if q.Count() != 0 {
		t.Errorf("invalid init state")
	}

	// read from empty queue
	for i := 0; i < 10; i++ {
		msgs, _ := q.Get(i)
		if len(msgs) != 0 {
			t.Errorf("unexpected result: %d", len(msgs))
		}
	}

	// write cnt messages one by one
	cnt := 1000
	for i := 0; i < cnt; i++ {
		if err := q.Add([]interface{}{&testMsg{uint64(i), fmt.Sprintf("msg#%d", i)}}); err != nil {
			t.Errorf("failed to add message: %s", err.Error())
		}
		if q.Count() != uint64(i+1) {
			t.Errorf("unexpected count: %d (expected %d)", q.Count(), i+1)
		}
	}

	read := 0
	for read < cnt {
		msgs, err := q.Get(100)
		if err != nil {
			t.Errorf("failed to get: %s", err.Error())
		}
		if len(msgs) != 100 {
			t.Errorf("unexpected result size: %d", len(msgs))
		}
		for _, msg := range msgs {
			m := msg.(*testMsg)
			if m.ID != uint64(read) || strings.Compare(m.Text, fmt.Sprintf("msg#%d", read)) != 0 {
				t.Errorf("unexpected message: i=%d ID=%d Text=%s", read, m.ID, m.Text)
			}
			read++
		}
		if q.Count() != uint64(cnt-read) {
			t.Errorf("invalid count")
		}
	}

	// meta
	value, err := q.GetMeta("test")
	if err != nil || len(value) != 0 {
		t.Errorf("unexpected value")
	}

	if err := q.AddMeta("test", []byte{0, 1, 2}); err != nil {
		t.Errorf("failed to add meta: %s", err.Error())
	}

	value, _ = q.GetMeta("test")
	if len(value) != 3 || value[0] != 0 || value[1] != 1 || value[2] != 2 {
		t.Errorf("unexpected meta value")
	}

	if err := q.Close(); err != nil {
		t.Errorf("failed to close: %s", err.Error())
	}
}
