package bench

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/spf13/cobra"
	"github.com/totoview/xin/core"
)

var (
	totalBoltMsgs   uint32
	boltDequeueSize int32
	boltCacheSize   int32
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

func init() {
	BoltQueueCmd.Flags().Uint32VarP(&totalBoltMsgs, "total", "", 500000, "total messages to write and read")
	BoltQueueCmd.Flags().Int32VarP(&boltCacheSize, "cachesize", "", 5000, "write cache size")
	BoltQueueCmd.Flags().Int32VarP(&boltDequeueSize, "dequeuesize", "", 5000, "number of messages to dequeue for each read")
}

var BoltQueueCmd = &cobra.Command{
	Use:   "boltqueue",
	Short: "Benchmark BoltQueue",
	Long: `Benchmark BoltQueue performance by writing messages to an empty queue and then
reading them back. Messages are written one by one, but only flushed to disk after the write
cache has been filled up. Reading is performed in batches.`,
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

		q, err := core.NewBoltQueue(db, "testBoltQueue", &codec{}, int(boltCacheSize))
		if err != nil {
			fmt.Printf("Failed to create db: %s\n", err.Error())
			return
		}

		if err := q.Init(); err != nil {
			fmt.Printf("Failed to init queue: %s\n", err.Error())
			return
		}
		defer q.Close()

		fmt.Printf("Writing %d messages (queue cache size: %d)\n", totalBoltMsgs, boltCacheSize)
		start := time.Now()
		for i := uint32(0); i < totalBoltMsgs; i++ {
			q.Add([]interface{}{&testMsg{uint64(i), "hello"}})
		}
		fmt.Printf("%d message written in %v (%.2f/s)\n", totalBoltMsgs, time.Now().Sub(start), float64(totalBoltMsgs)/time.Now().Sub(start).Seconds())

		// fmt.Printf("BoltDB stats: %#v\n", db.Stats())

		fmt.Printf("Reading %d messages (%d message per read)\n", totalBoltMsgs, boltDequeueSize)
		start, read := time.Now(), uint32(0)
		for read < totalBoltMsgs {
			msgs, err := q.Get(int(boltDequeueSize))
			if err != nil {
				fmt.Printf("Failed to read: %s\n", err.Error())
				return
			}
			read += uint32(len(msgs))
		}
		fmt.Printf("%d message read in %v (%.2f/s)\n", totalBoltMsgs, time.Now().Sub(start), float64(totalBoltMsgs)/time.Now().Sub(start).Seconds())

		// fmt.Printf("BoltDB stats: %#v\n", db.Stats())
	},
}
