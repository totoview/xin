package kafka

import "encoding/binary"

// ProducerMsg is message to be sent to Kafka.
type ProducerMsg struct {
	ID    uint64 // used internally for sorting
	Topic string
	Key   []byte
	Value []byte
}

// Marshal serializes message m into bytes.
func (m *ProducerMsg) Marshal() []byte {
	topic := []byte(m.Topic)
	key := []byte(m.Key)
	data := make([]byte, 4+len(topic)+len(key)+len(m.Value))

	binary.LittleEndian.PutUint16(data, uint16(len(topic)))
	copy(data[2:], topic)
	offset := 2 + len(topic)

	binary.LittleEndian.PutUint16(data[offset:], uint16(len(key)))
	copy(data[offset+2:], key)
	offset += 2 + len(key)

	copy(data[offset:], m.Value)
	return data
}

// Unmarshal deserializes message m from v.
func (m *ProducerMsg) Unmarshal(v []byte) {
	offset := 2
	len := binary.LittleEndian.Uint16(v)
	m.Topic = string(v[offset : offset+int(len)])
	offset += int(len)

	len = binary.LittleEndian.Uint16(v[offset:])
	offset += 2
	m.Key = v[offset : offset+int(len)]

	m.Value = v[offset+int(len):]
}

// to keep failed messages in order. in general, when there are errors,
// we can't guarantee the messages are sent in strict order, but we try our best
type msgHeap []*ProducerMsg

func (h msgHeap) Len() int           { return len(h) }
func (h msgHeap) Less(i, j int) bool { return h[i].ID < h[j].ID }
func (h msgHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *msgHeap) Push(x interface{}) {
	*h = append(*h, x.(*ProducerMsg))
}

func (h *msgHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
