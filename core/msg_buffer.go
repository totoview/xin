package core

import "sync"

/*
MsgBuffer buffers messages from upstream. It decouples message receiving
from message saving and makes batching more efficient. It uses a mutex internally for Add() and Get().
To minimize overhead of locking, add messages in batches.

The following snippet illustrates how it can be used to batch
incoming message processing:

	buf := NewMsgBuffer()

	// receive messages
	go func() {
		for {
			select {
			case msgs := <- incomingChannel:
				buf.Add(msgs.([]interface{}))
			}
		}
	}

	// process messages
	go func() {
		for {
			select {
			case <- buf.Ready:
				msgs := buf.Get()
				// ...
			}
		}
	}
*/
type MsgBuffer struct {
	sync.Mutex
	msgs    []interface{}
	isReady bool
	Ready   chan struct{}
}

// NewMsgBuffer creates a new buffer.
func NewMsgBuffer() *MsgBuffer {
	return &MsgBuffer{msgs: make([]interface{}, 0), isReady: false, Ready: make(chan struct{})}
}

// Add appends reqs to the end of buf.
func (buf *MsgBuffer) Add(reqs []interface{}) {
	buf.Lock()
	defer buf.Unlock()

	buf.msgs = append(buf.msgs, reqs...)

	if !buf.isReady {
		buf.isReady = true
		close(buf.Ready)
	}
}

// Get removes and returns all the messages in buf.
func (buf *MsgBuffer) Get() []interface{} {
	buf.Lock()
	defer buf.Unlock()

	msgs := buf.msgs
	buf.msgs = make([]interface{}, 0)

	if buf.isReady {
		buf.isReady = false
		buf.Ready = make(chan struct{})
	}
	return msgs
}
