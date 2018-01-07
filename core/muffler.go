package core

import (
	"sync"
)

// Muffler is a thread-safe implementation of backpressure by ensuring the number of pending
// requests are within a threshold.
type Muffler struct {
	sync.Mutex
	maxPending int64
	pending    int64
	isReady    bool
	Ready      chan struct{}
}

// NewMuffler creates a new Muffler.
func NewMuffler(maxPending int64) *Muffler {
	m := &Muffler{maxPending: maxPending, isReady: true, Ready: make(chan struct{})}
	close(m.Ready)
	return m
}

// AddPending adds n pending requests
func (m *Muffler) AddPending(n int64) int64 {
	m.Lock()
	defer m.Unlock()
	m.pending += n
	if m.pending > m.maxPending && m.isReady {
		m.isReady = false
		m.Ready = make(chan struct{})
	}
	return m.pending
}

// RemovePending removes n pending requests
func (m *Muffler) RemovePending(n int64) int64 {
	m.Lock()
	defer m.Unlock()
	m.pending -= n
	if m.pending <= m.maxPending && !m.isReady {
		m.isReady = true
		close(m.Ready)
	}
	return m.pending
}

// Pending returns the current pending count
func (m *Muffler) Pending() int64 {
	m.Lock()
	defer m.Unlock()
	return m.pending
}
