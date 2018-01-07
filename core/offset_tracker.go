package core

import (
	"container/heap"
)

type offsetHeap []int64

func (h offsetHeap) Len() int           { return len(h) }
func (h offsetHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h offsetHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *offsetHeap) Push(x interface{}) {
	*h = append(*h, x.(int64))
}

func (h *offsetHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// OffsetTracker keeps track of the maximum offset before which all the previous
// requests have been confirmed. For instance, given the starting offset of 100,
// if requests for 101, 102, 104, 105, 110 have been confirmed, the maximum would
// be 102. If later on 103, 111 have also been confirmed, the maximum would become 105.
type OffsetTracker struct {
	offset int64
	h      *offsetHeap
}

// NewOffsetTracker creates a new OffsetTracker instance.
func NewOffsetTracker(initOffset int64) *OffsetTracker {
	p := &OffsetTracker{offset: initOffset, h: &offsetHeap{}}
	heap.Init(p.h)
	return p
}

// Offset returns the current offset
func (p *OffsetTracker) Offset() int64 {
	return p.offset
}

// SetOffset sets the current offset
func (p *OffsetTracker) SetOffset(offset int64) {
	p.offset = offset
}

// Add inserts a new offset and returns the current offset.
func (p *OffsetTracker) Add(n int64) int64 {
	if n == p.offset+1 {
		p.offset++
		for p.h.Len() > 0 {
			n2 := heap.Pop(p.h).(int64)
			if n2 == p.offset+1 {
				p.offset++
			} else {
				heap.Push(p.h, n2)
				break
			}
		}
	} else if n > p.offset {
		heap.Push(p.h, n)
	}
	return p.offset
}

// AddAll adds an array of new offsets.
func (p *OffsetTracker) AddAll(ns []int64) int64 {
	for _, n := range ns {
		p.Add(n)
	}
	return p.offset
}
