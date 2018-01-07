package core_test

import (
	"math/rand"
	"testing"
	"time"

	. "github.com/totoview/xin/core"
)

func TestOffsetTracker(t *testing.T) {
	ot := NewOffsetTracker(10)

	if ot.Add(10) != 10 {
		t.Errorf("Failed to set init offset")
	}

	for i := int64(-1); i <= 10; i++ {
		if ot.Add(i) != 10 {
			t.Errorf("Failed to handle lower offset")
		}
	}

	for i := int64(11); i <= 20; i++ {
		if ot.Add(i) != i {
			t.Errorf("Failed to handle next offset")
		}
	}

	for i := int64(22); i <= 30; i += 2 {
		if ot.Add(i) != 20 {
			t.Errorf("fail to handle non consecutive offset")
		}
	}

	for i := int64(21); i <= 30; i += 2 {
		if ot.Add(i) != i+1 {
			t.Errorf("Failed to handle offset jumping: i=%v, offset=%v", i, ot.Offset())
		}
	}
}

// Test OffsetTracker performance by adding blocks of randomized offsets
// to the tracker. The ranges of the randomized blocks form a continous sequence.
// This matches the usage pattern of PullSourceNode producing blocks of messages
// with continuous offsets.
func benchmarkOffsetTracker(blockSize int, b *testing.B) {
	offsets := make([]int64, blockSize, blockSize)
	for i := 0; i < blockSize; i++ {
		offsets[i] = int64(i)
	}

	// in reality, offsets would be added with pretty strong ordering.
	// here we assume the order is completely random
	rand.Seed(time.Now().UnixNano())
	for i := blockSize - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		offsets[i], offsets[j] = offsets[j], offsets[i]
	}

	ot := NewOffsetTracker(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := int64(i * blockSize)
		for j := 0; j < blockSize; j++ {
			offsets[j] = offsets[j] + n
		}
		ot.AddAll(offsets)
	}
}

func BenchmarkOffsetTracker100(b *testing.B)  { benchmarkOffsetTracker(100, b) }
func BenchmarkOffsetTracker500(b *testing.B)  { benchmarkOffsetTracker(500, b) }
func BenchmarkOffsetTracker1000(b *testing.B) { benchmarkOffsetTracker(1000, b) }
func BenchmarkOffsetTracker2000(b *testing.B) { benchmarkOffsetTracker(2000, b) }
