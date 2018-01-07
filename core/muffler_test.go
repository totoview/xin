package core_test

import (
	"sync"
	"testing"
	"time"

	. "github.com/totoview/xin/core"
)

func TestMuffler(t *testing.T) {

	m := NewMuffler(1500)
	m.AddPending(2000)

	readyTs := make(chan time.Time)
	next := make(chan struct{})
	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-m.Ready:
				readyTs <- time.Now()
				<-next
			}
		}
	}()

	for i := 0; i < 100; i++ {
		now := time.Now()
		<-time.After(1 * time.Millisecond)
		m.RemovePending(1000)
		ready := <-readyTs
		// should be at least 1ms
		if ready.Sub(now) < 1*time.Millisecond {
			t.Errorf("too early")
		}

		next <- struct{}{}
		ready2 := <-readyTs
		if ready2.Sub(ready) > 100*time.Microsecond {
			t.Errorf("too late: i=%d delay=%v", i, ready2.Sub(ready))
		}

		next <- struct{}{}
		ready3 := <-readyTs

		if ready3.Sub(ready2) > 100*time.Microsecond {
			t.Errorf("too late: i=%d delay=%v", i, ready3.Sub(ready2))
		}

		m.AddPending(1000)
		next <- struct{}{}
	}

	done <- struct{}{}
	wg.Wait()
}

func BenchmarkMuffler(b *testing.B) {
	m := NewMuffler(1000)
	for i := 0; i < b.N; i++ {
		m.AddPending(2000)
		m.RemovePending(2000)
	}
}
