package core_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	. "github.com/totoview/xin/core"
)

var order = 0

type testBaseNode struct {
	id               int
	order            int
	withInitErr      bool
	withInitTimeout  bool
	withStartErr     bool
	withStartTimeout bool
	withStopErr      bool
	withStopTimeout  bool
}

func (n *testBaseNode) Init(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	n.order = order
	order++
	if n.withInitErr {
		errc <- errors.Errorf("init#%d", n.id)
	}
	go func() {
		defer wg.Done()
		if n.withInitTimeout {
			select {
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()
}

func (n *testBaseNode) Start(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	n.order = order
	order++
	if n.withStartErr {
		errc <- errors.Errorf("start#%d", n.id)
	}
	go func() {
		defer wg.Done()
		if n.withStartTimeout {
			select {
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()
}

func (n *testBaseNode) Stop(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	n.order = order
	order++
	if n.withStopErr {
		errc <- errors.Errorf("stop#%d", n.id)
	}
	go func() {
		defer wg.Done()
		if n.withStopTimeout {
			select {
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()
}

type testSrcNode struct {
	testBaseNode
}

func setup() (*Pipeline, *testBaseNode, *testBaseNode, *testBaseNode, *testBaseNode, *testBaseNode, *testBaseNode) {
	order = 0
	p := NewPipeline("test", log.NewNopLogger())

	sn := &testBaseNode{}
	sn.id = 0
	n1 := &testBaseNode{id: 1}
	n2 := &testBaseNode{id: 2}
	n3 := &testBaseNode{id: 3}
	n4 := &testBaseNode{id: 4}
	n5 := &testBaseNode{id: 5}

	p.AddSourceNode(sn)
	p.AddChildNode(n1, sn)
	p.AddChildNode(n2, sn)
	p.AddChildNode(n3, n1)
	p.AddChildNode(n4, n2)
	p.AddService(n5)

	return p, sn, n1, n2, n3, n4, n5
}

func TestAddNode(t *testing.T) {
	p := NewPipeline("test", log.NewNopLogger())
	sn := &testSrcNode{}
	sn.id = 0
	// add new source node
	if err := p.AddSourceNode(sn); err != nil {
		t.Errorf("Failed to add new source node: %s", err.Error())
	}
	// add the same source node again
	if p.AddSourceNode(sn) == nil {
		t.Errorf("Failed to detect duplicate source node")
	}

	n1 := &testBaseNode{id: 1}
	n2 := &testBaseNode{id: 2}

	// add n1 with invalid parent node
	if p.AddChildNode(n1, n2) == nil {
		t.Errorf("Failed to detect invalid parent node")
	}
	// add n1 with proper parent node
	if err := p.AddChildNode(n1, sn); err != nil {
		t.Errorf("Failed to add valid child node: %v", err)
	}
	// add n1 again
	if p.AddChildNode(n1, sn) == nil {
		t.Errorf("Failed to detect duplicate child node")
	}
	// add n2 as n1's child
	if err := p.AddChildNode(n2, n1); err != nil {
		t.Errorf("Failed to add valid grand child node: %v", err)
	}
}

func TestInit(t *testing.T) {
	p, sn, n1, n2, n3, n4, n5 := setup()

	if err := p.Init(100); err != nil {
		t.Errorf("Failed to init normally: %s", err.Error())
	}

	// n5 must be init'ed before n3, n4
	if n5.order >= n3.order || n5.order >= n4.order {
		t.Errorf("Failed to init n5 before n3,n4")
	}

	// n3 must be init'ed before n1, n2
	if n3.order >= n1.order || n3.order >= n2.order {
		t.Errorf("Failed to init n3 before n1,n2")
	}

	// n4 must be init'ed before n1, n2
	if n4.order >= n1.order || n4.order >= n2.order {
		t.Errorf("Failed to init n4 before n1,n2")
	}

	// n1,n2 must be init'ed before sn
	if n1.order >= sn.order || n2.order >= sn.order {
		t.Errorf("Failed to init n1,n2 before sn")
	}
}

func TestInitErr(t *testing.T) {
	for i := 0; i < 6; i++ {
		p, sn, n1, n2, n3, n4, n5 := setup()
		ns := []*testBaseNode{sn, n1, n2, n3, n4, n5}
		ns[i].withInitErr = true

		if err := p.Init(100); err == nil || err.Error() != fmt.Sprintf("init#%d", i) {
			t.Errorf("Failed to capture init error for sn")
		}
	}
}

func TestInitTimeout(t *testing.T) {
	for i := 0; i < 6; i++ {
		p, sn, n1, n2, n3, n4, n5 := setup()
		ns := []*testBaseNode{sn, n1, n2, n3, n4, n5}
		ns[i].withInitTimeout = true

		if err := p.Init(100); err == nil || err != ErrTimeout {
			t.Errorf("Failed to capture init timeout for node %d", i)
		}
	}
}

func TestStart(t *testing.T) {
	p, sn, n1, n2, n3, n4, n5 := setup()

	p.Init(100)

	if err := p.Start(100); err != nil {
		t.Errorf("Failed to init normally: %s", err.Error())
	}

	// n5 must be started before n3, n4
	if n5.order >= n3.order || n5.order >= n4.order {
		t.Errorf("Failed to start n5 before n3,n4")
	}

	// n3 must be started before n1, n2
	if n3.order >= n1.order || n3.order >= n2.order {
		t.Errorf("Failed to start n3 before n1,n2")
	}

	// n4 must be started before n1, n2
	if n4.order >= n1.order || n4.order >= n2.order {
		t.Errorf("Failed to start n4 before n1,n2")
	}

	// n1,n2 must be started before sn
	if n1.order >= sn.order || n2.order >= sn.order {
		t.Errorf("Failed to start n1,n2 before sn")
	}
}

func TestStartErr(t *testing.T) {
	for i := 0; i < 6; i++ {
		p, sn, n1, n2, n3, n4, n5 := setup()
		ns := []*testBaseNode{sn, n1, n2, n3, n4, n5}
		ns[i].withStartErr = true

		p.Init(100)

		if err := p.Start(100); err == nil || err.Error() != fmt.Sprintf("start#%d", i) {
			t.Errorf("Failed to capture start error for node %d: err=%s", i, err.Error())
		}
	}
}

func TestStartTimeout(t *testing.T) {
	for i := 0; i < 6; i++ {
		p, sn, n1, n2, n3, n4, n5 := setup()
		ns := []*testBaseNode{sn, n1, n2, n3, n4, n5}
		ns[i].withStartTimeout = true

		p.Init(100)

		if err := p.Start(100); err == nil || err != ErrTimeout {
			t.Errorf("Failed to capture start timeout for node %d", i)
		}
	}
}

func TestStop(t *testing.T) {
	p, sn, n1, n2, n3, n4, n5 := setup()

	p.Init(100)
	p.Start(100)

	if err := p.Stop(100); err != nil {
		t.Errorf("Failed to init normally: %s", err.Error())
	}

	// n5 must be stopped after n3, n4
	if n5.order <= n3.order || n5.order <= n4.order {
		t.Errorf("Failed to stop n5 after n3,n4")
	}

	// n3 must be stopped after n1, n2
	if n3.order <= n1.order || n3.order <= n2.order {
		t.Errorf("Failed to stop n3 after n1,n2")
	}

	// n4 must be stopped after n1, n2
	if n4.order <= n1.order || n4.order <= n2.order {
		t.Errorf("Failed to stop n4 after n1,n2")
	}

	// n1,n2 must be stopped after sn
	if n1.order <= sn.order || n2.order <= sn.order {
		t.Errorf("Failed to stop n1,n2 after sn")
	}
}

func TestStopErr(t *testing.T) {
	for i := 0; i < 6; i++ {
		p, sn, n1, n2, n3, n4, n5 := setup()
		ns := []*testBaseNode{sn, n1, n2, n3, n4, n5}
		ns[i].withStopErr = true

		p.Init(100)
		p.Start(100)

		if err := p.Stop(100); err == nil || err.Error() != fmt.Sprintf("stop#%d", i) {
			t.Errorf("Failed to capture stop error for node %d: err=%s", i, err.Error())
		}
	}
}

func TestStopTimeout(t *testing.T) {
	for i := 0; i < 6; i++ {
		p, sn, n1, n2, n3, n4, n5 := setup()
		ns := []*testBaseNode{sn, n1, n2, n3, n4, n5}
		ns[i].withStopTimeout = true

		p.Init(100)
		p.Start(100)

		if err := p.Stop(100); err == nil || err != ErrTimeout {
			t.Errorf("Failed to capture stop timeout for node %d: err=%s", i, err.Error())
		}
	}
}

type msg struct {
	offset int64
	msg    string
}
type bmSrcNode struct {
	batchSize int
	offset    int64

	stop chan struct{}
	done chan struct{}
}

func (n *bmSrcNode) Init(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	n.stop, n.done = make(chan struct{}), make(chan struct{})
}

func (n *bmSrcNode) Start(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	go func() {
		for {
			select {
			case <-n.stop:
				n.done <- struct{}{}
				return
			}
		}
	}()
}

func (n *bmSrcNode) Stop(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	n.stop <- struct{}{}
	<-n.done
}

func (n *bmSrcNode) Recv(max int, tmout time.Duration) []interface{} {
	msgs := make([]interface{}, n.batchSize, n.batchSize)
	for i := 0; i < n.batchSize; i++ {
		msgs[i] = &msg{offset: n.offset, msg: "hello"}
		n.offset++
	}
	return msgs
}

func newBmSrcNode(batchSize int) *bmSrcNode {
	return &bmSrcNode{batchSize: batchSize}
}

type bmDispatchNode struct {
	BasicPushProducer
	src  PullSourceNode
	pull chan struct{}
	stop chan struct{}
	done sync.WaitGroup
}

func (n *bmDispatchNode) Init(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	n.pull, n.stop = make(chan struct{}), make(chan struct{})
}

func (n *bmDispatchNode) Start(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	subsCnt := len(n.Subs)
	n.done.Add(1)
	go func() {
		defer n.done.Done()
		for {
			select {
			case <-n.pull:
				batches := make([][]interface{}, subsCnt, subsCnt)
				for _, req := range n.src.Recv(10000, 1000) {
					// distribute requests to subscribers based on offset
					m := req.(*msg)
					i := int(m.offset) % subsCnt
					batches[i] = append(batches[i], m)
				}
				for i := 0; i < subsCnt; i++ {
					if len(batches[i]) > 0 {
						n.Subs[i] <- batches[i]
					}
				}
			case <-n.stop:
				return
			}
		}
	}()
}

func (n *bmDispatchNode) Stop(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	n.stop <- struct{}{}
	n.done.Wait()
}

func (n *bmDispatchNode) Pull() {
	n.pull <- struct{}{}
}

type bmProcNode struct {
	BasicPushProducer
	reqc chan interface{}
	stop chan struct{}
	done sync.WaitGroup
}

func (n *bmProcNode) Init(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
}

func (n *bmProcNode) Start(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	n.done.Add(1)
	go func() {
		defer n.done.Done()
		for {
			select {
			case <-n.stop:
				return
			case ms := <-n.reqc:
				for _, ch := range n.Subs {
					ch <- ms
				}
			}
		}
	}()
}

func (n *bmProcNode) Stop(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	n.stop <- struct{}{}
	n.done.Wait()
}

func newBmProcNode(p PushProducer) *bmProcNode {
	n := &bmProcNode{reqc: make(chan interface{})}
	n.stop = make(chan struct{})
	p.Subscribe(n.reqc)
	return n
}

type bmSinkNode struct {
	update chan<- []int64
	reqc   chan interface{}

	stop chan struct{}
	done sync.WaitGroup
}

func (n *bmSinkNode) Init(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	n.stop = make(chan struct{})
}

func (n *bmSinkNode) Start(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	n.done.Add(1)
	go func() {
		defer n.done.Done()
		for {
			select {
			case <-n.stop:
				return
			case ms := <-n.reqc:
				cnt := len(ms.([]interface{}))
				offsets := make([]int64, cnt, cnt)
				for i, m := range ms.([]interface{}) {
					offsets[i] = m.(*msg).offset
				}
				n.update <- offsets
			}
		}
	}()
}

func (n *bmSinkNode) Stop(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	n.stop <- struct{}{}
	n.done.Wait()
}

func newBmSinkNode(p PushProducer, update chan<- []int64) *bmSinkNode {
	n := &bmSinkNode{}
	n.update = update
	n.reqc = make(chan interface{})
	p.Subscribe(n.reqc)
	return n
}

// benchmark a pipeline with a single PullSourceNode that produces requests in
// batchSize blocks. A dispatcher distributes the requests to width sub-pipelines
// that have depth sequential nodes each. so the actual depth of the pipeline
// is depth + 2
func BenchPipeline(srcNode PullSourceNode, width, depth, batchSize int, b *testing.B) {

	// source
	pipeline := NewPipeline("test", log.NewNopLogger())
	pipeline.AddSourceNode(srcNode)

	// dispatcher
	disp := &bmDispatchNode{src: srcNode}
	pipeline.AddChildNode(disp, srcNode)

	// offset tracker
	var wg sync.WaitGroup
	wg.Add(1)
	ot := NewOffsetTrackingNode(0, func(offset int64) {
		if offset >= int64(batchSize*b.N-1) {
			wg.Done()
		}
	})
	pipeline.AddService(ot)

	// processors
	for i := 0; i < width; i++ {
		parent := PushProducer(disp)
		for j := 0; j < depth-1; j++ {
			proc := newBmProcNode(parent)
			pipeline.AddChildNode(proc, parent.(Node))
			parent = proc
		}
		sink := newBmSinkNode(parent, ot.UpdateChannel())
		pipeline.AddChildNode(sink, parent.(Node))
	}

	pipeline.Init(100)

	b.ResetTimer()
	pipeline.Start(100)
	for i := 0; i < b.N; i++ {
		disp.Pull()
	}
	wg.Wait()
	pipeline.Stop(100)
}

func BenchmarkPipeline_1_1_100(b *testing.B)  { BenchPipeline(newBmSrcNode(100), 1, 1, 100, b) }
func BenchmarkPipeline_1_1_200(b *testing.B)  { BenchPipeline(newBmSrcNode(200), 1, 1, 200, b) }
func BenchmarkPipeline_1_1_500(b *testing.B)  { BenchPipeline(newBmSrcNode(500), 1, 1, 500, b) }
func BenchmarkPipeline_1_1_1000(b *testing.B) { BenchPipeline(newBmSrcNode(1000), 1, 1, 1000, b) }

func BenchmarkPipeline_1_4_100(b *testing.B)  { BenchPipeline(newBmSrcNode(100), 1, 4, 100, b) }
func BenchmarkPipeline_1_4_200(b *testing.B)  { BenchPipeline(newBmSrcNode(200), 1, 4, 200, b) }
func BenchmarkPipeline_1_4_500(b *testing.B)  { BenchPipeline(newBmSrcNode(500), 1, 4, 500, b) }
func BenchmarkPipeline_1_4_1000(b *testing.B) { BenchPipeline(newBmSrcNode(1000), 1, 4, 1000, b) }

func BenchmarkPipeline_4_1_100(b *testing.B)  { BenchPipeline(newBmSrcNode(100), 4, 1, 100, b) }
func BenchmarkPipeline_4_1_200(b *testing.B)  { BenchPipeline(newBmSrcNode(200), 4, 1, 200, b) }
func BenchmarkPipeline_4_1_500(b *testing.B)  { BenchPipeline(newBmSrcNode(500), 4, 1, 500, b) }
func BenchmarkPipeline_4_1_1000(b *testing.B) { BenchPipeline(newBmSrcNode(1000), 4, 1, 1000, b) }

func BenchmarkPipeline_4_4_100(b *testing.B)  { BenchPipeline(newBmSrcNode(100), 4, 4, 100, b) }
func BenchmarkPipeline_4_4_200(b *testing.B)  { BenchPipeline(newBmSrcNode(200), 4, 4, 200, b) }
func BenchmarkPipeline_4_4_500(b *testing.B)  { BenchPipeline(newBmSrcNode(500), 4, 4, 500, b) }
func BenchmarkPipeline_4_4_1000(b *testing.B) { BenchPipeline(newBmSrcNode(1000), 4, 4, 1000, b) }
