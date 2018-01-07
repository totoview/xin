package core

//go:generate stringer -type=PipelineStat

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// for Pipeline internal use
type pnode struct {
	node     Node
	children []*pnode
}

func (p *pnode) find(n Node) *pnode {
	if n == p.node {
		return p
	}
	for _, c := range p.children {
		if r := c.find(n); r != nil {
			return r
		}
	}
	return nil
}

// PipelineStat is pipeline status
type PipelineStat uint8

const (
	PipelineUnint PipelineStat = iota
	PipelineInited
	PipelineStarted
	PipelineStopped
)

// Pipeline implements staged event processing
type Pipeline struct {
	Name     string
	logger   *zap.Logger
	Stat     PipelineStat
	sources  []*pnode
	services []*pnode
}

// NewPipeline creats a new pipeline.
func NewPipeline(name string, logger *zap.Logger) *Pipeline {
	return &Pipeline{Name: name, Stat: PipelineUnint, logger: logger.With(zap.String("comp", "pipeline"))}
}

// collect all nodes in breadth-first order
func (p *Pipeline) collect() []*pnode {
	nodes, rest := []*pnode{}, p.sources[:]

	for len(rest) > 0 {
		n := len(rest)
		nodes = append(nodes, rest[0:n]...)
		for _, node := range rest {
			if len(node.children) > 0 {
				rest = append(rest, node.children...)
			}
		}
		rest = rest[n:]
	}

	return append(nodes, p.services...)
}

// find a matching node
func (p *Pipeline) find(n Node) *pnode {
	for _, pn := range p.sources {
		if r := pn.find(n); r != nil {
			return r
		}
	}
	return nil
}

// Init prepares a pipeline for running
func (p *Pipeline) forEach(tmoutMillis int64, reverse bool, cb func(*pnode, context.Context, chan<- error, *sync.WaitGroup)) error {
	var (
		nodes = p.collect()
		cnt   = len(nodes)
	)

	if cnt == 0 {
		return nil
	}

	if reverse {
		for i := cnt/2 - 1; i >= 0; i-- {
			j := cnt - 1 - i
			nodes[i], nodes[j] = nodes[j], nodes[i]
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(tmoutMillis)*time.Millisecond)
	defer cancel()

	var (
		err  error
		done sync.WaitGroup
		errc = make(chan error)
	)

	// check for error or timeout
	done.Add(1)
	go func() {
		defer done.Done()
		select {
		case e := <-errc:
			err = e
		case <-ctx.Done():
			err = ErrTimeout
		}
	}()

	// apply callback to every node
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, n := range nodes {
		cb(n, ctx, errc, &wg)
	}
	wg.Wait()

	close(errc)
	done.Wait()

	return err
}

// AddService adds a service node to p
func (p *Pipeline) AddService(n Node) error {
	p.services = append(p.services, &pnode{node: n})
	return nil
}

// AddSourceNode adds node to pipeline.
func (p *Pipeline) AddSourceNode(n SourceNode) error {
	if p.find(n) != nil {
		return errors.New("Existing source node")
	}
	p.sources = append(p.sources, &pnode{node: n})
	return nil
}

// AddChildNode adds child and its parent to p, where parent
// must be an existing node in p.
func (p *Pipeline) AddChildNode(child Node, parent Node) error {
	if pn := p.find(child); pn != nil {
		return errors.New("Existing child node")
	}
	if pn := p.find(parent); pn != nil {
		pn.children = append(pn.children, &pnode{node: child})
		return nil
	}
	return errors.New("Parent node not found")
}

//////////////////////////////////////////////////////////////////////
// Service interface

// Init prepares a pipeline for running
func (p *Pipeline) Init(tmoutMillis int64) error {
	if p.Stat != PipelineUnint {
		return errors.Errorf("Invalid state: %s", p.Stat.String())
	}
	err := p.forEach(tmoutMillis, true, func(n *pnode, ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
		n.node.Init(ctx, errc, wg)
	})
	if err == nil {
		p.Stat = PipelineInited
	}
	return err
}

// Start runs a pipeline after initialization.
func (p *Pipeline) Start(tmoutMillis int64) error {
	if p.Stat != PipelineInited {
		return errors.Errorf("Invalid state: %s", p.Stat.String())
	}
	err := p.forEach(tmoutMillis, true, func(n *pnode, ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
		n.node.Start(ctx, errc, wg)
	})
	if err == nil {
		p.Stat = PipelineStarted
		p.logger.Debug("Start pipeline", zap.String("name", p.Name))
	} else {
		p.logger.Error("Failed to start pipeline", zap.String("name", p.Name), zap.Error(err))
	}
	return err
}

// Stop stops a pipeline.
func (p *Pipeline) Stop(tmoutMillis int64) error {
	if p.Stat != PipelineStarted {
		return errors.Errorf("Invalid state: %s", p.Stat.String())
	}
	err := p.forEach(tmoutMillis, false, func(n *pnode, ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
		n.node.Stop(ctx, errc, wg)
	})
	if err == nil {
		p.Stat = PipelineStopped
		p.logger.Debug("Stop pipeline", zap.String("name", p.Name))
	} else {
		p.logger.Error("Failed to stop pipeline", zap.String("name", p.Name), zap.Error(err))
	}
	return err
}
