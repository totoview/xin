package store

import (
	"context"
	"sync"
	"time"

	"github.com/totoview/xin/core"
	"go.uber.org/zap"
)

const (
	minErrDelay = 100 * time.Millisecond
	maxErrDelay = 2 * time.Second
)

// Save messages to ElasticSearch
type storeNode struct {
	logger *zap.Logger
	store  core.Store

	msgs chan interface{}
	stop chan struct{}
	done chan struct{}
}

// NewStoreNode constructs a new message store Node.
func NewStoreNode(msgSource core.PushProducer, msgStore core.Store, logger *zap.Logger) (core.Node, error) {
	var err error
	sn := &storeNode{store: msgStore, logger: logger, msgs: make(chan interface{})}
	if err = msgSource.Subscribe(sn.msgs); err != nil {
		return nil, err
	}
	return sn, err
}

///////////////////////////////////////////////////////////////////////////////////////
// core.Runnable

func (sn *storeNode) Init(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
}

func (sn *storeNode) Start(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	sn.stop, sn.done = make(chan struct{}), make(chan struct{})

	var wgUpload sync.WaitGroup
	buf := core.NewMsgBuffer()

	// save message to store
	wgUpload.Add(1)
	go func() {
		defer wgUpload.Done()
		delay := minErrDelay
		retry := make(chan struct{})
		close(retry)
		var toSave []interface{}
		var failed []interface{}
		for {
			for len(toSave) > 0 {
				select {
				case <-sn.stop:
					return
				case <-retry:
					failed = []interface{}{}
					for i, total, batch := 0, len(toSave), 1000; i < total; i += batch {
						var msgs []interface{}
						if i+batch <= total {
							msgs = toSave[i:(i + batch)]
						} else {
							msgs = toSave[i:]
						}
						if err := sn.store.Save(msgs); err != nil {

							// FIXME: need sampler here, may fire too many errors
							sn.logger.Error("Failed to save messages",
								zap.Error(err),
								zap.Int("count", len(msgs)))

							// skip the rest whenever there's error as it's highly unlikely
							// that the remaining messages could be saved successfully now.
							// we break to let the delay logic to kick in.
							failed = append(failed, toSave[i:]...)
							break
						}
					}

					toSave = failed

					if len(failed) > 0 {
						delay *= 2
						if delay > maxErrDelay {
							delay = maxErrDelay
						}
						<-time.After(delay)
					} else {
						delay = minErrDelay
					}
				}
			}

			select {
			case <-sn.stop:
				return
			case <-buf.Ready:
				toSave = buf.Get()
			}
		}
	}()

	// receive message from source
	go func() {
		for {
			select {
			case <-sn.stop:
				wgUpload.Wait()
				sn.done <- struct{}{}
				return
			case msgs := <-sn.msgs:
				buf.Add(msgs.([]interface{}))
			}
		}
	}()
}

func (sn *storeNode) Stop(ctx context.Context, errc chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	close(sn.stop)
	<-sn.done
}
