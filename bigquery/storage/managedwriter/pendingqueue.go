// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package managedwriter

import (
	"container/list"
	"fmt"
	"io"
	"sync"
	"time"
)

var globalQueueId string = "GLOBAL_FIFO_QUEUE"

// pendingQueue is responsible for maintaining the queue of pendingWrites that have been sent
// and are awaiting acknowledgement.  The default behavior of an AppendRows connection is to
// respect global FIFO ordering, but for multiplex scenarios where writes are being interleaved
// the backend can respect per-destination ordering.
type pendingQueue struct {
	multiple  bool
	mu        sync.Mutex
	dests     map[string]*list.List
	waitingCh chan struct{}
	onceClose *sync.Once
	closed    bool
}

func newPendingQueue(allowMultiple bool, maxDepth int) *pendingQueue {
	return &pendingQueue{
		multiple:  allowMultiple,
		dests:     make(map[string]*list.List),
		waitingCh: make(chan struct{}, maxDepth),
		onceClose: &sync.Once{},
	}
}

// enqueue adds a pendingwrite to the queue.
func (pq *pendingQueue) enqueue(pw *pendingWrite) error {
	if pw == nil {
		return fmt.Errorf("won't enqueue nil writes")
	}
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if pq.closed {
		return fmt.Errorf("queue already closed")
	}
	dest := pw.writeStreamID
	if !pq.multiple {
		dest = globalQueueId
	}
	l, ok := pq.dests[dest]
	if !ok {
		// subqueue not yet present, create it.
		l = list.New()
		pq.dests[dest] = l
	}
	l.PushBack(pw)
	pq.waitingCh <- struct{}{}
	return nil
}

// close signals the queue is closed for enqueue, but can still be drained.
func (pq *pendingQueue) close() {
	pq.onceClose.Do(func() {
		pq.mu.Lock()
		defer pq.mu.Unlock()
		pq.closed = true
		close(pq.waitingCh)
	})
}

// msgWaiting provides a channel for monitoring that there are pending writes in flight, as
// we use a select-based process for dequeuing.
//
// Consumers are expected to call next to pop the appropriate message off the queue.  Failure
// to do so may cause consistency issues.
func (pq *pendingQueue) msgWaiting() <-chan struct{} {
	return pq.waitingCh
}

// listDests returns the currently available queues and the number of elements assigned to each.
func (pq *pendingQueue) listDests() map[string]int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	m := make(map[string]int)
	for k, l := range pq.dests {
		m[k] = l.Len()
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

// dequeue provides the next element in the given destination.
//
// if the pendingQueue is not configured to support multiple destinations, the next message is grabbed from the
// global queue regardless of the provided destination.
func (pq *pendingQueue) dequeue(destId string) (*pendingWrite, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if !pq.multiple {
		destId = globalQueueId
	}
	l, ok := pq.dests[destId]
	if !ok {
		return nil, fmt.Errorf("no message in queue %q", destId)
	}
	e := l.Front()
	l.Remove(e)
	if l.Len() == 0 {
		delete(pq.dests, destId)
	}
	return e.Value.(*pendingWrite), nil
}

// drain handles draining any queued pending writes.
//
// if cause is not provided, io.EOF is used.
func (pq *pendingQueue) drain(co *connection, cause error) {
	if cause == nil {
		cause = io.EOF
	}
	for {
		for d, ct := range pq.listDests() {
			for i := 0; i < ct; i++ {
				if pw, _ := pq.dequeue(d); pw != nil {
					if co != nil {
						co.release(pw)
					}
					if pw.writer == nil {
						// can't attribute a writer.  simply mark done.
						pw.markDone(nil, cause)
					} else {
						pw.writer.processRetry(pw, co, nil, cause)
					}
				}
			}
		}
		select {

		case _, ok := <-pq.msgWaiting():
			if !ok {
				return
			}
		case <-time.After(50 * time.Millisecond):
			continue
		}
	}

}
