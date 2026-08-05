// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"fmt"
	"sync"
)

// defaultAccessLogQueueSize matches the Python reference's default.
const defaultAccessLogQueueSize = 10000

// asyncEmitter hands access-log records to a writer goroutine so disk latency
// stays out of the request path, and reports what it drops.
//
// Writing synchronously puts the file system in the dispatch path: a slow or
// full volume shows up as slow RPCs, and rotation happens inline. Moving the
// write off-thread removes that, but only if the queue is bounded and the
// enqueue never blocks — an unbounded queue turns a stalled disk into an OOM,
// and a blocking send reintroduces exactly the latency the goroutine was
// meant to remove. Full therefore means drop.
//
// What makes dropping acceptable rather than silent corruption is that it is
// reported: the next record to get through carries dropped_records, so the
// loss is visible in the log itself rather than only in a metric nobody
// exports. A log that loses records without saying so is worse than a slow
// one, because a consumer cannot tell a quiet period from a lossy one.
//
// The trade is durability — with a synchronous writer, a record on disk means
// the call completed; here a crash loses whatever is still queued. That is
// why it is opt-in, and why it is the wrong default for audit.
type asyncEmitter struct {
	ch    chan map[string]any
	done  chan struct{}
	write func(map[string]any)

	mu      sync.Mutex
	dropped int64
	closed  bool
}

// newAsyncEmitter starts a writer goroutine draining a queue of queueSize
// records into write.
func newAsyncEmitter(queueSize int, write func(map[string]any)) (*asyncEmitter, error) {
	if queueSize <= 0 {
		return nil, fmt.Errorf("vgirpc: access-log queue size must be positive, got %d", queueSize)
	}
	a := &asyncEmitter{
		ch:    make(chan map[string]any, queueSize),
		done:  make(chan struct{}),
		write: write,
	}
	go func() {
		defer close(a.done)
		for record := range a.ch {
			a.write(record)
		}
	}()
	return a, nil
}

// enqueue hands record to the writer goroutine, or counts it as dropped when
// the queue is full. It never blocks.
func (a *asyncEmitter) enqueue(record map[string]any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	if a.dropped > 0 {
		// Attribute the loss to the first record that gets through after it,
		// so the count reaches the same file the lost records would have.
		record["dropped_records"] = a.dropped
	}
	select {
	case a.ch <- record:
		a.dropped = 0
	default:
		// Undo the stamp: this record is not the one that got through, and
		// the count must survive to ride the one that does.
		delete(record, "dropped_records")
		a.dropped++
	}
}

// close stops the writer goroutine after it has drained what is queued.
// Records enqueued afterwards are discarded.
func (a *asyncEmitter) close() {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	close(a.ch)
	a.mu.Unlock()
	<-a.done
}
