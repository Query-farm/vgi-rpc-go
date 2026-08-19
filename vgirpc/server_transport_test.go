// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNotifyTransportRetriesAfterHookFailure(t *testing.T) {
	server := NewServer()
	var calls atomic.Int64
	server.SetServeStartHook(func(kind TransportKind, _ map[string]bool) error {
		if kind != TransportKindHTTP {
			t.Fatalf("hook kind = %q, want %q", kind, TransportKindHTTP)
		}
		if calls.Add(1) == 1 {
			return errors.New("transient startup failure")
		}
		return nil
	})

	if err := server.notifyTransport(TransportKindHTTP, nil); err == nil {
		t.Fatal("first notifyTransport succeeded, want injected failure")
	}
	if got := server.TransportKind(); got != "" {
		t.Fatalf("failed hook committed transport kind %q", got)
	}
	if err := server.notifyTransport(TransportKindHTTP, nil); err != nil {
		t.Fatalf("retry notifyTransport: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("hook calls = %d, want 2", got)
	}
	if got := server.TransportKind(); got != TransportKindHTTP {
		t.Fatalf("transport kind = %q, want %q", got, TransportKindHTTP)
	}
}

func TestNotifyTransportConcurrentFirstCallsAreSingleFlight(t *testing.T) {
	server := NewServer()
	var calls atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	server.SetServeStartHook(func(TransportKind, map[string]bool) error {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return nil
	})

	const goroutines = 32
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- server.notifyTransport(TransportKindHTTP, nil)
		}()
	}
	<-entered
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("notifyTransport: %v", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("concurrent hook calls = %d, want 1", got)
	}
}
