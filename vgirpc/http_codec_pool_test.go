// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"
	"weak"
)

type idleCodecFixture struct{ io.Writer }

func (*idleCodecFixture) Close() error { return nil }

func TestCodecPoolRetainsBoundedIdleWriters(t *testing.T) {
	for _, count := range []int{maxIdleCodecWriters - 1, maxIdleCodecWriters, maxIdleCodecWriters + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			pool := &codecWriterPool{idle: make(chan io.WriteCloser, maxIdleCodecWriters)}
			for range count {
				pool.put(&idleCodecFixture{io.Discard})
			}
			if got, want := len(pool.idle), min(count, maxIdleCodecWriters); got != want {
				t.Fatalf("retained %d idle writers, want %d", got, want)
			}
		})
	}
}

type failingCodecFixture struct {
	closes   int
	closeErr error
}

func (*failingCodecFixture) Write(data []byte) (int, error) { return len(data), nil }
func (w *failingCodecFixture) Close() error                 { w.closes++; return w.closeErr }

func TestCodecPoolCloseFailureAndDuplicateClose(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			pool := &codecWriterPool{idle: make(chan io.WriteCloser, maxIdleCodecWriters)}
			underlying := &failingCodecFixture{}
			if failed {
				underlying.closeErr = errors.New("write failed")
			}
			reset := 0
			writer := &pooledCodecWriter{WriteCloser: underlying, pool: pool, resetNil: func() { reset++ }}
			for range 2 {
				if e := writer.Close(); e != underlying.closeErr {
					t.Fatalf("close error: %v", e)
				}
			}
			if underlying.closes != 1 || reset != 1 {
				t.Fatalf("close/reset repeated: %d/%d", underlying.closes, reset)
			}
			want := 1
			if failed {
				want = 0
			}
			if len(pool.idle) != want {
				t.Fatalf("pooled a failed or duplicate writer: %d", len(pool.idle))
			}
		})
	}
}

type codecDestination struct{ _ [4096]byte }

func (*codecDestination) Write(data []byte) (int, error) { return len(data), nil }

func codecDestinationReference(t *testing.T, encoding string) weak.Pointer[codecDestination] {
	t.Helper()
	destination := &codecDestination{}
	reference := weak.Make(destination)
	writer, e := newCompressWriter(encoding, destination, DefaultCompressionLevel, -1)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := writer.Write([]byte("payload")); e != nil {
		t.Fatal(e)
	}
	if e := writer.Close(); e != nil {
		t.Fatal(e)
	}
	return reference
}

func TestCodecPoolDropsResponseDestination(t *testing.T) {
	for _, encoding := range []string{"zstd", "gzip"} {
		t.Run(encoding, func(t *testing.T) {
			reference := codecDestinationReference(t, encoding)
			runtime.GC()
			runtime.GC()
			if reference.Value() != nil {
				t.Fatal("pooled codec retains its response destination")
			}
		})
	}
}

func TestCodecPoolReusesAcrossGoroutinesAndGC(t *testing.T) {
	writer := &idleCodecFixture{io.Discard}
	pool := &codecWriterPool{idle: make(chan io.WriteCloser, maxIdleCodecWriters), newWriter: func() (io.WriteCloser, error) { return nil, fmt.Errorf("idle writer was not reused") }}
	pool.put(writer)
	// sync.Pool discards unused entries across two collections. The bounded
	// shared pool must preserve the reusable encoder without per-P ownership.
	runtime.GC()
	runtime.GC()
	for range 20 {
		done := make(chan error, 1)
		go func() {
			got, e := pool.get()
			if e == nil && got != writer {
				e = fmt.Errorf("unexpected writer")
			}
			if e == nil {
				pool.put(got)
			}
			done <- e
		}()
		if e := <-done; e != nil {
			t.Fatal(e)
		}
	}
}

func TestCodecPoolConcurrentRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("bounded reusable encoder"), 1024)
	for _, encoding := range []string{"zstd", "gzip"} {
		t.Run(encoding, func(t *testing.T) {
			var workers sync.WaitGroup
			for range maxIdleCodecWriters * 2 {
				workers.Go(func() {
					for range 10 {
						var output bytes.Buffer
						writer, e := newCompressWriter(encoding, &output, DefaultCompressionLevel, -1)
						if e != nil {
							t.Error(e)
							return
						}
						if _, e = writer.Write(payload); e != nil {
							t.Error(e)
						}
						if e = writer.Close(); e != nil {
							t.Error(e)
							return
						}
						decoded, e := decompressBounded(encoding, output.Bytes(), int64(len(payload)))
						if e != nil || !bytes.Equal(decoded, payload) {
							t.Errorf("round trip: %v", e)
							return
						}
					}
				})
			}
			workers.Wait()
		})
	}
}
