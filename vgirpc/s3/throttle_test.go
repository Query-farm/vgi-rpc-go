// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package vgis3

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Throttle tests against a fake S3 endpoint (plain http, path-style): a
// slow transfer that keeps moving succeeds in one attempt; one that stops
// moving fails within the stall timeout per attempt plus slack.

const (
	throttleStall = time.Second
	throttleRate  = 8 << 20  // bytes per second
	throttleBody  = 32 << 20 // 4 s at throttleRate: 4x the stall timeout
)

func pace(start time.Time, n int) {
	if ahead := time.Duration(float64(n)/throttleRate*float64(time.Second)) - time.Since(start); ahead > 0 {
		time.Sleep(ahead)
	}
}

type fakeS3 struct {
	puts, gets atomic.Int64
	received   atomic.Int64
	stall      bool // stop moving bytes after the first 256 KiB
	done       chan struct{}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		f.puts.Add(1)
		start, total, buf := time.Now(), 0, make([]byte, 16<<10)
		for {
			if f.stall && total >= 256<<10 {
				f.wait(r)
				return
			}
			n, err := r.Body.Read(buf)
			total += n
			pace(start, total)
			if err != nil {
				break
			}
		}
		f.received.Store(int64(total))
		w.Header().Set("ETag", `"fake"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		f.gets.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(throttleBody))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		start, chunk := time.Now(), bytes.Repeat([]byte("g"), 16<<10)
		for sent := 0; sent < throttleBody; sent += len(chunk) {
			if f.stall && sent >= 256<<10 {
				w.(http.Flusher).Flush()
				f.wait(r)
				return
			}
			if _, err := w.Write(chunk); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			pace(start, sent+len(chunk))
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (f *fakeS3) wait(r *http.Request) {
	select {
	case <-f.done:
	case <-r.Context().Done():
	}
}

func newThrottledS3(t *testing.T, stall bool) (*fakeS3, *S3Storage) {
	t.Helper()
	fake := &fakeS3{stall: stall, done: make(chan struct{})}
	lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
		var serr error
		if err := c.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 64<<10)
		}); err != nil {
			return err
		}
		return serr
	}}
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(fake)
	srv.Listener = ln
	srv.Start()
	t.Cleanup(func() {
		close(fake.done)
		srv.CloseClientConnections()
		srv.Close()
	})
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	storage, err := NewS3Storage("bucket", S3Config{
		EndpointURL: srv.URL, AccessKeyID: "test", SecretAccessKey: "test", StallTimeout: throttleStall,
	})
	if err != nil {
		t.Fatal(err)
	}
	return fake, storage
}

func TestSlowPutObjectSucceedsInOneAttempt(t *testing.T) {
	fake, storage := newThrottledS3(t, false)
	start := time.Now()
	if _, err := storage.PutObject(bytes.Repeat([]byte("p"), throttleBody)); err != nil {
		t.Fatalf("slow PutObject failed after %s: %v", time.Since(start), err)
	}
	elapsed := time.Since(start)
	if fake.puts.Load() != 1 || fake.received.Load() != throttleBody {
		t.Fatalf("puts=%d received=%d, want 1 PUT of %d bytes", fake.puts.Load(), fake.received.Load(), throttleBody)
	}
	if elapsed < 3*throttleStall {
		t.Fatalf("PutObject took %s; the throttle did not hold it past the stall timeout", elapsed)
	}
	t.Logf("PutObject of %d bytes: %s, 1 attempt (stall timeout %s)", throttleBody, elapsed, throttleStall)
}

func TestSlowGetSucceedsInOneAttempt(t *testing.T) {
	fake, storage := newThrottledS3(t, false)
	start := time.Now()
	data, err := storage.Get("vgi-rpc/key.bin", 1<<30)
	if err != nil {
		t.Fatalf("slow Get failed after %s: %v", time.Since(start), err)
	}
	elapsed := time.Since(start)
	if fake.gets.Load() != 1 || len(data) != throttleBody {
		t.Fatalf("gets=%d bytes=%d, want 1 GET of %d bytes", fake.gets.Load(), len(data), throttleBody)
	}
	if elapsed < 3*throttleStall {
		t.Fatalf("Get took %s; the throttle did not hold it past the stall timeout", elapsed)
	}
	t.Logf("Get of %d bytes: %s, 1 attempt", throttleBody, elapsed)
}

// stallBudget: three attempts that each stall (buffer fill plus the stall
// timeout), the SDK's jittered backoffs (at most 2 s + 4 s), and slack.
const stallBudget = 3*(throttleStall+time.Second) + 6*time.Second + 3*time.Second

func TestStalledPutObjectFailsWithinStallTimeout(t *testing.T) {
	fake, storage := newThrottledS3(t, true)
	start := time.Now()
	_, err := storage.PutObject(bytes.Repeat([]byte("s"), throttleBody))
	elapsed := time.Since(start)
	if err == nil || elapsed > stallBudget || fake.puts.Load() != 3 {
		t.Fatalf("err=%v elapsed=%s puts=%d; want a failure within %s after 3 attempts", err, elapsed, fake.puts.Load(), stallBudget)
	}
	t.Logf("stalled PutObject failed after %s, %d attempts", elapsed, fake.puts.Load())
}

func TestStalledGetFailsWithinStallTimeout(t *testing.T) {
	fake, storage := newThrottledS3(t, true)
	start := time.Now()
	_, err := storage.Get("vgi-rpc/key.bin", 1<<30)
	elapsed := time.Since(start)
	// The SDK retries a GET that fails before its headers; a body that stalls
	// part-way surfaces from the read (one attempt) -- either way it must
	// fail within the budget.
	if err == nil || elapsed > stallBudget {
		t.Fatalf("err=%v elapsed=%s gets=%d; want a failure within %s", err, elapsed, fake.gets.Load(), stallBudget)
	}
	t.Logf("stalled Get failed after %s, %d attempts", elapsed, fake.gets.Load())
}
