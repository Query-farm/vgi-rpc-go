// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package vgigcs

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Throttle tests against a minimal fake GCS (STORAGE_EMULATOR_HOST): a slow
// transfer that keeps moving succeeds in one attempt; one that stops moving
// fails within the stall timeout per attempt plus slack.

const (
	throttleStall = 2 * time.Second
	throttleRate  = 3 << 20  // bytes per second (drains a 4 MiB socket buffer within the stall timeout)
	throttleBody  = 15 << 20 // 5 s at throttleRate; under the 16 MiB chunk size, so one upload request
)

func pace(start time.Time, n int) {
	if ahead := time.Duration(float64(n)/throttleRate*float64(time.Second)) - time.Since(start); ahead > 0 {
		time.Sleep(ahead)
	}
}

type fakeGCS struct {
	uploads, gets atomic.Int64
	received      atomic.Int64
	stall         bool
	done          chan struct{}
	t             *testing.T
}

func (f *fakeGCS) wait(r *http.Request) {
	select {
	case <-f.done:
	case <-r.Context().Done():
	}
}

func (f *fakeGCS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/storage/v1/b/"):
		f.uploads.Add(1)
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
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"bucket":"bucket","name":"obj","size":"%d","generation":"1","metageneration":"1"}`, total)
	case r.Method == http.MethodGet:
		f.gets.Add(1)
		// The client resumes a broken read with "Range: bytes=N-".
		offset := 0
		if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
			offset, _ = strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(rng, "bytes="), "-"))
		}
		w.Header().Set("Content-Length", strconv.Itoa(throttleBody-offset))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Goog-Generation", "1")
		w.Header().Set("X-Goog-Metageneration", "1")
		if offset > 0 {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, throttleBody-1, throttleBody))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		start, chunk := time.Now(), bytes.Repeat([]byte("g"), 16<<10)
		for sent := offset; sent < throttleBody; sent += len(chunk) {
			// Stalling: the first response stops after 256 KiB, a resumed one
			// sends nothing at all.
			if f.stall && (offset > 0 || sent >= 256<<10) {
				w.(http.Flusher).Flush()
				f.wait(r)
				return
			}
			if _, err := w.Write(chunk); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			pace(start, sent-offset+len(chunk))
		}
	default:
		f.t.Logf("fake GCS: unhandled %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func newThrottledGCS(t *testing.T, stall bool) (*fakeGCS, *GCSStorage) {
	t.Helper()
	fake := &fakeGCS{stall: stall, done: make(chan struct{}), t: t}
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
	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(srv.URL, "http://"))
	storage, err := NewGCSStorage("bucket", GCSConfig{StallTimeout: throttleStall})
	if err != nil {
		t.Fatal(err)
	}
	return fake, storage
}

func TestSlowPutObjectSucceedsInOneAttempt(t *testing.T) {
	fake, storage := newThrottledGCS(t, false)
	start := time.Now()
	if _, err := storage.PutObject(bytes.Repeat([]byte("p"), throttleBody)); err != nil {
		t.Fatalf("slow PutObject failed after %s: %v", time.Since(start), err)
	}
	elapsed := time.Since(start)
	if fake.uploads.Load() != 1 || fake.received.Load() < throttleBody {
		t.Fatalf("uploads=%d received=%d, want 1 upload of >= %d bytes", fake.uploads.Load(), fake.received.Load(), throttleBody)
	}
	if elapsed < 2*throttleStall {
		t.Fatalf("PutObject took %s; the throttle did not hold it past the stall timeout", elapsed)
	}
	t.Logf("PutObject of %d bytes: %s, 1 attempt (stall timeout %s)", throttleBody, elapsed, throttleStall)
}

func TestSlowGetSucceedsInOneAttempt(t *testing.T) {
	fake, storage := newThrottledGCS(t, false)
	start := time.Now()
	data, err := storage.Get("vgi-rpc/obj.bin", 1<<30)
	if err != nil {
		t.Fatalf("slow Get failed after %s: %v", time.Since(start), err)
	}
	elapsed := time.Since(start)
	if fake.gets.Load() != 1 || len(data) != throttleBody {
		t.Fatalf("gets=%d bytes=%d, want 1 GET of %d bytes", fake.gets.Load(), len(data), throttleBody)
	}
	if elapsed < 2*throttleStall {
		t.Fatalf("Get took %s; the throttle did not hold it past the stall timeout", elapsed)
	}
	t.Logf("Get of %d bytes: %s, 1 attempt", throttleBody, elapsed)
}

// stallBudget: three attempts that each stall (buffer fill plus the stall
// timeout), backoffs of at most 0.25 s + 0.5 s, and slack.
const stallBudget = 3*(throttleStall+2*time.Second) + 4*time.Second

func TestStalledPutObjectFailsWithinStallTimeout(t *testing.T) {
	fake, storage := newThrottledGCS(t, true)
	start := time.Now()
	_, err := storage.PutObject(bytes.Repeat([]byte("s"), throttleBody))
	elapsed := time.Since(start)
	if err == nil || elapsed > stallBudget || fake.uploads.Load() != 3 {
		t.Fatalf("err=%v elapsed=%s uploads=%d; want a failure within %s after 3 attempts", err, elapsed, fake.uploads.Load(), stallBudget)
	}
	t.Logf("stalled PutObject failed after %s, %d attempts", elapsed, fake.uploads.Load())
}

func TestStalledGetFailsWithinStallTimeout(t *testing.T) {
	fake, storage := newThrottledGCS(t, true)
	start := time.Now()
	_, err := storage.Get("vgi-rpc/obj.bin", 1<<30)
	elapsed := time.Since(start)
	if err == nil || elapsed > stallBudget {
		t.Fatalf("err=%v elapsed=%s gets=%d; want a failure within %s", err, elapsed, fake.gets.Load(), stallBudget)
	}
	t.Logf("stalled Get failed after %s, %d GETs: %v", elapsed, fake.gets.Load(), err)
}
