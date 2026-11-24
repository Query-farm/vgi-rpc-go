// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Throttle tests for the storage client: a slow transfer that keeps moving
// must succeed in one attempt however long it takes; one that stops moving
// must fail within the stall timeout (per attempt) plus slack.

const (
	throttleStall = time.Second
	throttleRate  = 8 << 20  // bytes per second (drains a 4 MiB socket buffer within the stall timeout)
	throttleBody  = 32 << 20 // 4 s at throttleRate: 4x the stall timeout
)

// smallBufferListener caps the server's receive buffer so a slow reader
// pushes back on the client quickly, instead of the kernel absorbing the body.
func smallBufferListener(t *testing.T) net.Listener {
	t.Helper()
	lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
		var serr error
		if err := c.Control(func(fd uintptr) {
			serr = setRecvBuffer(fd, 64<<10)
		}); err != nil {
			return err
		}
		return serr
	}}
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

// throttleServer starts a TLS server running handler, and a vendor API whose
// storage client trusts it. done closes when the test ends, releasing
// handlers that are stalling on purpose.
func throttleServer(t *testing.T, handler func(w http.ResponseWriter, r *http.Request, done <-chan struct{})) (*httptest.Server, *vendorAPI) {
	t.Helper()
	done := make(chan struct{})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler(w, r, done)
	}))
	srv.Listener = smallBufferListener(t)
	srv.StartTLS()
	t.Cleanup(func() {
		close(done)
		srv.CloseClientConnections()
		srv.Close()
	})
	api, err := newVendorAPI("http://127.0.0.1:1", "token", VendorOptions{
		Timeout:      throttleStall, // the old whole-request deadline
		StallTimeout: throttleStall,
		Backoff:      10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	api.storage.Transport.(*http.Transport).TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	t.Cleanup(api.close)
	return srv, api
}

// pace sleeps so that n bytes since start move at no more than throttleRate.
func pace(start time.Time, n int) {
	ahead := time.Duration(float64(n)/throttleRate*float64(time.Second)) - time.Since(start)
	if ahead > 0 {
		time.Sleep(ahead)
	}
}

func TestStorageSlowUploadSucceedsInOneAttempt(t *testing.T) {
	var attempts, received atomic.Int64
	srv, api := throttleServer(t, func(w http.ResponseWriter, r *http.Request, _ <-chan struct{}) {
		attempts.Add(1)
		start, total, buf := time.Now(), 0, make([]byte, 16<<10)
		for {
			n, err := r.Body.Read(buf)
			total += n
			pace(start, total)
			if err != nil {
				break
			}
		}
		received.Store(int64(total))
		w.WriteHeader(http.StatusOK)
	})
	data := bytes.Repeat([]byte("v"), throttleBody)
	grant := &PutGrant{UploadURL: srv.URL + "/bucket/object"}
	start := time.Now()
	if err := api.putObject(context.Background(), grant, data, ""); err != nil {
		t.Fatalf("slow upload failed after %s: %v", time.Since(start), err)
	}
	elapsed := time.Since(start)
	if attempts.Load() != 1 || received.Load() != throttleBody {
		t.Fatalf("attempts=%d received=%d, want 1 attempt of %d bytes", attempts.Load(), received.Load(), throttleBody)
	}
	if elapsed < 3*throttleStall {
		t.Fatalf("upload took %s; the throttle did not hold it past the stall timeout", elapsed)
	}
	t.Logf("upload of %d bytes took %s in one attempt (stall timeout %s)", throttleBody, elapsed, throttleStall)
}

func TestStorageSlowDownloadSucceedsInOneAttempt(t *testing.T) {
	var attempts atomic.Int64
	srv, api := throttleServer(t, func(w http.ResponseWriter, r *http.Request, _ <-chan struct{}) {
		attempts.Add(1)
		w.Header().Set("Content-Length", "33554432")
		w.WriteHeader(http.StatusOK)
		start, chunk := time.Now(), bytes.Repeat([]byte("d"), 16<<10)
		for sent := 0; sent < throttleBody; sent += len(chunk) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			pace(start, sent+len(chunk))
		}
	})
	start := time.Now()
	data, err := api.download(context.Background(), srv.URL+"/bucket/object", func(string) error { return nil }, 1<<30)
	if err != nil {
		t.Fatalf("slow download failed after %s: %v", time.Since(start), err)
	}
	elapsed := time.Since(start)
	if attempts.Load() != 1 || len(data) != throttleBody {
		t.Fatalf("attempts=%d bytes=%d, want 1 attempt of %d bytes", attempts.Load(), len(data), throttleBody)
	}
	if elapsed < 3*throttleStall {
		t.Fatalf("download took %s; the throttle did not hold it past the stall timeout", elapsed)
	}
	t.Logf("download of %d bytes took %s in one attempt (stall timeout %s)", throttleBody, elapsed, throttleStall)
}

// stallBudget is the most a 3-attempt operation that stalls every attempt
// may take: three stall timeouts, the (10 ms) backoffs, and slack for
// filling the socket buffers before each stall.
const stallBudget = 3*throttleStall + 4*time.Second

func TestStorageStalledUploadFailsWithinStallTimeout(t *testing.T) {
	var attempts atomic.Int64
	srv, api := throttleServer(t, func(w http.ResponseWriter, r *http.Request, done <-chan struct{}) {
		attempts.Add(1)
		_, _ = io.CopyN(io.Discard, r.Body, 256<<10) // then stop reading
		select {
		case <-done:
		case <-r.Context().Done():
		}
	})
	start := time.Now()
	err := api.putObject(context.Background(), &PutGrant{UploadURL: srv.URL + "/o"}, bytes.Repeat([]byte("s"), 32<<20), "")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("stalled upload succeeded")
	}
	if elapsed > stallBudget {
		t.Fatalf("stalled upload took %s to fail, want <= %s", elapsed, stallBudget)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts=%d, want 3 (a stall is retried)", attempts.Load())
	}
	t.Logf("stalled upload failed after %s, %d attempts: %v", elapsed, attempts.Load(), err)
}

func TestStorageStalledDownloadFailsWithinStallTimeout(t *testing.T) {
	var attempts atomic.Int64
	srv, api := throttleServer(t, func(w http.ResponseWriter, r *http.Request, done <-chan struct{}) {
		attempts.Add(1)
		w.Header().Set("Content-Length", "33554432")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bytes.Repeat([]byte("p"), 256<<10)) // then stop sending
		w.(http.Flusher).Flush()
		select {
		case <-done:
		case <-r.Context().Done():
		}
	})
	start := time.Now()
	_, err := api.download(context.Background(), srv.URL+"/o", func(string) error { return nil }, 1<<30)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("stalled download succeeded")
	}
	if elapsed > stallBudget {
		t.Fatalf("stalled download took %s to fail, want <= %s", elapsed, stallBudget)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts=%d, want 3 (a stall is retried)", attempts.Load())
	}
	t.Logf("stalled download failed after %s, %d attempts: %v", elapsed, attempts.Load(), err)
}

func TestStorageNoResponseFailsWithinStallTimeout(t *testing.T) {
	var attempts atomic.Int64
	srv, api := throttleServer(t, func(w http.ResponseWriter, r *http.Request, done <-chan struct{}) {
		attempts.Add(1)
		select { // never answers
		case <-done:
		case <-r.Context().Done():
		}
	})
	start := time.Now()
	_, err := api.download(context.Background(), srv.URL+"/o", func(string) error { return nil }, 1<<30)
	elapsed := time.Since(start)
	if err == nil || elapsed > stallBudget || attempts.Load() != 3 {
		t.Fatalf("err=%v elapsed=%s attempts=%d; want a failure within %s after 3 attempts", err, elapsed, attempts.Load(), stallBudget)
	}
}

func TestStorageStallTimeoutFromEnv(t *testing.T) {
	cases := map[string]time.Duration{
		"":     DefaultStorageStallTimeout,
		"90":   90 * time.Second,
		"0.5":  500 * time.Millisecond,
		"0":    DefaultStorageStallTimeout,
		"-3":   DefaultStorageStallTimeout,
		"soon": DefaultStorageStallTimeout,
	}
	for raw, want := range cases {
		got := StorageStallTimeoutFromEnv(func(name string) string {
			if name == EnvStorageStallTimeout {
				return raw
			}
			return ""
		})
		if got != want {
			t.Errorf("%s=%q: got %s, want %s", EnvStorageStallTimeout, raw, got, want)
		}
	}
}

func TestStorageClientsHaveNoWholeRequestDeadline(t *testing.T) {
	config, err := ExternalConfigFromEnv(func(name string) string {
		switch name {
		case EnvBlobVendorURL:
			return "https://vendor.example"
		case "VGI_BLOB_VENDOR_TOKEN":
			return "token"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, client := range map[string]*http.Client{
		"pointer fetch (env config)":     config.HTTPClient,
		"pointer fetch (default config)": DefaultExternalLocationConfig(nil).httpClient(),
		"request upload":                 storageUploadClient(),
		"vendor storage":                 config.Storage.(*VendorStorage).api.storage,
	} {
		if client.Timeout != 0 {
			t.Errorf("%s client has a whole-request Timeout %s", name, client.Timeout)
		}
		if _, ok := client.Transport.(*http.Transport); !ok {
			t.Errorf("%s client does not use the storage transport", name)
		}
	}
}
