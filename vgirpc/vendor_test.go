// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// The blob-vendor client is client-side code with no server surface, so it is
// tested here (see CLAUDE.md "Testing Policy") against vgi-rpc-python's fake
// vendor -- the same one the vgi extension's blob-vendor lane runs -- started
// from VGI_RPC_FAKE_VENDOR_PYTHON (an interpreter with vgi-rpc installed; CI's
// race job and `make ci` set it), or a vendor the caller already runs at
// VGI_RPC_FAKE_VENDOR_URL (token "test-token", --test-routes). Without either
// the vendor tests skip.

const fakeVendorToken = "test-token"

var (
	fakeVendorOnce sync.Once
	fakeVendorURL  string
	fakeVendorErr  error
	fakeVendorCmd  *exec.Cmd
)

func TestMain(m *testing.M) {
	code := m.Run()
	if fakeVendorCmd != nil && fakeVendorCmd.Process != nil {
		_ = fakeVendorCmd.Process.Kill()
		_ = fakeVendorCmd.Wait()
	}
	os.Exit(code)
}

func startFakeVendor() (string, error) {
	if url := os.Getenv("VGI_RPC_FAKE_VENDOR_URL"); url != "" {
		return url, nil
	}
	python := os.Getenv("VGI_RPC_FAKE_VENDOR_PYTHON")
	if python == "" {
		return "", errors.New("set VGI_RPC_FAKE_VENDOR_PYTHON (with vgi-rpc installed) or VGI_RPC_FAKE_VENDOR_URL")
	}
	if err := exec.Command(python, "-c", "import vgi_rpc.conformance.fake_vendor").Run(); err != nil {
		return "", fmt.Errorf("%s cannot import vgi_rpc.conformance.fake_vendor", python)
	}
	cmd := exec.Command(python, "-m", "vgi_rpc.conformance.fake_vendor", "--test-routes", "--token", fakeVendorToken+"=tenant-a")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return "", err
	}
	fakeVendorCmd = cmd
	lines := make(chan string)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()
	deadline := time.After(60 * time.Second)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				return "", errors.New("fake vendor exited before printing its URL")
			}
			if rest, found := strings.CutPrefix(line, "URL:"); found {
				go func() {
					for range lines {
					}
				}()
				return rest, nil
			}
		case <-deadline:
			return "", errors.New("timed out waiting for the fake vendor")
		}
	}
}

func fakeVendor(t *testing.T) string {
	t.Helper()
	fakeVendorOnce.Do(func() { fakeVendorURL, fakeVendorErr = startFakeVendor() })
	if fakeVendorErr != nil {
		t.Skipf("fake blob vendor unavailable: %v", fakeVendorErr)
	}
	vendorAdmin(t, fakeVendorURL, "/__admin__/reset")
	return fakeVendorURL
}

func vendorAdmin(t *testing.T, base, path string) map[string]any {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return out
}

func vendorStat(t *testing.T, base, key string) int64 {
	t.Helper()
	v, ok := vendorAdmin(t, base, "/__stats__")[key].(float64)
	if !ok {
		t.Fatalf("no %q in /__stats__", key)
	}
	return int64(v)
}

func vendorErrorCode(err error) (string, int) {
	var ve *VendorError
	if errors.As(err, &ve) {
		return ve.Code, ve.Status
	}
	return "", -1
}

func TestVendorBlobClientRoundTrip(t *testing.T) {
	base := fakeVendor(t)
	client, err := NewVendorBlobClient(base, fakeVendorToken, "", VendorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	data := bytes.Repeat([]byte("blob-bytes-"), 1000)
	putsBefore := vendorStat(t, base, "puts")
	ref, err := client.PutBytes(ctx, data, &BlobScope{Shard: "att-00ff", ExecutionID: "s.0102"}, "call.0")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if ref.Size != int64(len(data)) || ref.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("ref = %+v", ref)
	}
	if got := vendorStat(t, base, "puts") - putsBefore; got != 1 {
		t.Fatalf("puts delta = %d, want 1", got)
	}
	// The same idempotency key with the same bytes reuses the object.
	again, err := client.PutBytes(ctx, data, &BlobScope{Shard: "att-00ff", ExecutionID: "s.0102"}, "call.0")
	if err != nil || again.ObjectID != ref.ObjectID {
		t.Fatalf("idempotent put: %v %+v vs %+v", err, again, ref)
	}
	// ... and with other bytes is refused.
	_, err = client.PutBytes(ctx, []byte("other"), &BlobScope{Shard: "att-00ff", ExecutionID: "s.0102"}, "call.0")
	if code, status := vendorErrorCode(err); code != "idempotency_conflict" || status != 409 {
		t.Fatalf("conflicting put: code=%q status=%d err=%v", code, status, err)
	}
	got, err := client.GetBytes(ctx, ref)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("GetBytes: %v (len %d)", err, len(got))
	}
	// Empty blobs work.
	empty, err := client.PutBytes(ctx, nil, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := client.GetBytes(ctx, empty); err != nil || len(got) != 0 {
		t.Fatalf("empty GetBytes: %v %d", err, len(got))
	}
	// Scoped delete removes the scoped object; the blob is then missing.
	if n, err := client.DeleteScope(ctx, "att-00ff", "s.0102"); err != nil || n != 1 {
		t.Fatalf("DeleteScope = %d, %v", n, err)
	}
	_, err = client.GetBytes(ctx, ref)
	if code, status := vendorErrorCode(err); status != 404 || (code != "not_found" && code != "not_uploaded") {
		t.Fatalf("get after delete: code=%q status=%d err=%v", code, status, err)
	}
	if n, err := client.Delete(ctx, []string{empty.ObjectID, "0123456789abcdef0123456789abcdef"}); err != nil || n != 1 {
		t.Fatalf("Delete = %d, %v", n, err)
	}
}

func TestVendorBlobClientVerifiesAgainstRef(t *testing.T) {
	base := fakeVendor(t)
	client, err := NewVendorBlobClient(base, fakeVendorToken, "", VendorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	data := []byte(strings.Repeat("x", 5000))
	ref, err := client.PutBytes(ctx, data, nil, "")
	if err != nil {
		t.Fatal(err)
	}

	// A ref whose digest disagrees with the bytes: sha256_mismatch.
	bad := ref
	bad.SHA256 = strings.Repeat("0", 64)
	if code, _ := vendorErrorCode(func() error { _, e := client.GetBytes(ctx, bad); return e }()); code != VendorCodeSHA256Mismatch {
		t.Fatalf("wrong digest: code %q", code)
	}
	// A ref whose size disagrees with the vendor: size_mismatch.
	bad = ref
	bad.Size++
	if code, _ := vendorErrorCode(func() error { _, e := client.GetBytes(ctx, bad); return e }()); code != VendorCodeSizeMismatch {
		t.Fatalf("wrong size: code %q", code)
	}

	// The storage corrupts the next fs download: same length, other bytes.
	vendorAdmin(t, base, "/__admin__/fault?purpose=fs&action=corrupt&count=1")
	_, err = client.GetBytes(ctx, ref)
	if code, _ := vendorErrorCode(err); code != VendorCodeSHA256Mismatch {
		t.Fatalf("corrupt download: code %q err %v", code, err)
	}
	vendorAdmin(t, base, "/__admin__/reset")
	if got, err := client.GetBytes(ctx, ref); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("after reset: %v", err)
	}

	// The storage loses the next fs download: the signed GET 404s (NoSuchKey).
	ref2, err := client.PutBytes(ctx, data, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	vendorAdmin(t, base, "/__admin__/fault?purpose=fs&action=missing&count=1")
	_, err = client.GetBytes(ctx, ref2)
	if code, status := vendorErrorCode(err); status != 404 || code != "NoSuchKey" {
		t.Fatalf("missing download: code=%q status=%d err=%v", code, status, err)
	}
	if strings.Contains(err.Error(), "X-Amz") || strings.Contains(err.Error(), "?") {
		t.Fatalf("error leaks the signed query: %v", err)
	}
}

func TestVendorStorageUploadAndUploadURL(t *testing.T) {
	base := fakeVendor(t)
	storage, err := NewVendorStorage(base, fakeVendorToken, "", VendorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	// The fake counts grants by origin: a server write sends none (puts_rpc),
	// a client-upload grant sends origin=client_upload (puts_upload), and the
	// signed PUTs to client-upload objects count apart (client_uploads).
	stat := func(key string) int64 { return vendorStat(t, base, key) }
	putsRPC, putsUpload, clientUploads := stat("puts_rpc"), stat("puts_upload"), stat("client_uploads")
	data := bytes.Repeat([]byte{1, 2, 3}, 4000)
	download, err := storage.Upload(data, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := stat("puts_rpc") - putsRPC; got != 1 {
		t.Fatalf("server write: puts_rpc +%d, want +1", got)
	}
	if got := stat("puts_upload") - putsUpload; got != 0 {
		t.Fatalf("server write was marked as a client upload: puts_upload +%d", got)
	}
	if err := storage.URLValidator()(download); err != nil {
		t.Fatalf("download URL off the pinned hosts: %v", err)
	}
	resp, err := http.Get(download)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Equal(got, data) {
		t.Fatalf("download: %d, %d bytes", resp.StatusCode, len(got))
	}

	body := []byte("request-body")
	digest := sha256.Sum256(body)
	pair, err := storage.GenerateUploadURL(int64(len(body)), hex.EncodeToString(digest[:]))
	if err != nil {
		t.Fatal(err)
	}
	if pair.ExpiresAt.IsZero() || pair.UploadURL == "" || pair.DownloadURL == "" {
		t.Fatalf("upload pair = %+v", pair)
	}
	if got := stat("puts_upload") - putsUpload; got != 1 {
		t.Fatalf("client-upload grant: puts_upload +%d, want +1 (origin=%s missing?)", got, ClientUploadOrigin)
	}
	if got := stat("puts_rpc") - putsRPC; got != 1 {
		t.Fatalf("client-upload grant counted as a server write: puts_rpc +%d, want +1", got)
	}
	if _, ok := pair.Headers["x-amz-checksum-sha256"]; !ok {
		t.Fatalf("sized upload URL carries no x-amz-checksum-sha256 header: %v", pair.Headers)
	}
	for name := range pair.Headers {
		if strings.EqualFold(name, "Content-Length") {
			t.Fatalf("upload headers must not carry Content-Length: %v", pair.Headers)
		}
	}
	vendorPut := func(payload []byte) int {
		t.Helper()
		put, _ := http.NewRequest(http.MethodPut, pair.UploadURL, bytes.NewReader(payload))
		for name, value := range pair.Headers {
			put.Header.Set(name, value)
		}
		resp, err := storageUploadClient.Do(put)
		if err != nil {
			t.Fatalf("PUT to vended URL: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	// Storage verifies the sized grant: same length, different bytes is a
	// checksum refusal; a different length is refused too.
	if status := vendorPut([]byte("request-bodx")); status/100 == 2 {
		t.Fatalf("PUT of tampered bytes accepted (HTTP %d)", status)
	}
	if status := vendorPut([]byte("short")); status/100 == 2 {
		t.Fatalf("PUT of a short body accepted (HTTP %d)", status)
	}
	if status := vendorPut(body); status/100 != 2 {
		t.Fatalf("PUT of the declared body refused (HTTP %d)", status)
	}
	if got := stat("client_uploads") - clientUploads; got != 1 {
		t.Fatalf("client_uploads +%d, want +1", got)
	}
	resp, err = http.Get(pair.DownloadURL)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != "request-body" {
		t.Fatalf("vended download = %q", got)
	}

	if _, err := storage.GenerateUploadURL(int64(len(body)), "not-hex"); err == nil {
		t.Fatal("GenerateUploadURL accepted a malformed sha256")
	}
}

func TestVendorBadTokenAndRetry(t *testing.T) {
	base := fakeVendor(t)
	client, err := NewVendorBlobClient(base, "wrong-token", "", VendorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutBytes(context.Background(), []byte("x"), nil, "")
	if _, status := vendorErrorCode(err); status != 401 {
		t.Fatalf("bad token: status %d err %v", status, err)
	}
	if strings.Contains(err.Error(), "wrong-token") {
		t.Fatalf("error leaks the token: %v", err)
	}
}

func TestVendorRetriesServerErrors(t *testing.T) {
	var calls int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(401)
			return
		}
		if n < 3 {
			w.WriteHeader(503)
			return
		}
		_, _ = w.Write([]byte(`{"deleted": 2}`))
	}))
	defer srv.Close()
	client, err := NewVendorBlobClient(srv.URL, "tok", "", VendorOptions{Backoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	n, err := client.Delete(context.Background(), []string{"a", "b"})
	if err != nil || n != 2 || calls != 3 {
		t.Fatalf("Delete = %d, %v after %d calls", n, err, calls)
	}
	calls = -10
	_, err = client.Delete(context.Background(), []string{"a"})
	if _, status := vendorErrorCode(err); status != 503 {
		t.Fatalf("exhausted retries: status %d err %v", status, err)
	}
}

func TestVendorStorageRequestsCarryNoCallerCredentials(t *testing.T) {
	base := fakeVendor(t)
	// A caller transport that stamps credentials on everything it sends.
	var seen []http.Header
	var mu sync.Mutex
	stamping := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		r.Header.Set("Cookie", "session=secret")
		r.Header.Set("X-Caller", "1")
		mu.Lock()
		seen = append(seen, r.Header.Clone())
		mu.Unlock()
		return http.DefaultTransport.RoundTrip(r)
	})
	storage, err := NewVendorStorage(base, fakeVendorToken, "", VendorOptions{HTTPClient: &http.Client{Transport: stamping}})
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	if _, err := storage.Upload([]byte("payload"), nil, ""); err != nil {
		t.Fatal(err)
	}
	// Only vendor API calls went through the caller's transport; the signed
	// PUT went out on the bare storage client.
	mu.Lock()
	defer mu.Unlock()
	for _, h := range seen {
		if h.Get("X-Amz-Checksum-Sha256") != "" {
			t.Fatal("the signed PUT used the caller's transport")
		}
	}
	if len(seen) == 0 {
		t.Fatal("vendor API calls did not use the caller's transport")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPinnedHostsValidator(t *testing.T) {
	v, err := PinnedHostsValidator([]string{"r2.example.com", "127.0.0.1:9000", "[::1]:8443"}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, ok := range []string{
		"https://r2.example.com/x?sig=1",
		"https://R2.Example.com:443/x",
		"https://127.0.0.1:9000/x",
		"https://[::1]:8443/x",
	} {
		if err := v(ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{
		"http://r2.example.com/x",
		"https://r2.example.com:8443/x",
		"https://evil.r2.example.com/x",
		"https://user:pw@r2.example.com/x",
		"https://127.0.0.1/x",
		"ftp://r2.example.com/x",
	} {
		if err := v(bad); err == nil {
			t.Errorf("%s: accepted", bad)
		} else if strings.Contains(err.Error(), "sig=") {
			t.Errorf("%s: error leaks query: %v", bad, err)
		}
	}
	withHTTP, _ := PinnedHostsValidator([]string{"localhost"}, true, []string{"http://localhost/r2/"})
	if err := withHTTP("http://localhost/r2/obj?X-Amz-Signature=s"); err != nil {
		t.Errorf("prefix match: %v", err)
	}
	for _, bad := range []string{"http://localhost/other/obj", "http://localhost/r2/../other"} {
		if err := withHTTP(bad); err == nil {
			t.Errorf("%s: accepted outside the prefix", bad)
		}
	}
	if _, err := PinnedHostsValidator(nil, false, nil); err == nil {
		t.Error("empty host list accepted")
	}
	if _, err := PinnedHostsValidator([]string{"host/path"}, false, nil); err == nil {
		t.Error("entry with a path accepted")
	}
}

func TestVendorURLRules(t *testing.T) {
	if _, err := NewVendorBlobClient("http://vendor.example.com", "t", "", VendorOptions{}); err == nil {
		t.Error("plain http to a non-loopback vendor accepted")
	}
	if _, err := NewVendorBlobClient("https://vendor.example.com", "", "", VendorOptions{}); err == nil {
		t.Error("empty token accepted")
	}
	if _, err := NewVendorBlobClient("http://127.0.0.1:1", "t", "", VendorOptions{}); err != nil {
		t.Errorf("loopback http refused: %v", err)
	}
}

func TestExternalConfigFromEnv(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if cfg, err := ExternalConfigFromEnv(env(nil)); cfg != nil || err != nil {
		t.Fatalf("unset vendor: %v %v", cfg, err)
	}
	if _, err := ExternalConfigFromEnv(env(map[string]string{EnvBlobVendorURL: "https://v.example"})); err == nil {
		t.Fatal("vendor URL without a token accepted")
	}
	cfg, err := ExternalConfigFromEnv(env(map[string]string{
		EnvBlobVendorURL:           "https://v.example",
		EnvBlobVendorTokenFallback: "tok",
		EnvExternalizeThreshold:    "4096",
		EnvExternalCompression:     "zstd:3",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ExternalizeThresholdBytes != 4096 || cfg.Compression == nil || cfg.Compression.Level != 3 {
		t.Fatalf("config = %+v", cfg)
	}
	if _, ok := cfg.Storage.(*VendorStorage); !ok {
		t.Fatalf("storage = %T", cfg.Storage)
	}
	for _, bad := range []map[string]string{
		{EnvBlobVendorURL: "https://v.example", EnvBlobVendorToken: "t", EnvExternalizeThreshold: "0"},
		{EnvBlobVendorURL: "https://v.example", EnvBlobVendorToken: "t", EnvExternalizeThreshold: "x"},
		{EnvBlobVendorURL: "https://v.example", EnvBlobVendorToken: "t", EnvExternalCompression: "brotli"},
	} {
		if _, err := ExternalConfigFromEnv(env(bad)); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
	if n, err := MaxUploadBytesFromEnv(env(map[string]string{EnvMaxUploadBytes: "268435456"})); err != nil || n != 268435456 {
		t.Fatalf("MaxUploadBytesFromEnv = %d, %v", n, err)
	}
}

// TestVendorStorageObjectStore covers the protocol 3.4 ObjectStore contract on
// the vendor: PutObject is a sized rpc grant marked origin=state_ref (counted
// in puts_state_ref and puts_rpc), Get mints the download from the id
// (gets_state_ref), the grant's object_id rides GenerateUploadURL, and a
// deleted object is ErrObjectMissing.
func TestVendorStorageObjectStore(t *testing.T) {
	base := fakeVendor(t)
	storage, err := NewVendorStorage(base, fakeVendorToken, "", VendorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	stat := func(key string) int64 { return vendorStat(t, base, key) }
	putsStateRef, putsRPC, getsStateRef := stat("puts_state_ref"), stat("puts_rpc"), stat("gets_state_ref")

	data := bytes.Repeat([]byte("sealed stateref object "), 300)
	id, err := storage.PutObject(data)
	if err != nil || id == "" {
		t.Fatalf("PutObject: id=%q err=%v", id, err)
	}
	if stat("puts_state_ref")-putsStateRef != 1 || stat("puts_rpc")-putsRPC != 1 {
		t.Fatal("PutObject must be one rpc grant with origin state_ref")
	}
	got, err := storage.Get(id, 0)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Get: %d bytes, err=%v", len(got), err)
	}
	if stat("gets_state_ref")-getsStateRef != 1 {
		t.Fatal("Get must mint the download from the object id")
	}
	if _, err := storage.Get(id, int64(len(data)-1)); err == nil || IsObjectMissing(err) {
		t.Fatalf("Get over maxBytes must be refused (not missing), got %v", err)
	}

	digest := sha256.Sum256([]byte("arg"))
	pair, err := storage.GenerateUploadURL(3, hex.EncodeToString(digest[:]))
	if err != nil || pair.ObjectID == "" {
		t.Fatalf("GenerateUploadURL must return the grant's object id: %+v %v", pair, err)
	}

	vendorAdmin(t, base, "/__admin__/delete?origin=state_ref")
	if _, err := storage.Get(id, 0); !IsObjectMissing(err) {
		t.Fatalf("a deleted object must be ErrObjectMissing, got %v", err)
	}
	if _, err := storage.Get("no-such-object", 0); !IsObjectMissing(err) {
		t.Fatalf("an unknown id must be ErrObjectMissing, got %v", err)
	}
}
