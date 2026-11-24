// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package vgigcs provides a GCS-backed ExternalStorage implementation for vgi-rpc.
//
// Usage:
//
//	storage, err := vgigcs.NewGCSStorage("my-bucket", vgigcs.GCSConfig{
//	    Prefix: "vgi-rpc/",
//	})
//	server.SetExternalLocation(vgirpc.DefaultExternalLocationConfig(storage))
package vgigcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/uuid"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// GCSConfig configures the GCS storage backend.
type GCSConfig struct {
	// Prefix is the object key prefix for uploaded objects. Default: "vgi-rpc/".
	Prefix string
	// PresignExpirySeconds is the lifetime of signed GET URLs. Default: 3600 (1 hour).
	PresignExpirySeconds int
	// Project is the GCS project ID. If empty, uses Application Default Credentials.
	Project string
	// StallTimeout fails a transfer that sends or receives no byte for this
	// long; a slow transfer that keeps moving is never cut off (there is no
	// whole-request deadline). Default: VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS,
	// else 30s. The connect timeout is 30s. Failed requests (connection
	// errors, stalls, 408, 429, 5xx) are retried: 3 attempts, exponential
	// backoff from 250ms capped at 5s (on top of the client library's own
	// retries of idempotent requests).
	StallTimeout time.Duration
}

// GCSStorage implements vgirpc.ExternalStorage using Google Cloud Storage.
type GCSStorage struct {
	bucket        string
	prefix        string
	presignExpiry time.Duration
	stall         time.Duration
	client        *storage.Client
}

// NewGCSStorage creates a new GCS-backed ExternalStorage.
func NewGCSStorage(bucket string, cfg GCSConfig) (*GCSStorage, error) {
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = "vgi-rpc/"
	}
	expiry := time.Duration(cfg.PresignExpirySeconds) * time.Second
	if expiry <= 0 {
		expiry = time.Hour
	}

	stall := stallTimeout(cfg.StallTimeout)
	client, err := newStorageClient(context.Background(), stall)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}

	return &GCSStorage{
		bucket:        bucket,
		prefix:        prefix,
		presignExpiry: expiry,
		stall:         stall,
		client:        client,
	}, nil
}

// newStorageClient builds a GCS client whose HTTP transport has the 30 s
// connect timeout and the stall timeout (and no whole-request deadline),
// authenticated with Application Default Credentials -- or unauthenticated
// against STORAGE_EMULATOR_HOST, as storage.NewClient itself does -- and
// the reference retry budget (3 attempts, 250ms..5s backoff).
func newStorageClient(ctx context.Context, stall time.Duration) (*storage.Client, error) {
	base := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           stallDialer(stall),
		TLSHandshakeTimeout:   storageConnectTimeout,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       stall,
		ExpectContinueTimeout: time.Second,
	}
	var rt http.RoundTripper = base
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		authed, err := htransport.NewTransport(ctx, base,
			option.WithScopes(storage.ScopeFullControl, "https://www.googleapis.com/auth/cloud-platform"))
		if err != nil {
			return nil, err
		}
		rt = authed
	}
	client, err := storage.NewClient(ctx, option.WithHTTPClient(&http.Client{Transport: rt}))
	if err != nil {
		return nil, err
	}
	client.SetRetry(
		storage.WithMaxAttempts(storageAttempts),
		storage.WithBackoff(gax.Backoff{Initial: storageBackoff, Max: storageMaxBackoff, Multiplier: 2}),
		// A stall is retried by withStallRetries, never by the client: its
		// retry loop for uploads has no attempt limit, so a server that
		// keeps stalling would be retried forever.
		storage.WithErrorFunc(func(err error) bool {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return false
			}
			return storage.ShouldRetry(err)
		}),
	)
	return client, nil
}

const (
	storageAttempts   = 3
	storageBackoff    = 250 * time.Millisecond
	storageMaxBackoff = 5 * time.Second
)

// withStallRetries runs op up to storageAttempts times while it fails with a
// stall (a timeout) or a transient error the client gave up on: the client's
// own retries do not cover a stall, and for single-request uploads they are
// not bounded by an attempt count. Every attempt of a write uses a fresh
// object name, so a retry never overwrites anything.
func withStallRetries(op func() error) error {
	var err error
	for attempt := 0; attempt < storageAttempts; attempt++ {
		if attempt > 0 {
			delay := min(storageBackoff<<(attempt-1), storageMaxBackoff)
			time.Sleep(time.Duration(float64(delay) * (0.5 + rand.Float64()/2)))
		}
		if err = op(); err == nil || !transientStorageError(err) {
			return err
		}
	}
	return err
}

func transientStorageError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return storage.ShouldRetry(err)
}

// Upload implements vgirpc.ExternalStorage.
func (s *GCSStorage) Upload(data []byte, schema *arrow.Schema, contentEncoding string) (string, error) {
	var url string
	err := withStallRetries(func() error {
		var err error
		url, err = s.uploadOnce(data, contentEncoding)
		return err
	})
	return url, err
}

func (s *GCSStorage) uploadOnce(data []byte, contentEncoding string) (string, error) {
	ext := ".arrow"
	if contentEncoding == "zstd" {
		ext = ".arrow.zst"
	}
	key := s.prefix + uuid.New().String() + ext

	ctx := context.Background()
	obj := s.client.Bucket(s.bucket).Object(key)
	w := obj.NewWriter(ctx)
	w.ContentType = "application/vnd.apache.arrow.stream"
	if contentEncoding != "" {
		w.ContentEncoding = contentEncoding
	}

	if _, err := io.Copy(w, io.NopCloser(io.NewSectionReader(readerAt(data), 0, int64(len(data))))); err != nil {
		w.Close()
		return "", fmt.Errorf("GCS upload write: %w", err)
	}
	if err := w.Close(); err != nil {
		return "", fmt.Errorf("GCS upload close: %w", err)
	}

	// Generate signed GET URL
	url, err := s.client.Bucket(s.bucket).SignedURL(key, &storage.SignedURLOptions{
		Method:  "GET",
		Expires: time.Now().Add(s.presignExpiry),
	})
	if err != nil {
		return "", fmt.Errorf("GCS signed URL: %w", err)
	}

	return url, nil
}

// readerAt wraps a byte slice as an io.ReaderAt.
type readerAt []byte

func (r readerAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r)) {
		return 0, io.EOF
	}
	n := copy(p, r[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Ensure GCSStorage implements ExternalStorage at compile time.
// defaultMaxFetchBytes is the Get cap when the caller passes none (256 MiB,
// vgirpc's default inbound fetch cap).
const defaultMaxFetchBytes int64 = 256 * 1024 * 1024

// ErrObjectMissing is returned by Get when GCS reports the object absent. It
// carries an ObjectMissing() method, which vgirpc.IsObjectMissing recognizes,
// so StateRef and argument-reference resolution map it to state_ref_missing /
// arguments_ref_missing.
var ErrObjectMissing error = objectMissingError{}

type objectMissingError struct{}

func (objectMissingError) Error() string       { return "GCS object is gone" }
func (objectMissingError) ObjectMissing() bool { return true }

// PutObject stores data under a fresh name and returns the name: the
// vgirpc.ObjectStore contract of protocol 3.4 (StateRef objects). GCS cannot
// verify a SHA-256 at storage; the upload is length-exact, never overwrites
// (DoesNotExist), and callers verify the digest on read. Objects carry no
// TTL: operators MUST set Object Lifecycle Management of at least one day on
// the prefix.
func (s *GCSStorage) PutObject(data []byte) (string, error) {
	var key string
	err := withStallRetries(func() error {
		var err error
		key, err = s.putObjectOnce(data)
		return err
	})
	return key, err
}

func (s *GCSStorage) putObjectOnce(data []byte) (string, error) {
	key := s.prefix + uuid.New().String() + ".bin"
	ctx := context.Background()
	w := s.client.Bucket(s.bucket).Object(key).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	if _, err := w.Write(data); err != nil {
		w.Close()
		return "", fmt.Errorf("GCS object write: %w", err)
	}
	if err := w.Close(); err != nil {
		return "", fmt.Errorf("GCS object close: %w", err)
	}
	return key, nil
}

// Get fetches the object a name refers to (vgirpc.ObjectStore), refusing more
// than maxBytes (<= 0: 256 MiB). A missing object is [ErrObjectMissing]. The
// bytes are read raw (no transcoding), so a stored Content-Encoding can never
// inflate past the cap. The caller verifies size and digest.
func (s *GCSStorage) Get(objectID string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = defaultMaxFetchBytes
	}
	var data []byte
	err := withStallRetries(func() error {
		var err error
		data, err = s.getOnce(objectID, maxBytes)
		return err
	})
	return data, err
}

func (s *GCSStorage) getOnce(objectID string, maxBytes int64) ([]byte, error) {
	// The client resumes a broken read by itself, without an attempt limit;
	// the watchdog ends a read that moves no byte for the stall timeout.
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	progress := make(chan struct{}, 1)
	go stallWatchdog(ctx, cancel, progress, s.stall)
	data, err := s.read(ctx, objectID, maxBytes, progress)
	if err != nil && errors.Is(context.Cause(ctx), errStalled) {
		return nil, fmt.Errorf("GCS object read: %w", errStalled)
	}
	return data, err
}

func (s *GCSStorage) read(ctx context.Context, objectID string, maxBytes int64, progress chan struct{}) ([]byte, error) {
	r, err := s.client.Bucket(s.bucket).Object(objectID).ReadCompressed(true).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, ErrObjectMissing
		}
		return nil, fmt.Errorf("GCS object read: %w", err)
	}
	defer r.Close()
	if size := r.Attrs.Size; size > maxBytes {
		return nil, fmt.Errorf("GCS object is %d bytes, over max_fetch_bytes (%d)", size, maxBytes)
	}
	data, err := io.ReadAll(io.LimitReader(progressReader{r: r, progress: progress}, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("GCS object read: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("GCS object exceeds max_fetch_bytes (%d)", maxBytes)
	}
	return data, nil
}

// Ensure GCSStorage implements ExternalStorage at compile time. (It also
// implements vgirpc.ObjectStore; that check waits for the core release that
// declares the interface, since this module pins a published core.)
var _ vgirpc.ExternalStorage = (*GCSStorage)(nil)

// errStalled ends a read that made no progress for the stall timeout. It is
// a timeout, so withStallRetries retries it.
var errStalled error = stallError{}

type stallError struct{}

func (stallError) Error() string   { return "GCS transfer stalled" }
func (stallError) Timeout() bool   { return true }
func (stallError) Temporary() bool { return true }

// progressReader signals progress for every read that returns bytes.
type progressReader struct {
	r        io.Reader
	progress chan<- struct{}
}

func (p progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		select {
		case p.progress <- struct{}{}:
		default:
		}
	}
	return n, err
}

// stallWatchdog cancels ctx with errStalled when no progress arrives for
// stall.
func stallWatchdog(ctx context.Context, cancel context.CancelCauseFunc, progress <-chan struct{}, stall time.Duration) {
	timer := time.NewTimer(stall)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-progress:
			timer.Reset(stall)
		case <-timer.C:
			cancel(errStalled)
			return
		}
	}
}
