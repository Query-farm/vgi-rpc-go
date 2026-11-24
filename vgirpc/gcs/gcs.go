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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
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
	// Prefix is the object key prefix for uploaded objects. Default: "vgi-rpc/"
	// unless ExactPrefix is set.
	Prefix string
	// ExactPrefix uses Prefix exactly as given, even when it is empty.
	ExactPrefix bool
	// PresignExpirySeconds is the lifetime of signed URLs. Default: 3600 (1
	// hour); at most 7 days (the V4 limit). Service-account signatures do
	// not depend on short-lived tokens, so no credential cap applies.
	PresignExpirySeconds int
	// Project is the GCS project ID. If empty, uses Application Default Credentials.
	Project string
	// Endpoint is a custom GCS JSON API endpoint (an emulator such as
	// fake-gcs-server, or a private endpoint), e.g. "http://localhost:4443".
	// Signed URLs then use its host. Empty means storage.googleapis.com.
	Endpoint string
	// CredentialsFile is a service-account key file used for API calls and
	// for signing URLs. Empty means Application Default Credentials
	// (GOOGLE_APPLICATION_CREDENTIALS, workload identity, the metadata
	// server, which signs through IAM signBlob).
	CredentialsFile string
	// AllowPlainHTTP lets [GCSStorage.ValidateStorageURL] accept http:// URLs
	// on the configured endpoint (VGI_STORAGE_ALLOW_PLAIN_HTTP).
	AllowPlainHTTP bool
	// StallTimeout fails a transfer that sends or receives no byte for this
	// long; a slow transfer that keeps moving is never cut off (there is no
	// whole-request deadline). Default: VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS,
	// else 30s. The connect timeout is 30s. Failed requests (connection
	// errors, stalls, 408, 429, 5xx) are retried: 3 attempts, exponential
	// backoff from 250ms capped at 5s (on top of the client library's own
	// retries of idempotent requests).
	StallTimeout time.Duration
}

// GCSStorage implements vgirpc.ExternalStorage, vgirpc.UploadURLProvider and
// vgirpc.ObjectStore using Google Cloud Storage.
type GCSStorage struct {
	bucket        string
	prefix        string
	presignExpiry time.Duration
	stall         time.Duration
	client        *storage.Client
	endpoint      *url.URL // nil: storage.googleapis.com
	allowHTTP     bool
	signer        *storage.SignedURLOptions // GoogleAccessID + PrivateKey from CredentialsFile, else nil (ADC detection)

	verifyMu sync.Mutex
	verified bool
}

// maxSignedURLExpiry is the V4 signed-URL limit (7 days).
const maxSignedURLExpiry = 7 * 24 * time.Hour

// NewGCSStorage creates a new GCS-backed ExternalStorage.
func NewGCSStorage(bucket string, cfg GCSConfig) (*GCSStorage, error) {
	if strings.TrimSpace(bucket) == "" {
		return nil, errors.New("GCS bucket is empty")
	}
	prefix := cfg.Prefix
	if prefix == "" && !cfg.ExactPrefix {
		prefix = "vgi-rpc/"
	}
	expiry := time.Duration(cfg.PresignExpirySeconds) * time.Second
	if expiry <= 0 {
		expiry = time.Hour
	}
	expiry = min(expiry, maxSignedURLExpiry)
	var endpoint *url.URL
	if cfg.Endpoint != "" {
		u, err := url.Parse(strings.TrimRight(cfg.Endpoint, "/"))
		if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("GCS endpoint %q: want http(s)://host[:port]", cfg.Endpoint)
		}
		endpoint = u
	}
	var signer *storage.SignedURLOptions
	if cfg.CredentialsFile != "" {
		var err error
		if signer, err = signerFromKeyFile(cfg.CredentialsFile); err != nil {
			return nil, err
		}
	}

	stall := stallTimeout(cfg.StallTimeout)
	client, err := newStorageClient(context.Background(), stall, cfg.Endpoint, cfg.CredentialsFile)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}

	return &GCSStorage{
		bucket:        bucket,
		prefix:        prefix,
		presignExpiry: expiry,
		stall:         stall,
		client:        client,
		endpoint:      endpoint,
		allowHTTP:     cfg.AllowPlainHTTP,
		signer:        signer,
	}, nil
}

// signerFromKeyFile reads a service-account key file's client_email and
// private_key for signing URLs.
func signerFromKeyFile(path string) (*storage.SignedURLOptions, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("GCS credentials file: %w", err)
	}
	var key struct {
		Type        string `json:"type"`
		ClientEmail string `json:"client_email"`
		PrivateKey  string `json:"private_key"`
	}
	if err := json.Unmarshal(raw, &key); err != nil {
		return nil, fmt.Errorf("GCS credentials file %s is not JSON", path)
	}
	if key.ClientEmail == "" || key.PrivateKey == "" {
		return nil, fmt.Errorf("GCS credentials file %s is not a service-account key (no client_email/private_key); signed URLs need one, or ADC with IAM signBlob", path)
	}
	return &storage.SignedURLOptions{GoogleAccessID: key.ClientEmail, PrivateKey: []byte(key.PrivateKey)}, nil
}

// signedURL signs a V4 URL for key on the configured endpoint.
func (s *GCSStorage) signedURL(key string, opts storage.SignedURLOptions) (string, error) {
	opts.Scheme = storage.SigningSchemeV4
	opts.Expires = time.Now().Add(s.presignExpiry)
	if s.signer != nil {
		opts.GoogleAccessID, opts.PrivateKey = s.signer.GoogleAccessID, s.signer.PrivateKey
	}
	if s.endpoint != nil {
		opts.Hostname = s.endpoint.Host
		opts.Insecure = s.endpoint.Scheme == "http"
		opts.Style = storage.PathStyle()
	}
	return s.client.Bucket(s.bucket).SignedURL(key, &opts)
}

// newStorageClient builds a GCS client whose HTTP transport has the 30 s
// connect timeout and the stall timeout (and no whole-request deadline),
// authenticated with Application Default Credentials -- or unauthenticated
// against STORAGE_EMULATOR_HOST, as storage.NewClient itself does -- and
// the reference retry budget (3 attempts, 250ms..5s backoff).
func newStorageClient(ctx context.Context, stall time.Duration, endpoint, credentialsFile string) (*storage.Client, error) {
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
		authOpts := []option.ClientOption{option.WithScopes(storage.ScopeFullControl, "https://www.googleapis.com/auth/cloud-platform")}
		if credentialsFile != "" {
			authOpts = append(authOpts, option.WithAuthCredentialsFile(option.ServiceAccount, credentialsFile))
		}
		authed, err := htransport.NewTransport(ctx, base, authOpts...)
		switch {
		case err == nil:
			rt = authed
		case endpoint != "" && credentialsFile == "":
			// A custom endpoint without credentials anywhere: an emulator.
		default:
			return nil, err
		}
	}
	clientOpts := []option.ClientOption{option.WithHTTPClient(&http.Client{Transport: rt})}
	if endpoint != "" {
		clientOpts = append(clientOpts, option.WithEndpoint(strings.TrimRight(endpoint, "/")+"/storage/v1/"))
	}
	client, err := storage.NewClient(ctx, clientOpts...)
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
	if err := s.ensureVerified(context.Background()); err != nil {
		return "", err
	}
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
	signed, err := s.signedURL(key, storage.SignedURLOptions{Method: "GET"})
	if err != nil {
		return "", fmt.Errorf("GCS signed URL: %w", err)
	}

	return signed, nil
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
	if err := s.ensureVerified(context.Background()); err != nil {
		return "", err
	}
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
	if err := s.ensureVerified(context.Background()); err != nil {
		return nil, err
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

// GenerateUploadURL implements vgirpc.UploadURLProvider: a V4 signed PUT for
// exactly size bytes (x-goog-content-length-range: size,size and
// Content-Type: application/octet-stream are signed headers the client must
// send), a signed GET for the same object, and the object name as its
// ObjectID. GCS cannot verify a SHA-256 at storage: the server's
// vgi_rpc.location.sha256 check on the pointer (and the digest check on an
// argument reference) catches a wrong body. sha256Hex is validated.
func (s *GCSStorage) GenerateUploadURL(size int64, sha256Hex string) (vgirpc.UploadURL, error) {
	if size < 0 {
		return vgirpc.UploadURL{}, fmt.Errorf("upload size %d is negative", size)
	}
	if raw, err := hex.DecodeString(sha256Hex); err != nil || len(raw) != 32 || strings.ToLower(sha256Hex) != sha256Hex {
		return vgirpc.UploadURL{}, errors.New("sha256 must be 64 lowercase hex characters")
	}
	if err := s.ensureVerified(context.Background()); err != nil {
		return vgirpc.UploadURL{}, err
	}
	key := s.prefix + uuid.New().String() + ".arrow"
	lengthRange := strconv.FormatInt(size, 10) + "," + strconv.FormatInt(size, 10)
	put, err := s.signedURL(key, storage.SignedURLOptions{
		Method:      "PUT",
		ContentType: "application/octet-stream",
		Headers:     []string{"x-goog-content-length-range:" + lengthRange},
	})
	if err != nil {
		return vgirpc.UploadURL{}, fmt.Errorf("GCS signed PUT URL: %w", err)
	}
	get, err := s.signedURL(key, storage.SignedURLOptions{Method: "GET"})
	if err != nil {
		return vgirpc.UploadURL{}, fmt.Errorf("GCS signed GET URL: %w", err)
	}
	return vgirpc.UploadURL{
		UploadURL:   put,
		DownloadURL: get,
		ExpiresAt:   time.Now().Add(s.presignExpiry).UTC(),
		Headers: map[string]string{
			"Content-Type":                "application/octet-stream",
			"x-goog-content-length-range": lengthRange,
		},
		ObjectID: key,
	}, nil
}

// ValidateStorageURL accepts only https:// URLs on the configured endpoint
// (storage.googleapis.com without one), or http:// there with
// AllowPlainHTTP. vgirpc uses it to pin inbound pointer fetches.
func (s *GCSStorage) ValidateStorageURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return errors.New("invalid storage URL")
	}
	if u.User != nil {
		return errors.New("storage URL userinfo is not allowed")
	}
	want := &url.URL{Scheme: "https", Host: "storage.googleapis.com"}
	if s.endpoint != nil {
		want = s.endpoint
	}
	if u.Scheme == "http" && !s.allowHTTP {
		return errors.New("plain http storage URL refused (set VGI_STORAGE_ALLOW_PLAIN_HTTP=1 to allow it for the configured endpoint)")
	}
	if u.Scheme != want.Scheme || hostPort(u) != hostPort(want) {
		return fmt.Errorf("URL host %q is not the configured GCS endpoint", u.Host)
	}
	return nil
}

func hostPort(u *url.URL) string {
	port := u.Port()
	if port == "" {
		port = map[string]string{"http": "80", "https": "443"}[u.Scheme]
	}
	return strings.ToLower(u.Hostname()) + ":" + port
}

// Verify checks the bucket (its metadata, which needs storage.buckets.get)
// and explains a failure: missing bucket or access denied.
func (s *GCSStorage) Verify(ctx context.Context) error {
	_, err := s.client.Bucket(s.bucket).Attrs(ctx)
	if err == nil {
		s.verifyMu.Lock()
		s.verified = true
		s.verifyMu.Unlock()
		return nil
	}
	if errors.Is(err, storage.ErrBucketNotExist) {
		return fmt.Errorf("GCS bucket %q does not exist: check VGI_STORAGE_GCS_BUCKET", s.bucket)
	}
	var gerr interface{ HTTPCode() int }
	if errors.As(err, &gerr) && (gerr.HTTPCode() == http.StatusForbidden || gerr.HTTPCode() == http.StatusUnauthorized) {
		return fmt.Errorf("GCS access denied to bucket %q: the credentials need storage.buckets.get, storage.objects.get and storage.objects.create on it", s.bucket)
	}
	if strings.Contains(err.Error(), "403") {
		return fmt.Errorf("GCS access denied to bucket %q: the credentials need storage.buckets.get, storage.objects.get and storage.objects.create on it", s.bucket)
	}
	return fmt.Errorf("GCS bucket %q check failed: %w", s.bucket, err)
}

func (s *GCSStorage) ensureVerified(ctx context.Context) error {
	s.verifyMu.Lock()
	done := s.verified
	s.verifyMu.Unlock()
	if done {
		return nil
	}
	return s.Verify(ctx)
}

// Compile-time checks: GCSStorage is every storage role protocol 3.4 asks of
// a backend.
var (
	_ vgirpc.ExternalStorage   = (*GCSStorage)(nil)
	_ vgirpc.UploadURLProvider = (*GCSStorage)(nil)
	_ vgirpc.ObjectStore       = (*GCSStorage)(nil)
)

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
