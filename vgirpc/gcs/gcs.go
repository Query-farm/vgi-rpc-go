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
	"time"

	"cloud.google.com/go/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/uuid"

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
}

// GCSStorage implements vgirpc.ExternalStorage using Google Cloud Storage.
type GCSStorage struct {
	bucket        string
	prefix        string
	presignExpiry time.Duration
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

	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}

	return &GCSStorage{
		bucket:        bucket,
		prefix:        prefix,
		presignExpiry: expiry,
		client:        client,
	}, nil
}

// Upload implements vgirpc.ExternalStorage.
func (s *GCSStorage) Upload(data []byte, schema *arrow.Schema, contentEncoding string) (string, error) {
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
	ctx := context.Background()
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
	data, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
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
