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
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/uuid"

	"github.com/Query-farm/vgi-rpc/vgirpc"
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
var _ vgirpc.ExternalStorage = (*GCSStorage)(nil)
