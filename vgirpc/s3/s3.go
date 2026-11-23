// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package vgis3 provides an S3-backed ExternalStorage implementation for vgi-rpc.
//
// Usage:
//
//	storage := vgis3.NewS3Storage("my-bucket", vgis3.S3Config{
//	    Prefix: "vgi-rpc/",
//	})
//	server.SetExternalLocation(vgirpc.DefaultExternalLocationConfig(storage))
package vgis3

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// S3Config configures the S3 storage backend.
type S3Config struct {
	// Prefix is the S3 key prefix for uploaded objects. Default: "vgi-rpc/".
	Prefix string
	// PresignExpirySeconds is the lifetime of pre-signed GET URLs. Default: 3600 (1 hour).
	PresignExpirySeconds int
	// Region overrides the AWS region. If empty, uses default config.
	Region string
	// EndpointURL overrides the S3 endpoint (for MinIO, LocalStack, etc.).
	EndpointURL string
}

// S3Storage implements vgirpc.ExternalStorage using AWS S3.
type S3Storage struct {
	bucket        string
	prefix        string
	presignExpiry time.Duration
	client        *s3.Client
	presignClient *s3.PresignClient
}

// NewS3Storage creates a new S3-backed ExternalStorage.
func NewS3Storage(bucket string, cfg S3Config) (*S3Storage, error) {
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = "vgi-rpc/"
	}
	expiry := time.Duration(cfg.PresignExpirySeconds) * time.Second
	if expiry <= 0 {
		expiry = time.Hour
	}

	// Load AWS config
	opts := []func(*config.LoadOptions) error{}
	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}
	awsCfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	s3Opts := []func(*s3.Options){}
	if cfg.EndpointURL != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)
	presignClient := s3.NewPresignClient(client)

	return &S3Storage{
		bucket:        bucket,
		prefix:        prefix,
		presignExpiry: expiry,
		client:        client,
		presignClient: presignClient,
	}, nil
}

// Upload implements vgirpc.ExternalStorage.
func (s *S3Storage) Upload(data []byte, schema *arrow.Schema, contentEncoding string) (string, error) {
	key := s.prefix + generateUUID()

	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/vnd.apache.arrow.stream"),
	}
	if contentEncoding != "" {
		input.ContentEncoding = aws.String(contentEncoding)
	}

	_, err := s.client.PutObject(context.Background(), input)
	if err != nil {
		return "", fmt.Errorf("S3 PutObject: %w", err)
	}

	// Generate pre-signed GET URL
	presignInput := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	presigned, err := s.presignClient.PresignGetObject(context.Background(), presignInput,
		s3.WithPresignExpires(s.presignExpiry))
	if err != nil {
		return "", fmt.Errorf("S3 presign: %w", err)
	}

	return presigned.URL, nil
}

// defaultMaxFetchBytes is the Get cap when the caller passes none (256 MiB,
// vgirpc's default inbound fetch cap).
const defaultMaxFetchBytes int64 = 256 * 1024 * 1024

// ErrObjectMissing is returned by Get when S3 reports the key absent
// (NoSuchKey / 404). It carries an ObjectMissing() method, which
// vgirpc.IsObjectMissing recognizes, so StateRef and argument-reference
// resolution map it to state_ref_missing / arguments_ref_missing.
var ErrObjectMissing error = objectMissingError{}

type objectMissingError struct{}

func (objectMissingError) Error() string       { return "S3 object is gone" }
func (objectMissingError) ObjectMissing() bool { return true }

// PutObject stores data under a fresh key and returns the key: the
// vgirpc.ObjectStore contract of protocol 3.4 (StateRef objects). A SigV4
// PutObject with ContentLength and ChecksumSHA256, so S3/R2 verify the body.
// Objects carry no TTL: operators MUST set a lifecycle rule of at least one
// day on the prefix.
func (s *S3Storage) PutObject(data []byte) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("S3 object key: %w", err)
	}
	key := s.prefix + hex.EncodeToString(raw[:]) + ".bin"
	sum := sha256.Sum256(data)
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:         aws.String(s.bucket),
		Key:            aws.String(key),
		Body:           bytes.NewReader(data),
		ContentLength:  aws.Int64(int64(len(data))),
		ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(sum[:])),
		ContentType:    aws.String("application/octet-stream"),
	})
	if err != nil {
		return "", fmt.Errorf("S3 PutObject: %w", err)
	}
	return key, nil
}

// Get fetches the object a key names (vgirpc.ObjectStore), refusing more than
// maxBytes (<= 0: 256 MiB). A missing key is [ErrObjectMissing]. The caller
// verifies size and digest.
func (s *S3Storage) Get(objectID string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = defaultMaxFetchBytes
	}
	resp, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectID),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "NoSuchKey", "NotFound", "404":
				return nil, ErrObjectMissing
			}
		}
		return nil, fmt.Errorf("S3 GetObject: %w", err)
	}
	defer resp.Body.Close()
	if resp.ContentLength != nil && *resp.ContentLength > maxBytes {
		return nil, fmt.Errorf("S3 object is %d bytes, over max_fetch_bytes (%d)", *resp.ContentLength, maxBytes)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("S3 GetObject read: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("S3 object exceeds max_fetch_bytes (%d)", maxBytes)
	}
	return data, nil
}

// Ensure S3Storage implements ExternalStorage at compile time. (It also
// implements vgirpc.ObjectStore; that check waits for the core release that
// declares the interface, since this module pins a published core.)
var _ vgirpc.ExternalStorage = (*S3Storage)(nil)

// generateUUID returns a random UUID string.
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = bytes.NewReader([]byte(fmt.Sprintf("%d", time.Now().UnixNano()))).Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
