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
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

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

// Ensure S3Storage implements ExternalStorage at compile time.
var _ vgirpc.ExternalStorage = (*S3Storage)(nil)

// generateUUID returns a random UUID string.
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = bytes.NewReader([]byte(fmt.Sprintf("%d", time.Now().UnixNano()))).Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
