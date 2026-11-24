// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgis3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// FunctionStore is the bucket's function-storage area, <prefix>fs/: large
// function-storage values (vgi's VGI_STORAGE_EXTERNALIZE) live here, grouped
// by scope so a whole scope can be dropped. Deletes are best effort; a
// lifecycle rule expiring fs/ after 2 days is the backstop.
type FunctionStore struct {
	s      *S3Storage
	prefix string
}

// FunctionStore returns the function-storage view of this bucket.
func (s *S3Storage) FunctionStore() *FunctionStore {
	return &FunctionStore{s: s, prefix: s.root + "fs/"}
}

// scopePrefix is the key prefix of one (shard, execution) scope.
func (f *FunctionStore) scopePrefix(scope string) string {
	if scope == "" {
		return f.prefix
	}
	return f.prefix + url.PathEscape(scope) + "/"
}

// PutObjectScoped stores data under a fresh key in scope ("" for none) and
// returns the key, sending its size and (unless ChecksumNone) SHA-256.
func (f *FunctionStore) PutObjectScoped(ctx context.Context, data []byte, scope string) (string, error) {
	if err := f.s.ensureVerified(ctx); err != nil {
		return "", err
	}
	key, err := randomKey(f.scopePrefix(scope), ".bin")
	if err != nil {
		return "", err
	}
	input := &s3.PutObjectInput{
		Bucket:        aws.String(f.s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String("application/octet-stream"),
	}
	if f.s.checksum == ChecksumSHA256 {
		sum := sha256.Sum256(data)
		input.ChecksumSHA256 = aws.String(base64.StdEncoding.EncodeToString(sum[:]))
	}
	if _, err := f.s.client.PutObject(ctx, input); err != nil {
		return "", fmt.Errorf("S3 PutObject: %w", err)
	}
	return key, nil
}

// Get fetches an object by key ([S3Storage.Get]); a missing one is
// [ErrObjectMissing].
func (f *FunctionStore) Get(objectID string, maxBytes int64) ([]byte, error) {
	return f.s.Get(objectID, maxBytes)
}

// DeleteObjects deletes keys one by one, best effort: failures are ignored
// (the lifecycle rule removes what is left). Returns how many deletes
// succeeded.
func (f *FunctionStore) DeleteObjects(ctx context.Context, keys []string) (int, error) {
	n := 0
	for _, key := range keys {
		if _, err := f.s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(f.s.bucket), Key: aws.String(key)}); err == nil {
			n++
		}
	}
	return n, nil
}

// DeleteScope deletes every object of scope, best effort (it lists the
// scope's prefix, which needs s3:ListBucket). Returns how many deletes
// succeeded.
func (f *FunctionStore) DeleteScope(ctx context.Context, scope string) (int, error) {
	if scope == "" {
		return 0, nil
	}
	var keys []string
	pager := s3.NewListObjectsV2Paginator(f.s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(f.s.bucket), Prefix: aws.String(f.scopePrefix(scope)),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			break
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return f.DeleteObjects(ctx, keys)
}
