// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgigcs

import (
	"context"
	"fmt"
	"net/url"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
)

// FunctionStore is the bucket's function-storage area, <prefix>fs/: large
// function-storage values (vgi's VGI_STORAGE_EXTERNALIZE) live here, grouped
// by scope so a whole scope can be dropped. Deletes are best effort; a
// lifecycle rule deleting fs/ after 2 days is the backstop.
type FunctionStore struct {
	s      *GCSStorage
	prefix string
}

// FunctionStore returns the function-storage view of this bucket.
func (s *GCSStorage) FunctionStore() *FunctionStore {
	return &FunctionStore{s: s, prefix: s.root + "fs/"}
}

func (f *FunctionStore) scopePrefix(scope string) string {
	if scope == "" {
		return f.prefix
	}
	return f.prefix + url.PathEscape(scope) + "/"
}

// PutObjectScoped stores data under a fresh name in scope ("" for none) and
// returns the name. It never overwrites (DoesNotExist).
func (f *FunctionStore) PutObjectScoped(ctx context.Context, data []byte, scope string) (string, error) {
	if err := f.s.ensureVerified(ctx); err != nil {
		return "", err
	}
	var key string
	err := withStallRetries(func() error {
		key = f.scopePrefix(scope) + uuid.New().String() + ".bin"
		w := f.s.client.Bucket(f.s.bucket).Object(key).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
		w.ContentType = "application/octet-stream"
		if _, err := w.Write(data); err != nil {
			w.Close()
			return fmt.Errorf("GCS object write: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("GCS object close: %w", err)
		}
		return nil
	})
	return key, err
}

// Get fetches an object by name ([GCSStorage.Get]).
func (f *FunctionStore) Get(objectID string, maxBytes int64) ([]byte, error) {
	return f.s.Get(objectID, maxBytes)
}

// DeleteObjects deletes names, best effort (failures are ignored). Returns
// how many deletes succeeded.
func (f *FunctionStore) DeleteObjects(ctx context.Context, keys []string) (int, error) {
	n := 0
	for _, key := range keys {
		if err := f.s.client.Bucket(f.s.bucket).Object(key).Delete(ctx); err == nil {
			n++
		}
	}
	return n, nil
}

// DeleteScope deletes every object of scope, best effort (it lists the
// scope's prefix). Returns how many deletes succeeded.
func (f *FunctionStore) DeleteScope(ctx context.Context, scope string) (int, error) {
	if scope == "" {
		return 0, nil
	}
	var keys []string
	it := f.s.client.Bucket(f.s.bucket).Objects(ctx, &storage.Query{Prefix: f.scopePrefix(scope)})
	for {
		attrs, err := it.Next()
		if err != nil {
			// iterator.Done, or a listing failure: delete what was listed.
			break
		}
		keys = append(keys, attrs.Name)
	}
	return f.DeleteObjects(ctx, keys)
}
