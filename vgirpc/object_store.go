// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import "errors"

// ObjectStore is the storage contract of protocol 3.4 (STATE_REF_V1.md §10):
// write one object and read it back by id. It is a separate interface next to
// [ExternalStorage] rather than new methods on it, so a third-party
// ExternalStorage keeps compiling; "storage is configured" for StateRef and
// argument references means the server's external storage also implements
// ObjectStore. Every in-tree backend does (the blob vendor, vgirpc/s3,
// vgirpc/gcs and the conformance fake storage).
//
// Objects carry no TTL and the server never deletes them: operators MUST set a
// bucket lifecycle rule of at least one day on the prefix these objects are
// written under.
//
// Implementations must be safe for concurrent use.
type ObjectStore interface {
	// PutObject stores data as one rpc-purpose object and returns the
	// backend's identifier for it (1-1024 bytes of UTF-8). It must send the
	// size and SHA-256 so storage verifies the body where it can.
	PutObject(data []byte) (string, error)
	// Get fetches an object by the id PutObject (or an upload grant)
	// returned. The download is minted server-side from the id, pinned to the
	// storage's hosts like every other storage fetch, and refused past
	// maxBytes (<= 0 means the backend's default cap). A missing object is an
	// error for which [IsObjectMissing] reports true; any other failure is
	// transient. Callers verify size and SHA-256 themselves.
	Get(objectID string, maxBytes int64) ([]byte, error)
}

// ErrObjectMissing is the sentinel an [ObjectStore] returns (directly or
// wrapped) when storage reports the object absent: a 404, or the blob
// vendor's not_found / not_uploaded.
var ErrObjectMissing = errors.New("object is missing from storage")

// IsObjectMissing reports whether err means "storage says the object is
// gone": [ErrObjectMissing] anywhere in its chain, or any error in the chain
// with an ObjectMissing() bool method returning true. The method form lets a
// backend module built against an older vgirpc release report missing objects
// without importing the sentinel.
func IsObjectMissing(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrObjectMissing) {
		return true
	}
	var marker interface{ ObjectMissing() bool }
	return errors.As(err, &marker) && marker.ObjectMissing()
}
