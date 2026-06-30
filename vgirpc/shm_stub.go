// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build !unix && !windows

// Stub implementation of the shared-memory side-channel for non-POSIX
// platforms. Functions return nil/false so callers transparently fall
// back to the pipe transport.

package vgirpc

import (
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
)

// shmSupported reports whether this build can use the POSIX shared-memory
// side-channel. False on non-unix platforms (see shm.go for the unix build).
const shmSupported = false

// ShmHeaderSize is exported as a public constant on every platform; the
// stub still defines the same value so user code can reference it
// portably (e.g. for sizing decisions).
const ShmHeaderSize = 65_536

// ShmSegment is unimplemented on non-POSIX platforms.
type ShmSegment struct{}

// ShmCreate is unimplemented on non-POSIX platforms.
func ShmCreate(size int) (*ShmSegment, error) {
	return nil, errors.New("shared-memory transport requires a POSIX platform")
}

// ShmAttach is unimplemented on non-POSIX platforms.
func ShmAttach(name string, size int, track bool) (*ShmSegment, error) {
	return nil, errors.New("shared-memory transport requires a POSIX platform")
}

// Name is a no-op on non-POSIX platforms.
func (*ShmSegment) Name() string { return "" }

// Size is a no-op on non-POSIX platforms.
func (*ShmSegment) Size() int { return 0 }

// Close is a no-op on non-POSIX platforms.
func (*ShmSegment) Close() error { return nil }

// Reset is a no-op on non-POSIX platforms.
func (*ShmSegment) Reset() {}

// AllocateAndWrite always reports the segment cannot fit the batch.
func (*ShmSegment) AllocateAndWrite(arrow.RecordBatch) (uint64, int, bool, error) {
	return 0, 0, false, nil
}

// ReadBatch is unimplemented on non-POSIX platforms.
func (*ShmSegment) ReadBatch(uint64, int, *arrow.Schema) (arrow.RecordBatch, error) {
	return nil, errors.New("shared-memory transport requires a POSIX platform")
}

// FreeOffset is a no-op on non-POSIX platforms.
func (*ShmSegment) FreeOffset(uint64) error { return nil }

// IsShmPointerBatch returns false on non-POSIX platforms.
func IsShmPointerBatch(arrow.RecordBatch) bool { return false }

// ResolveShmBatch returns the input batch unchanged on non-POSIX platforms.
func ResolveShmBatch(batch arrow.RecordBatch, _ *ShmSegment) (arrow.RecordBatch, uint64, bool, error) {
	return batch, 0, false, nil
}

// MaybeWriteToShm returns the input batch unchanged on non-POSIX platforms.
func MaybeWriteToShm(batch arrow.RecordBatch, _ *ShmSegment) (arrow.RecordBatch, bool, error) {
	return batch, false, nil
}

// shmAttachFromMetadata always returns nil on non-POSIX platforms.
func shmAttachFromMetadata(map[string]string, bool) *ShmSegment {
	return nil
}
