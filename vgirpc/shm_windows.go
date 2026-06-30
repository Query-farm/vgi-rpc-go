// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package vgirpc

import (
	"errors"
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

// makeShmName generates a unique Win32 file-mapping object name (session-local
// namespace, no leading slash). Used only when this process *creates* a segment
// (client role); a worker attaches by the engine-advertised name.
func makeShmName() string {
	n := shmNameCounter.Add(1)
	return fmt.Sprintf("vgi-rpc-shm-%d-%d", windows.GetCurrentProcessId(), n)
}

// ShmCreate creates a fresh page-file-backed named mapping of `size` bytes.
func ShmCreate(size int) (*ShmSegment, error) {
	if size <= ShmHeaderSize {
		return nil, fmt.Errorf("shm size must be > %d, got %d", ShmHeaderSize, size)
	}
	for i := 0; i < 16; i++ {
		seg, err := shmMap(makeShmName(), size, true /*create*/, true /*track*/)
		if err == nil {
			if err := seg.initializeHeader(); err != nil {
				_ = seg.Close()
				return nil, err
			}
			return seg, nil
		}
		if errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("CreateFileMapping exhausted name retries")
}

// ShmAttach attaches to an existing named mapping. If track is false (server
// role) Close does not destroy the OS object — but on Windows the mapping is
// reclaimed automatically once the last handle closes, so track is moot.
func ShmAttach(name string, size int, track bool) (*ShmSegment, error) {
	if size <= ShmHeaderSize {
		return nil, fmt.Errorf("shm size must be > %d, got %d", ShmHeaderSize, size)
	}
	seg, err := shmMap(name, size, false /*create*/, track)
	if err != nil {
		return nil, err
	}
	if err := seg.validateHeader(); err != nil {
		_ = seg.Close()
		return nil, err
	}
	return seg, nil
}

// shmMap create-or-opens a named page-file-backed mapping and maps a view.
// CreateFileMapping opens the existing object when the name already exists
// (returning ERROR_ALREADY_EXISTS with a valid handle), so it serves both the
// create and attach paths — exactly what Python's mmap(tagname=...) does.
func shmMap(name string, size int, create, track bool) (*ShmSegment, error) {
	namep, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, err
	}
	maxHigh := uint32(uint64(size) >> 32)
	maxLow := uint32(uint64(size) & 0xffffffff)
	h, cerr := windows.CreateFileMapping(windows.InvalidHandle, nil, windows.PAGE_READWRITE, maxHigh, maxLow, namep)
	if h == 0 {
		return nil, fmt.Errorf("CreateFileMapping %q: %w", name, cerr)
	}
	already := errors.Is(cerr, windows.ERROR_ALREADY_EXISTS)
	if create && already {
		windows.CloseHandle(h)
		return nil, windows.ERROR_ALREADY_EXISTS // collision; caller retries
	}
	addr, merr := windows.MapViewOfFile(h, windows.FILE_MAP_WRITE|windows.FILE_MAP_READ, 0, 0, uintptr(size))
	if addr == 0 {
		windows.CloseHandle(h)
		return nil, fmt.Errorf("MapViewOfFile %q: %w", name, merr)
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(addr)), size)
	return &ShmSegment{name: name, size: size, data: data, track: track, handle: uintptr(h), addr: addr}, nil
}

// Close unmaps the view and closes the mapping handle (idempotent).
func (s *ShmSegment) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	var firstErr error
	if s.addr != 0 {
		if err := windows.UnmapViewOfFile(s.addr); err != nil {
			firstErr = err
		}
		s.addr = 0
		s.data = nil
	}
	if s.handle != 0 {
		if err := windows.CloseHandle(windows.Handle(s.handle)); err != nil && firstErr == nil {
			firstErr = err
		}
		s.handle = 0
	}
	return firstErr
}
