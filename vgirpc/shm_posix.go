// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package vgirpc

/*
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

// Wrapper because cgo can't take the address of a varargs C function.
static int go_shm_open(const char* name, int oflag, mode_t mode) {
    return shm_open(name, oflag, mode);
}

static int go_shm_unlink(const char* name) {
    return shm_unlink(name);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// ShmCreate creates a new POSIX shm segment of `size` bytes (must be
// strictly greater than ShmHeaderSize). The returned segment owns the OS
// object — Close calls shm_unlink.
func ShmCreate(size int) (*ShmSegment, error) {
	if size <= ShmHeaderSize {
		return nil, fmt.Errorf("shm size must be > %d, got %d", ShmHeaderSize, size)
	}
	for i := 0; i < 16; i++ {
		name := makeShmName()
		seg, err := shmTryCreate(name, size)
		if err == nil {
			if err := seg.initializeHeader(); err != nil {
				_ = seg.Close()
				return nil, err
			}
			return seg, nil
		}
		if !errors.Is(err, unix.EEXIST) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("shm_open exhausted name retries")
}

// ShmAttach attaches to an existing segment by name. If track is false
// (server-side use) Close does NOT call shm_unlink.
func ShmAttach(name string, size int, track bool) (*ShmSegment, error) {
	if size <= ShmHeaderSize {
		return nil, fmt.Errorf("shm size must be > %d, got %d", ShmHeaderSize, size)
	}
	fd, err := shmOpenForAttach(name)
	if err != nil {
		return nil, err
	}
	data, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	_ = unix.Close(fd)
	if err != nil {
		return nil, fmt.Errorf("mmap: %w", err)
	}
	seg := &ShmSegment{name: name, size: size, data: data, track: track}
	if err := seg.validateHeader(); err != nil {
		_ = seg.Close()
		return nil, err
	}
	return seg, nil
}

// Close munmaps the region and, if this segment was the owner, calls
// shm_unlink. Safe to call multiple times.
func (s *ShmSegment) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	var firstErr error
	if s.data != nil {
		if err := unix.Munmap(s.data); err != nil {
			firstErr = err
		}
		s.data = nil
	}
	if s.track && s.name != "" {
		if err := shmUnlink(s.name); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// shmOpen wraps shm_open() via cgo. flags is a bitmask of O_* values
// from <fcntl.h>; mode is the POSIX permission bits (used only when
// O_CREAT is set).
func shmOpen(name string, flags int, mode uint32) (int, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	fd, err := C.go_shm_open(cname, C.int(flags), C.mode_t(mode))
	if fd < 0 {
		if err == nil {
			err = syscall.EINVAL
		}
		return -1, err
	}
	return int(fd), nil
}

func shmUnlink(name string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	rc, err := C.go_shm_unlink(cname)
	if rc < 0 {
		if err == nil {
			err = syscall.EINVAL
		}
		return err
	}
	return nil
}

// shmTryCreate calls shm_open(O_CREAT|O_EXCL|O_RDWR) + ftruncate + mmap.
// Returns *ShmSegment with track=true on success, an error wrapping
// EEXIST if another segment already owns the name, or a generic error
// otherwise.
func shmTryCreate(name string, size int) (*ShmSegment, error) {
	fd, err := shmOpen(name, unix.O_CREAT|unix.O_EXCL|unix.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Ftruncate(fd, int64(size)); err != nil {
		_ = unix.Close(fd)
		_ = shmUnlink(name)
		return nil, fmt.Errorf("ftruncate: %w", err)
	}
	data, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	_ = unix.Close(fd)
	if err != nil {
		_ = shmUnlink(name)
		return nil, fmt.Errorf("mmap: %w", err)
	}
	return &ShmSegment{name: name, size: size, data: data, track: true}, nil
}

// shmOpenForAttach calls shm_open(O_RDWR) on `name`, falling back to
// `/`+name on POSIX systems where the leading slash was stripped at the
// Python boundary.
func shmOpenForAttach(name string) (int, error) {
	fd, err := shmOpen(name, unix.O_RDWR, 0o600)
	if err == nil {
		return fd, nil
	}
	if len(name) == 0 || name[0] == '/' {
		return -1, fmt.Errorf("shm_open(attach) %q: %w", name, err)
	}
	fd2, err2 := shmOpen("/"+name, unix.O_RDWR, 0o600)
	if err2 == nil {
		return fd2, nil
	}
	return -1, fmt.Errorf("shm_open(attach) %q: %v; with leading slash: %v", name, err, err2)
}

// makeShmName returns a process-and-time-derived segment name, matching
// the leading-slash convention used by Rust and Python.
func makeShmName() string {
	pid := os.Getpid()
	nanos := unix.TimespecToNsec(unixNow())
	// Add an atomic counter so rapid successive calls in the same nanosecond
	// don't collide.
	c := shmNameCounter.Add(1)
	return fmt.Sprintf("/vgi_rpc_%x_%x_%x", pid, nanos, c)
}

func unixNow() unix.Timespec {
	var ts unix.Timespec
	_ = unix.ClockGettime(unix.CLOCK_REALTIME, &ts)
	return ts
}
