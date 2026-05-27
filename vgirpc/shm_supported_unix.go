// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package vgirpc

// shmSupported reports whether this build can use the POSIX shared-memory
// side-channel (shm_open/mmap via cgo). The !unix stub sets it false; see
// shm_stub.go. Reported to clients via __transport_options__.
const shmSupported = true
