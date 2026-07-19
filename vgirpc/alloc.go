// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build !leakcheck

package vgirpc

import "github.com/apache/arrow-go/v18/arrow/memory"

// goAlloc is the process-wide GoAllocator. GoAllocator is stateless and
// safe for concurrent use, so one instance serves every caller and the
// memory.Allocator interface value is built once rather than per call.
var goAlloc = memory.NewGoAllocator()

// defaultAllocator returns the Arrow memory allocator used by internal
// builders.
//
// Build with -tags leakcheck to swap in a single shared CheckedAllocator
// that tracks allocations and reports leaks. See vgirpc/alloc_leakcheck.go.
func defaultAllocator() memory.Allocator {
	return goAlloc
}

// LeakCheckSummary is unused in default builds. It returns an empty string.
func LeakCheckSummary() string { return "" }
