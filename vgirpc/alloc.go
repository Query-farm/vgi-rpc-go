// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build !leakcheck

package vgirpc

import "github.com/apache/arrow-go/v18/arrow/memory"

// defaultAllocator returns the Arrow memory allocator used by internal
// builders. The default build returns a fresh GoAllocator per call,
// matching the previous behavior of inline memory.NewGoAllocator() calls.
//
// Build with -tags leakcheck to swap in a single shared CheckedAllocator
// that tracks allocations and reports leaks. See vgirpc/alloc_leakcheck.go.
func defaultAllocator() memory.Allocator {
	return memory.NewGoAllocator()
}

// LeakCheckSummary is unused in default builds. It returns an empty string.
func LeakCheckSummary() string { return "" }
