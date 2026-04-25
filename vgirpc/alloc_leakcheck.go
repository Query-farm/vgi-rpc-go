// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build leakcheck

package vgirpc

import (
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// leakCheckOnce gates construction of the shared checked allocator.
var (
	leakCheckOnce sync.Once
	checked       *memory.CheckedAllocator
)

func leakCheckAllocator() *memory.CheckedAllocator {
	leakCheckOnce.Do(func() {
		checked = memory.NewCheckedAllocator(memory.NewGoAllocator())
	})
	return checked
}

// defaultAllocator returns the shared CheckedAllocator. Every internal
// builder routes through this single instance so LeakCheckSummary can
// report cumulative outstanding allocations across the whole process.
func defaultAllocator() memory.Allocator {
	return leakCheckAllocator()
}

// LeakCheckSummary returns a human-readable line describing outstanding
// allocations in the shared CheckedAllocator. Suitable for printing on
// process exit when running with -tags leakcheck.
func LeakCheckSummary() string {
	c := leakCheckAllocator()
	return fmt.Sprintf("vgirpc leakcheck: outstanding=%d bytes", c.CurrentAlloc())
}
