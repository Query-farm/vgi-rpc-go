// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package vgirpc

// physicalMemory is unknown off Linux; the object cache bound then defaults to
// 1 GiB (cgroup limits are Linux-only too).
func physicalMemory() int64 { return 0 }
