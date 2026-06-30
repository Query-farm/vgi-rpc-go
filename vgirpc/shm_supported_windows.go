// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package vgirpc

// shmSupported reports whether this build can use the shared-memory side-channel.
// Windows backs it with a named file mapping (CreateFileMapping/MapViewOfFile);
// reported to clients via __transport_options__.
const shmSupported = true
