// Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package conformance provides internal test fixtures for the vgi_rpc
// protocol conformance test suite. It registers a comprehensive set of
// RPC methods — unary, producer, and exchange — that exercise every
// feature of the protocol: scalar types, collections, nullable fields,
// ArrowSerializable round-trips, defaults, enums, error propagation,
// client-directed logging, stream headers, and bidirectional exchange.
//
// The only entry point intended for external use is [RegisterMethods],
// which registers all conformance methods on a [vgirpc.Server]. The
// domain types [Status], [Point], [BoundingBox], [AllTypes], and
// [ConformanceHeader] are exported because they serve as examples of
// [vgirpc.ArrowSerializable] implementations.
package conformance
