// Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package vgirpc implements a Go server for the vgi_rpc protocol, an
// Apache Arrow IPC-based RPC framework for high-performance data services.
//
// The protocol encodes all parameters and results as Arrow RecordBatch
// messages with per-batch custom metadata carrying method names, request
// IDs, log messages, and error information. This enables efficient
// columnar data transfer with zero-copy potential, while keeping the
// control plane human-readable.
//
// # Method types
//
// Three method types are supported:
//
//   - Unary: a single request produces a single response. Register with
//     [Unary] or [UnaryVoid].
//   - Producer: a single request initiates a server-driven stream of
//     output batches. The server calls [ProducerState.Produce] in a
//     lockstep loop until it signals completion via
//     [OutputCollector.Finish]. Register with [Producer] or
//     [ProducerWithHeader].
//   - Exchange: a single request initiates a bidirectional stream where
//     each client-sent input batch produces one output batch via
//     [ExchangeState.Exchange]. Register with [Exchange] or
//     [ExchangeWithHeader].
//
// # Struct tags
//
// Method parameters are declared as Go structs annotated with `vgirpc`
// struct tags. The tag format is:
//
//	`vgirpc:"wire_name[,option[,option...]]"`
//
// Supported options:
//
//   - default=VALUE  — default value when the client omits the parameter
//   - enum           — encode as an Arrow Dictionary (categorical string)
//   - int32          — use Arrow Int32 instead of the default Int64
//   - float32        — use Arrow Float32 instead of the default Float64
//   - binary         — serialize an [ArrowSerializable] value as IPC bytes
//
// Pointer fields (e.g. *string, *int64) become nullable Arrow columns.
//
// # ArrowSerializable
//
// Types that implement the [ArrowSerializable] interface provide their
// own Arrow schema via ArrowSchema(). Fields are mapped to Arrow columns
// using `arrow` struct tags. At the method parameter level these types are
// serialized as binary (embedded IPC stream); when nested inside another
// ArrowSerializable they become Arrow struct columns.
//
// # HTTP transport
//
// [HttpServer] wraps a [Server] and exposes it over HTTP with the
// following URL routes (default prefix /vgi):
//
//	POST /vgi/{method}           — unary call
//	POST /vgi/{method}/init      — stream initialization (producer or exchange)
//	POST /vgi/{method}/exchange  — exchange continuation with state token
//
// All request and response bodies use Content-Type
// application/vnd.apache.arrow.stream. Stateful exchange streams carry an
// HMAC-signed state token in batch custom metadata so the server remains
// stateless between HTTP requests. Call [RegisterStateType] for each
// concrete state type before using HTTP transport.
//
// # Transports
//
// The stdio transport ([Server.RunStdio], [Server.Serve]) reads and
// writes Arrow IPC streams on an io.Reader/io.Writer pair. This is the
// primary transport used by the vgi_rpc Python client for subprocess
// workers.
//
// # Reference implementation
//
// The Python reference implementation lives at
// https://github.com/Query-farm/vgi-rpc.
package vgirpc
