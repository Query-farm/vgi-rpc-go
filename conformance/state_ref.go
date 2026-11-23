// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// StateRef, the object cache and argument references (protocol 3.4,
// STATE_REF_V1.md §15). Mirrors state_ref_exchange, object_cache_stats,
// object_cache_age and resolve_argument_ref in the reference's
// vgi_rpc/conformance/_impl.py and _types.py; the shared suite
// (vgi_rpc.conformance._state_ref_pytest) drives them over raw HTTP.

func init() {
	vgirpc.RegisterStateType(&stateRefExchangeState{})
}

// stateRefInputSchema is state_ref_exchange's input: an optional raw StateRef
// resolved in place of the stream's own (how the shared suite presents
// tampered, foreign-stream and foreign-kind tags).
var stateRefInputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "override", Type: arrow.BinaryTypes.Binary, Nullable: true},
}, nil)

// stateRefOutputSchema is one row per exchange: size and SHA-256 of the
// resolved bytes, and the stream's own ref's form and encoded bytes (null
// while in memory).
var stateRefOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "size", Type: arrow.PrimitiveTypes.Int64},
	{Name: "sha256", Type: arrow.BinaryTypes.String},
	{Name: "form", Type: arrow.BinaryTypes.String},
	{Name: "ref", Type: arrow.BinaryTypes.Binary, Nullable: true},
}, nil)

// StateRefPayload is the deterministic state_ref_exchange payload every port
// builds (the reference's state_ref_payload):
//
//   - compressible: "vgi-rpc StateRef conformance <seed> " repeated and
//     truncated to size bytes;
//   - incompressible: SHA-256(seed || u64le(i)) for i = 0, 1, ...
//     concatenated and truncated to size bytes.
func StateRefPayload(size int64, compressible bool, seed string) []byte {
	if size <= 0 {
		return []byte{}
	}
	out := make([]byte, 0, size+32)
	if compressible {
		unit := []byte("vgi-rpc StateRef conformance " + seed + " ")
		for int64(len(out)) < size {
			out = append(out, unit...)
		}
		return out[:size]
	}
	block := make([]byte, len(seed)+8)
	copy(block, seed)
	for i := uint64(0); int64(len(out)) < size; i++ {
		binary.LittleEndian.PutUint64(block[len(seed):], i)
		sum := sha256.Sum256(block)
		out = append(out, sum[:]...)
	}
	return out[:size]
}

type stateRefExchangeParams struct {
	Size          int64  `vgirpc:"size"`
	Compressible  bool   `vgirpc:"compressible"`
	Seed          string `vgirpc:"seed"`
	Misuse        bool   `vgirpc:"misuse"`
	CursorPadding int64  `vgirpc:"cursor_padding"`
}

// stateRefExchange wraps the payload in a StateRef here, in the init handler,
// and resolves it on every exchange.
func stateRefExchange(_ context.Context, ctx *vgirpc.CallContext, p stateRefExchangeParams) (*vgirpc.StreamResult, error) {
	value, err := ctx.StateRef(StateRefPayload(p.Size, p.Compressible, p.Seed))
	if err != nil {
		return nil, err
	}
	return &vgirpc.StreamResult{
		OutputSchema: stateRefOutputSchema,
		InputSchema:  stateRefInputSchema,
		State: &stateRefExchangeState{
			Value:   value,
			Padding: StateRefPayload(p.CursorPadding, false, p.Seed+":padding"),
			Misuse:  p.Misuse,
		},
	}, nil
}

// stateRefExchangeState resolves the stream's StateRef (or a presented one)
// on every exchange. Value is created by StateRef in the init handler and
// copied by every turn; Padding sits in the cursor itself (to drive the
// cursor rule); Misuse makes every exchange call StateRef (refused:
// state_ref_misuse).
type stateRefExchangeState struct {
	Value   vgirpc.StateRef
	Padding []byte
	Misuse  bool
}

func (s *stateRefExchangeState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, ctx *vgirpc.CallContext) error {
	if s.Misuse {
		if _, err := ctx.StateRef([]byte("a continuation may not create a StateRef")); err != nil {
			return err
		}
	}
	ref := s.Value
	if input.NumRows() > 0 && input.NumCols() > 0 {
		if col, ok := input.Column(0).(*array.Binary); ok && !col.IsNull(0) {
			ref = vgirpc.StateRefFromBytes(col.Value(0))
		}
	}
	data, err := ctx.Resolve(ref)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)

	mem := memory.NewGoAllocator()
	sizeB := array.NewInt64Builder(mem)
	defer sizeB.Release()
	shaB := array.NewStringBuilder(mem)
	defer shaB.Release()
	formB := array.NewStringBuilder(mem)
	defer formB.Release()
	refB := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer refB.Release()
	sizeB.Append(int64(len(data)))
	shaB.Append(hex.EncodeToString(sum[:]))
	formB.Append(s.Value.Form())
	if encoded := s.Value.Encoded(); encoded != nil {
		refB.Append(encoded)
	} else {
		refB.AppendNull()
	}
	arrs := []arrow.Array{sizeB.NewArray(), shaB.NewArray(), formB.NewArray(), refB.NewArray()}
	defer func() {
		for _, a := range arrs {
			a.Release()
		}
	}()
	return out.EmitArrays(arrs, 1)
}

// ObjectCacheStatsResult is the worker process's object-cache counters
// (object_cache_stats); the reference's ObjectCacheStatsResult.
type ObjectCacheStatsResult struct {
	ResidentBytes int64 `arrow:"resident_bytes" vgirpc:"resident_bytes"`
	Entries       int64 `arrow:"entries" vgirpc:"entries"`
	MaxBytes      int64 `arrow:"max_bytes" vgirpc:"max_bytes"`
	Hits          int64 `arrow:"hits" vgirpc:"hits"`
	Misses        int64 `arrow:"misses" vgirpc:"misses"`
	Evictions     int64 `arrow:"evictions" vgirpc:"evictions"`
	Fetches       int64 `arrow:"fetches" vgirpc:"fetches"`
}

// ArrowSchema makes ObjectCacheStatsResult a record result (nested IPC bytes
// on the wire), as the reference's dataclass is.
func (ObjectCacheStatsResult) ArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "resident_bytes", Type: arrow.PrimitiveTypes.Int64},
		{Name: "entries", Type: arrow.PrimitiveTypes.Int64},
		{Name: "max_bytes", Type: arrow.PrimitiveTypes.Int64},
		{Name: "hits", Type: arrow.PrimitiveTypes.Int64},
		{Name: "misses", Type: arrow.PrimitiveTypes.Int64},
		{Name: "evictions", Type: arrow.PrimitiveTypes.Int64},
		{Name: "fetches", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
}

type objectCacheStatsParams struct{}

func objectCacheStats(_ context.Context, _ *vgirpc.CallContext, _ objectCacheStatsParams) (ObjectCacheStatsResult, error) {
	stats := vgirpc.DefaultObjectCache().Stats()
	return ObjectCacheStatsResult{
		ResidentBytes: stats.ResidentBytes,
		Entries:       stats.Entries,
		MaxBytes:      stats.MaxBytes,
		Hits:          stats.Hits,
		Misses:        stats.Misses,
		Evictions:     stats.Evictions,
		Fetches:       stats.Fetches,
	}, nil
}

type objectCacheAgeParams struct {
	Seconds int64 `vgirpc:"seconds"`
}

// objectCacheAge ages every object-cache entry (the test hook for the
// one-day rule).
func objectCacheAge(_ context.Context, _ *vgirpc.CallContext, p objectCacheAgeParams) (int64, error) {
	return int64(vgirpc.DefaultObjectCache().AgeEntries(time.Duration(p.Seconds) * time.Second)), nil
}

type resolveArgumentRefParams struct {
	Ref    []byte `vgirpc:"ref"`
	Size   int64  `vgirpc:"size"`
	SHA256 string `vgirpc:"sha256"`
}

// resolveArgumentRef resolves an argument reference through the framework
// primitive and returns the bytes' SHA-256 hex.
func resolveArgumentRef(_ context.Context, ctx *vgirpc.CallContext, p resolveArgumentRefParams) (string, error) {
	data, err := ctx.ResolveArgumentRef(p.Ref, p.Size, p.SHA256)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// registerStateRefMethods registers the four protocol 3.4 methods.
func registerStateRefMethods(server *vgirpc.Server) {
	vgirpc.Exchange(server, "state_ref_exchange", stateRefOutputSchema, stateRefInputSchema, stateRefExchange)
	vgirpc.Unary(server, "object_cache_stats", objectCacheStats)
	vgirpc.Unary(server, "object_cache_age", objectCacheAge)
	vgirpc.Unary(server, "resolve_argument_ref", resolveArgumentRef)
}
