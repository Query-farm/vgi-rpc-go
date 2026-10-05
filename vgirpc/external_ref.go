// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ---------------------------------------------------------------------------
// Pre-published references
// ---------------------------------------------------------------------------

// ExternalRef is a reference to an already-published unary result.
//
// A unary method answers with one by calling
// [CallContext.RespondWithExternalRef] instead of returning its value. The
// server then writes the ExternalLocation pointer batch for the ref directly:
// no result serialization, compression, or upload happens during the call,
// and the ref is used whether or not the server has external storage
// configured and regardless of the externalization threshold. Clients resolve
// it like any other pointer, so they need no change.
//
// Build one with [PublishExternal] (or [NewExternalRef] for an object
// published out of band). The object at the URL must be an Arrow IPC stream
// (optionally zstd Content-Encoding) whose schema is the method's result
// schema and which holds exactly one 1-row data batch.
//
// The caller owns caching the ref and the object's lifecycle: a long-lived
// ref must not point at an object under the short-TTL lifecycle rule used for
// per-call uploads, and a pre-signed URL expires -- re-sign or rebuild the ref
// before then. Only return a ref to callers who are all entitled to the same
// content.
//
// The zero value is not a valid ref; [CallContext.RespondWithExternalRef]
// refuses it.
type ExternalRef struct {
	url    string
	sha256 string
}

// NewExternalRef validates and returns a ref for url.
//
// sha256Hex is the lowercase hex SHA-256 of the raw (pre-compression) IPC
// stream bytes, sent as vgi_rpc.location.sha256. Pass "" to omit the key, so
// clients skip the content check -- use that for an object rewritten in place
// or one too large to hash. An empty url, or a digest that is not 64
// lowercase hex characters, is an error.
func NewExternalRef(url, sha256Hex string) (ExternalRef, error) {
	ref := ExternalRef{url: url, sha256: sha256Hex}
	if err := ref.validate(); err != nil {
		return ExternalRef{}, err
	}
	return ref, nil
}

// URL returns where the published IPC stream lives.
func (r ExternalRef) URL() string { return r.url }

// SHA256 returns the ref's lowercase hex digest, or "" when it carries none.
func (r ExternalRef) SHA256() string { return r.sha256 }

// PointerBatch builds the zero-row pointer batch announcing this ref against
// the method's result schema, as [MakeExternalLocationBatch] does. The caller
// owns (and must release) the returned batch.
func (r ExternalRef) PointerBatch(schema *arrow.Schema) (arrow.RecordBatch, arrow.Metadata) {
	return MakeExternalLocationBatch(schema, r.url, r.sha256)
}

func (r ExternalRef) validate() error {
	if r.url == "" {
		return errors.New("ExternalRef url must be non-empty")
	}
	if r.sha256 != "" && !isLowerHexSHA256(r.sha256) {
		return errors.New("ExternalRef sha256 must be 64 lowercase hex characters (or empty)")
	}
	return nil
}

func isLowerHexSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// PublishExternal publishes a unary result batch once and returns a reusable
// reference.
//
// It serializes batch exactly as the per-call externalizer does (an IPC
// stream of the schema plus this one batch), hashes the raw bytes, compresses
// when compression is non-nil (pass the server's
// [ExternalLocationConfig].Compression to match it), and calls storage.Upload
// once. Cache the returned ref and answer later calls with
// [CallContext.RespondWithExternalRef]; the server writes the pointer
// directly.
//
// batch must hold exactly one row and use the method's result schema (a
// single "result" column); [PublishExternalResult] builds it from a Go value.
// With includeSHA256 false the ref carries no digest, so clients skip the
// content check.
func PublishExternal(
	batch arrow.RecordBatch,
	storage ExternalStorage,
	compression *Compression,
	includeSHA256 bool,
) (ExternalRef, error) {
	if storage == nil {
		return ExternalRef{}, errors.New("PublishExternal requires a storage backend")
	}
	if batch.NumRows() != 1 {
		return ExternalRef{}, fmt.Errorf("PublishExternal expects a 1-row result batch, got %d rows", batch.NumRows())
	}
	ipcData, err := serializeBatchAsIPC(batch, nil)
	if err != nil {
		return ExternalRef{}, fmt.Errorf("serializing batch for external storage: %w", err)
	}
	// No call is in flight, so nothing is charged to an access record.
	locationURL, sha256Hex, err := uploadIPCBytes(context.Background(), ipcData, batch.Schema(), storage, compression)
	if err != nil {
		return ExternalRef{}, err
	}
	if !includeSHA256 {
		sha256Hex = ""
	}
	return NewExternalRef(locationURL, sha256Hex)
}

// PublishExternalResult is [PublishExternal] for a Go result value: it builds
// the 1-row result batch the way the dispatcher would for a method registered
// with [Unary] returning R, then publishes it.
//
//	ref, err := vgirpc.PublishExternalResult(catalogJSON, storage, cfg.Compression, true)
func PublishExternalResult[R any](
	value R,
	storage ExternalStorage,
	compression *Compression,
	includeSHA256 bool,
) (ExternalRef, error) {
	var zero R
	schema, err := resultSchema(reflect.TypeOf(zero))
	if err != nil {
		return ExternalRef{}, fmt.Errorf("deriving result schema for %T: %w", zero, err)
	}
	if schema.NumFields() == 0 {
		return ExternalRef{}, errors.New("PublishExternalResult needs a result type with a column")
	}
	batch, err := serializeResult(schema, value)
	if err != nil {
		return ExternalRef{}, fmt.Errorf("result serialization: %w", err)
	}
	defer batch.Release()
	return PublishExternal(batch, storage, compression, includeSHA256)
}

// RespondWithExternalRef answers the current unary call with a pre-published
// ref instead of the handler's return value.
//
// The handler then returns its zero value (and a nil error); the dispatcher
// ignores that value and writes the ref's pointer batch:
//
//	func catalog(_ context.Context, call *vgirpc.CallContext, _ catalogParams) (string, error) {
//		return "", call.RespondWithExternalRef(cachedRef)
//	}
//
// The pointer is written on every transport and regardless of external
// storage configuration or threshold -- a ref is never inlined or sent through
// shared memory -- and it is not counted toward the externalized-response cap,
// since nothing is uploaded during the call. A non-nil error returned by the
// handler still wins over the ref.
//
// Only unary methods that return a value can answer with a ref: on a void
// method or a stream it returns an error and records nothing, as it does for
// an invalid ref.
func (ctx *CallContext) RespondWithExternalRef(ref ExternalRef) error {
	if !ctx.externalRefAllowed {
		return &RpcError{
			Type:    "RuntimeError",
			Message: "RespondWithExternalRef is only supported by unary methods that return a value",
		}
	}
	if err := ref.validate(); err != nil {
		return &RpcError{Type: "ValueError", Message: err.Error()}
	}
	ctx.externalRef = &ref
	return nil
}

// externalRefResultBatch is the result batch a unary dispatcher writes for a
// handler that answered with a ref: the zero-row pointer batch with its
// location metadata attached, ready for [WriteUnaryResponse]. Nothing is
// built, validated, serialized or uploaded. The caller releases it.
func externalRefResultBatch(schema *arrow.Schema, ref *ExternalRef) arrow.RecordBatch {
	pointer, meta := ref.PointerBatch(schema)
	defer pointer.Release()
	return array.NewRecordBatchWithMetadata(pointer.Schema(), pointer.Columns(), pointer.NumRows(), meta)
}
