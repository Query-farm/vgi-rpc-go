// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"fmt"
	"reflect"
)

// MarshalRecord serializes a vgirpc-tagged Go struct (or a pointer to one) as
// a one-row Arrow IPC stream, by the same derivation the RPC layer uses.
func MarshalRecord(v any) ([]byte, error) {
	if v == nil {
		return nil, fmt.Errorf("vgirpc.MarshalRecord: nil value")
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr && rv.IsNil() {
		return nil, fmt.Errorf("vgirpc.MarshalRecord: nil %T", v)
	}
	return serializeVgirpcStruct(v)
}

// UnmarshalRecord decodes a one-row Arrow IPC stream into out, which must be a
// non-nil pointer to a vgirpc-tagged Go struct. The stream must hold exactly
// one batch (an end-of-stream marker is optional) whose schema equals the
// struct's derived schema; schema metadata is ignored.
func UnmarshalRecord(data []byte, out any) error {
	rv := reflect.ValueOf(out)
	if !rv.IsValid() || rv.Kind() != reflect.Ptr || rv.IsNil() || rv.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("vgirpc.UnmarshalRecord: out must be a non-nil pointer to a struct, got %T", out)
	}
	batch, err := readWrappedRequest(data, 0)
	if err != nil {
		return err
	}
	defer batch.Release()
	val, err := deserializeParams(batch, rv.Elem().Type())
	if err != nil {
		return err
	}
	rv.Elem().Set(val)
	return nil
}
