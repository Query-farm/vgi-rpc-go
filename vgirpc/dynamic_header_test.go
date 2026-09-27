// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
package vgirpc

import (
	"context"
	"github.com/apache/arrow-go/v18/arrow"
	"testing"
)

// Dynamic output schemas do not imply that a stream has an application header.
// Reflection drives client parsing and the canonical protocol hash.
func TestDynamicHeaderDeclarationMatchesSchema(t *testing.T) {
	header := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
	for _, schema := range []*arrow.Schema{nil, header} {
		s := NewServer()
		handler := func(context.Context, *CallContext, struct{}) (*StreamResult, error) { return nil, nil }
		DynamicProducerWithHeader(s, "producer", schema, handler)
		DynamicExchangeWithHeader(s, "exchange", schema, handler)
		DynamicStreamWithHeader(s, "dynamic", schema, handler)
		for _, name := range []string{"producer", "exchange", "dynamic"} {
			got := s.methods[name]
			if got.HasHeader != (schema != nil) {
				t.Fatalf("%s header declaration=%v schema=%v", name, got.HasHeader, schema)
			}
			if got.HeaderSchema != schema {
				t.Fatalf("%s lost declared header schema", name)
			}
		}
		for _, method := range hashMethodsOf(s.methods) {
			if method.HasHeader != (schema != nil) {
				t.Fatalf("%s hash input has incorrect header flag", method.Name)
			}
		}
	}
}
