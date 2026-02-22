// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package benchmark

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// Parameter structs

type NoopParams struct{}

type AddParams struct {
	A float64 `vgirpc:"a"`
	B float64 `vgirpc:"b"`
}

type GreetParams struct {
	Name string `vgirpc:"name"`
}

type RoundtripTypesParams struct {
	Color   string           `vgirpc:"color,enum"`
	Mapping map[string]int64 `vgirpc:"mapping"`
	Tags    []int64          `vgirpc:"tags"`
}

type GenerateParams struct {
	Count int64 `vgirpc:"count"`
}

type TransformParams struct {
	Factor float64 `vgirpc:"factor"`
}

// Stream schemas

var generateOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "i", Type: arrow.PrimitiveTypes.Int64},
	{Name: "value", Type: arrow.PrimitiveTypes.Int64},
}, nil)

var transformSchema = arrow.NewSchema([]arrow.Field{
	{Name: "value", Type: arrow.PrimitiveTypes.Float64},
}, nil)

// RegisterMethods registers the benchmark fixture methods on the server.
func RegisterMethods(server *vgirpc.Server) {
	vgirpc.RegisterStateType(&GenerateState{})
	vgirpc.RegisterStateType(&TransformState{})

	vgirpc.UnaryVoid(server, "noop", noop)
	vgirpc.Unary(server, "add", add)
	vgirpc.Unary(server, "greet", greet)
	vgirpc.Unary(server, "roundtrip_types", roundtripTypes)
	vgirpc.Producer(server, "generate", generateOutputSchema, generate)
	vgirpc.Exchange(server, "transform", transformSchema, transformSchema, transform)
}

// Handler implementations

func noop(_ context.Context, _ *vgirpc.CallContext, _ NoopParams) error {
	return nil
}

func add(_ context.Context, _ *vgirpc.CallContext, p AddParams) (float64, error) {
	return p.A + p.B, nil
}

func greet(_ context.Context, _ *vgirpc.CallContext, p GreetParams) (string, error) {
	return "Hello, " + p.Name + "!", nil
}

func roundtripTypes(_ context.Context, _ *vgirpc.CallContext, p RoundtripTypesParams) (string, error) {
	// Format matching Python: f"{color.name}:{isinstance(color, Color)}:{dict(sorted(mapping.items()))}:{sorted(tags)}"
	// Color arrives as raw enum string (e.g. "GREEN"), isinstance is always true
	keys := make([]string, 0, len(p.Mapping))
	for k := range p.Mapping {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var mappingParts []string
	for _, k := range keys {
		mappingParts = append(mappingParts, fmt.Sprintf("'%s': %d", k, p.Mapping[k]))
	}
	mappingStr := "{" + strings.Join(mappingParts, ", ") + "}"

	sortedTags := make([]int64, len(p.Tags))
	copy(sortedTags, p.Tags)
	sort.Slice(sortedTags, func(i, j int) bool { return sortedTags[i] < sortedTags[j] })

	var tagParts []string
	for _, t := range sortedTags {
		tagParts = append(tagParts, fmt.Sprintf("%d", t))
	}
	tagsStr := "[" + strings.Join(tagParts, ", ") + "]"

	return fmt.Sprintf("%s:true:%s:%s", p.Color, mappingStr, tagsStr), nil
}

func generate(_ context.Context, _ *vgirpc.CallContext, p GenerateParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: generateOutputSchema,
		State:        &GenerateState{Count: int(p.Count)},
	}, nil
}

func transform(_ context.Context, _ *vgirpc.CallContext, p TransformParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: transformSchema,
		InputSchema:  transformSchema,
		State:        &TransformState{Factor: p.Factor},
	}, nil
}
