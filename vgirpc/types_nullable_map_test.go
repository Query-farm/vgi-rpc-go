// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"reflect"
	"testing"
)

func TestNullableMapsPreserveAbsentEmptyAndPopulated(t *testing.T) {
	type payload struct {
		Tags   *map[string]string           `vgirpc:"tags"`
		Nested map[string]*map[string]int64 `vgirpc:"nested"`
	}
	empty := map[string]string{}
	populated := map[string]string{"a.b": "value"}
	nested := map[string]int64{"n": 42}
	for _, test := range []struct {
		name string
		tags *map[string]string
	}{
		{"absent", nil}, {"empty", &empty}, {"populated", &populated},
	} {
		t.Run(test.name, func(t *testing.T) {
			want := payload{Tags: test.tags, Nested: map[string]*map[string]int64{"present": &nested, "absent": nil}}
			got := roundTrip(t, want).Interface().(payload)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %#v, want %#v", got, want)
			}
		})
	}
}
