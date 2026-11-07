package vgirpc

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestNewTestOutputCollector(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	out := NewTestOutputCollector(schema)
	if err := out.Validate(); err == nil {
		t.Fatal("an unfinished turn with no data must fail Validate")
	}
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.AppendValues([]int64{1, 2, 3}, nil)
	col := b.NewArray()
	b.Release()
	out.ClientLog(LogInfo, "hello")
	if err := out.EmitArrays([]arrow.Array{col}, 3); err != nil {
		t.Fatal(err)
	}
	if got := out.Batches(); len(got) != 1 || got[0].NumRows() != 3 {
		t.Fatalf("Batches = %v", got)
	}
	if logs := out.Logs(); len(logs) != 1 || logs[0].Message != "hello" || logs[0].Level != LogInfo {
		t.Fatalf("Logs = %+v", logs)
	}
	if err := out.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := out.EmitArrays([]arrow.Array{col}, 3); err == nil {
		t.Fatal("second data batch accepted")
	}
	out.Reset()
	if len(out.Batches()) != 0 || out.Validate() == nil {
		t.Fatal("Reset must clear the turn")
	}
	if err := out.Finish(); err != nil || !out.Finished() || out.Validate() != nil {
		t.Fatalf("Finish: %v", err)
	}
}
