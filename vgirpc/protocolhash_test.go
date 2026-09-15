package vgirpc

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// The cross-port contract. These digests are produced by the Python reference
// (vgi-rpc-python, vgi_rpc/rpc/_protocol_hash.py); a mismatch here means this
// port and that one would disagree about whether they speak the same protocol.
//
// A failure is a JSON diff, not a guess: CanonicalDescription returns the exact
// preimage, so print it and compare against the reference's
// tests/golden/protocol_hash_vector.json.
func TestProtocolHashMatchesPythonReference(t *testing.T) {
	methods := []HashMethod{
		{
			Name:       "echo",
			MethodType: "unary",
			HasReturn:  true,
			ParamsSchema: arrow.NewSchema([]arrow.Field{
				{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false},
			}, nil),
			ResultSchema: arrow.NewSchema([]arrow.Field{
				{Name: "result", Type: arrow.BinaryTypes.String, Nullable: false},
			}, nil),
		},
	}
	got, err := ComputeProtocolHash("demo.Hash.v1", methods)
	if err != nil {
		t.Fatal(err)
	}
	const want = "e337ddbbd5758bbc042ce85df28786dfca482b655adfcad28c9cb11f7b9c8f45"
	if got != want {
		preimage, _ := CanonicalDescription("demo.Hash.v1", methods)
		t.Errorf("hash = %s, want %s\npreimage: %s", got, want, preimage)
	}
}

func TestCanonicalPreimageShape(t *testing.T) {
	methods := []HashMethod{{
		Name:         "fire",
		MethodType:   "unary",
		HasReturn:    false,
		ParamsSchema: arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.String, Nullable: false}}, nil),
	}}
	got, err := CanonicalDescription("demo.Void.v1", methods)
	if err != nil {
		t.Fatal(err)
	}
	// A method returning nothing must omit "result" entirely: absent and empty
	// are different, and must not hash alike.
	want := `{"methods":[{"has_header":false,"has_return":false,"is_exchange":false,"name":"fire","params":[{"name":"v","nullable":false,"type":"utf8"}],"type":"unary"}],"protocol":"demo.Void.v1"}`
	if string(got) != want {
		t.Errorf("preimage =\n  %s\nwant\n  %s", got, want)
	}
}

func TestMethodsAreSorted(t *testing.T) {
	// A port iterating a hash map must still produce this order.
	mk := func(name string) HashMethod {
		return HashMethod{Name: name, MethodType: "unary", ParamsSchema: arrow.NewSchema(nil, nil)}
	}
	a, err := ComputeProtocolHash("p", []HashMethod{mk("a"), mk("b")})
	if err != nil {
		t.Fatal(err)
	}
	b, err := ComputeProtocolHash("p", []HashMethod{mk("b"), mk("a")})
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Error("declaration order must not affect the hash")
	}
}
