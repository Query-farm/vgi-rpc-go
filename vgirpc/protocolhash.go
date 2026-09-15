// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgirpc

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
)

// The protocol hash: a fingerprint of a protocol's wire surface.
//
// A client and a worker agree on a protocol or they do not, and the hash is how
// either side says which one it has without shipping the whole description. For
// that to be worth anything the same protocol must hash the same in every port,
// which the previous definition could not promise: it hashed serialized Arrow
// IPC bytes, and each language's Arrow implementation may legitimately emit
// different bytes for the same logical schema. The docs said so, which made the
// field advisory -- comparable only against itself.
//
// So the preimage is canonical JSON of what Arrow *decodes to*:
//
//	sha256("vgi_rpc.protocol_hash.v1|" + canonicalJSON(description))
//
// Profile: RFC 8785 (JCS), chosen for its published test vectors. The structure
// is deliberately restricted to objects, arrays, strings and booleans; every
// number is folded into a type token ("decimal128(38,9)"), so JCS's hardest rule
// -- number canonicalisation, and the likeliest place for six ports to diverge
// -- never applies. Keep it that way.
//
// Not in the preimage: server identity, docstrings, parameter defaults,
// language-specific type names, and the framework's own request/describe
// versions. Those vary across processes, builds and ports without changing what
// is on the wire. The "v1" domain tag is the only version the hash carries, and
// it moves only when the hash *definition* moves.

// hashDomain separates this digest from any other use of SHA-256. It moves only
// when the hash definition moves, never when a protocol changes -- that is what
// the hash itself is for.
const hashDomain = "vgi_rpc.protocol_hash.v1|"

// hashMethodEntry is one method as it appears in the preimage.
//
// Field order in this struct is irrelevant: canonical JSON sorts object keys,
// which is exactly why the preimage is JSON rather than a hand-rolled framing.
type hashMethodEntry struct {
	Name       string       `json:"name"`
	Type       string       `json:"type"`
	HasReturn  bool         `json:"has_return"`
	HasHeader  bool         `json:"has_header"`
	IsExchange bool         `json:"is_exchange"`
	Params     []FieldToken `json:"params"`
	// Absent and empty are different: a method returning nothing is not a
	// method returning an empty struct, and they must not hash alike. omitempty
	// on a nil slice is what expresses that.
	Result []FieldToken `json:"result,omitempty"`
	Header []FieldToken `json:"header,omitempty"`
}

// hashDescription is the whole preimage.
type hashDescription struct {
	Protocol string            `json:"protocol"`
	Methods  []hashMethodEntry `json:"methods"`
}

// HashMethod is the per-method input to ComputeProtocolHash.
//
// Deliberately takes decoded schemas rather than serialized IPC: the hash is
// over structure, and accepting bytes would invite a caller to pass whatever its
// encoder produced.
type HashMethod struct {
	Name         string
	MethodType   string // "unary" or "stream"
	HasReturn    bool
	HasHeader    bool
	IsExchange   bool
	ParamsSchema *arrow.Schema
	ResultSchema *arrow.Schema // nil when HasReturn is false
	HeaderSchema *arrow.Schema // nil when HasHeader is false
}

// ComputeProtocolHash returns the SHA-256 hex digest of a protocol's canonical
// description.
//
// Identical in every port for the same protocol -- which is a property
// conformance can assert, and could not before.
func ComputeProtocolHash(protocolName string, methods []HashMethod) (string, error) {
	// Sorted so two ports iterating differently-ordered maps still agree.
	sorted := make([]HashMethod, len(methods))
	copy(sorted, methods)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	entries := make([]hashMethodEntry, 0, len(sorted))
	for _, m := range sorted {
		params, err := SchemaTokens(m.ParamsSchema)
		if err != nil {
			return "", err
		}
		entry := hashMethodEntry{
			Name:       m.Name,
			Type:       m.MethodType,
			HasReturn:  m.HasReturn,
			HasHeader:  m.HasHeader,
			IsExchange: m.IsExchange,
			Params:     params,
		}
		if m.HasReturn && m.ResultSchema != nil {
			if entry.Result, err = SchemaTokens(m.ResultSchema); err != nil {
				return "", err
			}
		}
		if m.HasHeader && m.HeaderSchema != nil {
			if entry.Header, err = SchemaTokens(m.HeaderSchema); err != nil {
				return "", err
			}
		}
		entries = append(entries, entry)
	}

	// encoding/json sorts struct fields by declaration, not by name, so the
	// description is marshalled through a generic form that sorts keys.
	preimage, err := canonicalJSON(hashDescription{Protocol: protocolName, Methods: entries})
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(hashDomain))
	h.Write(preimage)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// CanonicalDescription returns the exact preimage bytes ComputeProtocolHash
// digests.
//
// Exposed because a hash mismatch between ports is otherwise one bit of
// information. With the preimage in hand a failing port diffs two JSON
// documents and sees which method, field or type token it spells differently.
func CanonicalDescription(protocolName string, methods []HashMethod) ([]byte, error) {
	sorted := make([]HashMethod, len(methods))
	copy(sorted, methods)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	entries := make([]hashMethodEntry, 0, len(sorted))
	for _, m := range sorted {
		params, err := SchemaTokens(m.ParamsSchema)
		if err != nil {
			return nil, err
		}
		entry := hashMethodEntry{
			Name:       m.Name,
			Type:       m.MethodType,
			HasReturn:  m.HasReturn,
			HasHeader:  m.HasHeader,
			IsExchange: m.IsExchange,
			Params:     params,
		}
		if m.HasReturn && m.ResultSchema != nil {
			if entry.Result, err = SchemaTokens(m.ResultSchema); err != nil {
				return nil, err
			}
		}
		if m.HasHeader && m.HeaderSchema != nil {
			if entry.Header, err = SchemaTokens(m.HeaderSchema); err != nil {
				return nil, err
			}
		}
		entries = append(entries, entry)
	}
	return canonicalJSON(hashDescription{Protocol: protocolName, Methods: entries})
}

// canonicalJSON serializes v as RFC 8785 canonical JSON.
//
// Go's encoding/json already emits object keys in sorted order for map[string]T
// and escapes control characters with the short forms JCS requires. It does
// *not* sort struct fields, and it HTML-escapes < > & by default, so the value
// is round-tripped through a generic form and encoded with escaping disabled.
func canonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var generic any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&generic); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(generic); err != nil {
		return nil, err
	}
	// Encode appends a newline, which is not part of the preimage.
	out := buf.Bytes()
	if n := len(out); n > 0 && out[n-1] == '\n' {
		out = out[:n-1]
	}
	return out, nil
}
