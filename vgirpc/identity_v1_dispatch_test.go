// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

// vgi_rpc.Identity.v1, driven end to end over a raw transport.
//
// Every other identity test in this port constructs an *IdentityImpl directly
// and hands it a *CallContext it built itself. That is exactly the shape that
// hid a framework bug in the reference implementation for the whole life of its
// identity protocol: CallContext injection there resolved "does this method
// want a ctx?" against the PRIMARY binding's method set rather than against the
// binding that owns the dispatched method, and identity is a *secondary*
// protocol whose methods both take one. Every call over every transport failed
// with a missing-argument error before any guard ran, and all sixty-one of the
// reference's identity tests stayed green because not one of them called the
// protocol.
//
// Go cannot reproduce that bug: Unary registers a handler whose signature
// statically includes *CallContext, dispatch reads the handler off
// binding.Methods[method] -- the resolved binding, never the server's primary
// -- and passes the context unconditionally. There is no per-server map of
// ctx-taking method names to get keyed wrong. But "cannot" is a claim about
// code nobody has run, so this file runs it.
//
// This is port-local rather than upstream by the fixture contract's own
// design: the shared group is HTTP-only, because Identity's guards all read an
// authenticated caller and HTTP is the transport that carries one (§2), and §6
// lists raw-transport dispatch among the properties it deliberately does not
// assert -- "Do cover it port-locally". So this is not a Go-local duplicate of
// a conformance case; it is the half the conformance suite structurally cannot
// reach.
//
// The three cases are the ones IDENTITY_CONFORMANCE_FIXTURE.md §7 prescribes:
// an allowlisted caller resolves, a non-allowlisted caller on the same
// transport is refused, and an anonymous raw transport cannot mint. The middle
// one is not decoration -- without it, a dispatch path that supplied an EMPTY
// context (which refuses everything) would be indistinguishable from a working
// allowlist.

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// rawIdentityServer hosts an application protocol as the primary, then
// reflection, then identity -- so identity is a secondary binding reached by
// its routing key, which is the arrangement §7 is about.
func rawIdentityServer(t *testing.T) *Server {
	t.Helper()
	return idServer(t, mustIdentity(t, IdentityConfig{
		ResolveToken:         idResolver,
		MintGrant:            idMinter,
		IntrospectPrincipals: []string{"proxy"},
	}))
}

// rawCall dispatches one unary call through serveOne with auth installed as a
// connection identity, the way a stateful raw transport (unix, TCP) does.
func rawCall(t *testing.T, s *Server, auth *AuthContext, method string, params any) []byte {
	t.Helper()
	paramBytes, err := serializeVgirpcStruct(params)
	if err != nil {
		t.Fatalf("serialize %s params: %v", method, err)
	}
	batch := readOneBatch(t, paramBytes)
	defer batch.Release()

	var request bytes.Buffer
	if err := WriteRequest(&request, method, batch, IdentityProtocolName, ""); err != nil {
		t.Fatalf("write %s request: %v", method, err)
	}

	ctx := context.Background()
	if auth != nil {
		// The same call a transport adapter makes once per connection. A nil
		// auth leaves the context bare, which is the anonymous raw transport.
		ctx, err = WithConnectionIdentity(ctx, auth, EmptyPeerEvidence())
		if err != nil {
			t.Fatalf("WithConnectionIdentity: %v", err)
		}
	}
	var response bytes.Buffer
	if err := s.serveOne(ctx, bytes.NewReader(request.Bytes()), &response, &shmConnState{}); err != nil {
		t.Fatalf("serveOne(%s): %v", method, err)
	}
	return response.Bytes()
}

// rawRefusal reads the EXCEPTION batch a refused call answered with, returning
// the wire-visible triple a client classifies on.
func rawRefusal(t *testing.T, body []byte) (kind, errType, message string) {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("open response: %v", err)
	}
	defer r.Release()
	for r.Next() {
		rb, ok := r.RecordBatch().(arrow.RecordBatchWithMetadata)
		if !ok {
			continue
		}
		md := rb.Metadata()
		if level, _ := md.GetValue(MetaLogLevel); level != string(LogException) {
			continue
		}
		kind, _ = md.GetValue(MetaErrorKind)
		extra, _ := md.GetValue(MetaLogExtra)
		var decoded errorExtra
		if err := json.Unmarshal([]byte(extra), &decoded); err != nil {
			t.Fatalf("decode error metadata: %v", err)
		}
		return kind, decoded.ExceptionType, decoded.ExceptionMessage
	}
	if err := r.Err(); err != nil {
		t.Fatalf("read response: %v", err)
	}
	t.Fatalf("response carried no EXCEPTION batch; a guard that did not fire "+
		"answers with a result, which is what this is distinguishing: %q", body)
	return "", "", ""
}

// An allowlisted caller's credential resolves, over a transport that carries
// its identity on the connection rather than in a request header.
//
// This is the case the reference could not pass: the handler never received a
// context, so it failed on a missing argument before CheckIntrospector ran.
func TestRawTransportIntrospectionResolvesForAnAllowlistedCaller(t *testing.T) {
	body := rawCall(t, rawIdentityServer(t), idAuth("proxy", true, noAuthTime),
		"introspect_token", introspectTokenParams{Token: "good"})

	_, resultBytes, ok := ReadUnaryResult(body)
	if !ok {
		kind, errType, message := rawRefusal(t, body)
		t.Fatalf("an allowlisted caller was refused over a raw transport: "+
			"error_kind=%q type=%q message=%q. A missing or empty CallContext "+
			"refuses every caller, which is what §7 of the fixture contract is about.",
			kind, errType, message)
	}
	batch := readOneBatch(t, resultBytes)
	defer batch.Release()
	decoded, err := deserializeParams(batch, reflect.TypeOf(TokenIdentity{}))
	if err != nil {
		t.Fatalf("decode TokenIdentity: %v", err)
	}
	got := decoded.Interface().(TokenIdentity)
	if got.Principal != "bob" || got.TokenName != "ci-key" || got.TTLSeconds != DefaultTokenTTLSeconds {
		t.Errorf("resolved %+v, want principal bob / ci-key / %d", got, DefaultTokenTTLSeconds)
	}
}

// The same transport, a caller who is not on the allowlist. Refused.
//
// The positive case alone cannot tell a working allowlist from a dispatch path
// that hands every handler the same context: this is what makes the pair a
// statement about the caller's identity reaching the guard, rather than about
// the guard running.
func TestRawTransportIntrospectionRefusesACallerOffTheAllowlist(t *testing.T) {
	body := rawCall(t, rawIdentityServer(t), idAuth("mallory", true, noAuthTime),
		"introspect_token", introspectTokenParams{Token: "good"})
	kind, errType, _ := rawRefusal(t, body)
	if kind != "introspection_refused" {
		t.Errorf("error_kind = %q, want introspection_refused -- a caller holding "+
			"*a* credential is not an introspector", kind)
	}
	if errType != "IntrospectionRefusedError" {
		t.Errorf("exception_type = %q, want IntrospectionRefusedError", errType)
	}
}

// A raw transport carrying no authenticated principal fails closed, for free.
//
// Subprocess, unix and TCP have no authenticated caller at all, so the
// freshness guard -- which needs an auth_time claim -- refuses them without
// anything transport-specific having to be written.
func TestRawTransportCannotMint(t *testing.T) {
	s := rawIdentityServer(t)
	for _, tc := range []struct {
		name string
		auth *AuthContext
	}{
		{"no connection identity at all", nil},
		{"a connection identity that authenticated nobody", Anonymous()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := rawCall(t, s, tc.auth, "issue_grant",
				issueGrantParams{Purpose: "conformance", Scopes: []string{"read"}, TTLSeconds: 60})
			kind, errType, _ := rawRefusal(t, body)
			if kind != "stale_auth" {
				t.Errorf("error_kind = %q, want stale_auth", kind)
			}
			if errType != "StaleAuthError" {
				t.Errorf("exception_type = %q, want StaleAuthError", errType)
			}
		})
	}
}

// And the complement: a recently authenticated caller on the same raw
// transport does mint, so the refusals above are about the caller rather than
// about issue_grant being unreachable there.
//
// Without this the three cases above all pass against a build where no
// secondary-protocol call reaches its handler at all -- which is precisely the
// bug §7 names.
func TestRawTransportMintsForAFreshlyAuthenticatedCaller(t *testing.T) {
	auth := idAuth("alice", true, float64(time.Now().Unix()-60))
	body := rawCall(t, rawIdentityServer(t), auth, "issue_grant",
		issueGrantParams{Purpose: "conformance", Scopes: []string{"read"}, TTLSeconds: 60})

	_, resultBytes, ok := ReadUnaryResult(body)
	if !ok {
		kind, errType, message := rawRefusal(t, body)
		t.Fatalf("a freshly authenticated caller could not mint over a raw transport: "+
			"error_kind=%q type=%q message=%q", kind, errType, message)
	}
	batch := readOneBatch(t, resultBytes)
	defer batch.Release()
	decoded, err := deserializeParams(batch, reflect.TypeOf(IssuedGrant{}))
	if err != nil {
		t.Fatalf("decode IssuedGrant: %v", err)
	}
	if got := decoded.Interface().(IssuedGrant); got.Token != "grant-for-alice" {
		t.Errorf("minted %+v, want a grant for the CALLER (grant-for-alice)", got)
	}
}
