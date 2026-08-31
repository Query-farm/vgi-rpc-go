// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestRawConnectionIdentityReachesUnaryCallContext(t *testing.T) {
	server := NewServer()
	server.SetProtocolVersion("2.0.0")
	var observedAuth *AuthContext
	var observedEvidence *PeerEvidenceSet
	UnaryVoid(server, "observe", func(_ context.Context, call *CallContext, _ struct{}) error {
		observedAuth = call.Auth
		observedEvidence = call.PeerEvidence
		return nil
	})
	if err := server.notifyTransport(TransportKindTcp, nil); err != nil {
		t.Fatal(err)
	}

	peer, err := NewPeerIdentity(PeerIdentityOptions{
		Provider: "iroh", EvidenceSource: "endpoint",
		Assurance: IdentityAssuranceCryptographicPeer, Issuer: "iroh:test",
		Transport: "iroh", SubjectKind: PeerSubjectEndpoint, SubjectKey: "node-7",
		SubjectStability: SubjectStabilityStable, SubjectVerified: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := NewAvailablePeerIdentityResult("iroh", peer)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := NewPeerEvidenceSet(result)
	if err != nil {
		t.Fatal(err)
	}
	auth := &AuthContext{Domain: "iroh", Authenticated: true, Principal: "node-7", Claims: map[string]any{"role": "worker"}}
	ctx, err := WithConnectionIdentity(context.Background(), auth, evidence)
	if err != nil {
		t.Fatal(err)
	}
	// Caller-owned top-level claims are snapshotted before dispatch.
	auth.Claims["role"] = "changed"

	schema := arrow.NewSchema(nil, nil)
	batch := array.NewRecordBatch(schema, nil, 0)
	defer batch.Release()
	var request bytes.Buffer
	if err := WriteRequest(&request, "observe", batch, "2.0.0"); err != nil {
		t.Fatal(err)
	}
	if err := server.serveOne(ctx, &request, io.Discard, &shmConnState{}); err != nil {
		t.Fatal(err)
	}
	if observedAuth == nil || !observedAuth.Authenticated || observedAuth.Domain != "iroh" || observedAuth.Principal != "node-7" {
		t.Fatalf("unexpected auth snapshot: %#v", observedAuth)
	}
	if got := observedAuth.Claims["role"]; got != "worker" {
		t.Fatalf("claim snapshot = %v, want worker", got)
	}
	if observedEvidence != evidence || len(observedEvidence.ForProvider("iroh")) != 1 {
		t.Fatal("peer evidence did not reach raw unary CallContext")
	}
}

func TestWithConnectionIdentityDefaultsNilValues(t *testing.T) {
	ctx, err := WithConnectionIdentity(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	auth, evidence := identityFromConnectionContext(ctx)
	if auth.Authenticated || len(evidence.Identities()) != 0 {
		t.Fatalf("unexpected defaults: auth=%#v evidence=%#v", auth, evidence)
	}
	var nilContext context.Context
	if _, err := WithConnectionIdentity(nilContext, nil, nil); err == nil {
		t.Fatal("nil context accepted")
	}
}
