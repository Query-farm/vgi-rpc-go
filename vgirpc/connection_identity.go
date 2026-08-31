// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"fmt"
)

type connectionIdentityContextKey struct{}

type connectionIdentity struct {
	auth     *AuthContext
	evidence *PeerEvidenceSet
}

// WithConnectionIdentity attaches one adapter-resolved identity snapshot to a
// raw, stateful VGI connection. Pass the returned context to ServeWithContext
// (or a custom transport's equivalent serve loop). Every unary call, stream
// factory, stream turn, and cancellation hook on that connection sees the same
// Auth and PeerEvidence values.
//
// The identity is off-wire: a VGI client cannot populate or replace it.
func WithConnectionIdentity(ctx context.Context, auth *AuthContext, evidence *PeerEvidenceSet) (context.Context, error) {
	if ctx == nil {
		return nil, fmt.Errorf("vgirpc: connection identity requires a context")
	}
	if auth == nil {
		auth = Anonymous()
	}
	if evidence == nil {
		evidence = EmptyPeerEvidence()
	}
	claims := make(map[string]any, len(auth.Claims))
	for key, value := range auth.Claims {
		claims[key] = value
	}
	authSnapshot := &AuthContext{
		Domain: auth.Domain, Authenticated: auth.Authenticated,
		Principal: auth.Principal, Claims: claims,
	}
	return context.WithValue(ctx, connectionIdentityContextKey{}, &connectionIdentity{
		auth: authSnapshot, evidence: evidence,
	}), nil
}

func identityFromConnectionContext(ctx context.Context) (*AuthContext, *PeerEvidenceSet) {
	if ctx != nil {
		if identity, ok := ctx.Value(connectionIdentityContextKey{}).(*connectionIdentity); ok && identity != nil {
			return identity.auth, identity.evidence
		}
	}
	return Anonymous(), EmptyPeerEvidence()
}
