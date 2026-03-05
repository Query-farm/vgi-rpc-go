// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package jwtauth provides a JWT-based [vgirpc.AuthenticateFunc] backed by
// JWKS key discovery and validation. It is a separate module to avoid pulling
// JWT dependencies into the core vgirpc package.
package jwtauth

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/golang-jwt/jwt/v5"
)

// JWTAuthConfig configures JWT-based authentication.
type JWTAuthConfig struct {
	// Issuer is the expected "iss" claim. Required.
	Issuer string
	// Audience is the expected "aud" claim. Required.
	Audience string
	// JWKSURI is the JWKS endpoint URL. Required.
	JWKSURI string
	// PrincipalClaim is the JWT claim used as the principal identity.
	// Defaults to "sub" if empty.
	PrincipalClaim string
	// Domain is the authentication domain name. Defaults to "jwt" if empty.
	Domain string
}

// NewAuthenticateFunc creates a [vgirpc.AuthenticateFunc] that validates JWT
// Bearer tokens using JWKS. It returns the auth callback, a cleanup function
// that should be called when the server shuts down (to stop background JWKS
// refresh), and any initialization error.
func NewAuthenticateFunc(cfg JWTAuthConfig) (vgirpc.AuthenticateFunc, func(), error) {
	if cfg.Issuer == "" {
		return nil, nil, fmt.Errorf("jwtauth: issuer is required")
	}
	if cfg.Audience == "" {
		return nil, nil, fmt.Errorf("jwtauth: audience is required")
	}
	if cfg.PrincipalClaim == "" {
		cfg.PrincipalClaim = "sub"
	}
	if cfg.Domain == "" {
		cfg.Domain = "jwt"
	}

	if cfg.JWKSURI == "" {
		return nil, nil, fmt.Errorf("jwtauth: jwks_uri is required")
	}

	// Create JWKS keyfunc with auto-refresh; cancel stops background goroutine
	ctx, cancel := context.WithCancel(context.Background())
	k, err := keyfunc.NewDefaultCtx(ctx, []string{cfg.JWKSURI})
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("jwtauth: failed to create JWKS keyfunc: %w", err)
	}

	cleanup := func() {
		cancel()
	}

	parserOpts := []jwt.ParserOption{
		jwt.WithIssuer(cfg.Issuer),
		jwt.WithAudience(cfg.Audience),
		jwt.WithValidMethods([]string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}),
	}

	authFunc := func(r *http.Request) (*vgirpc.AuthContext, error) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			return nil, &vgirpc.RpcError{
				Type:    "ValueError",
				Message: "Missing Authorization header",
			}
		}
		if !strings.HasPrefix(authHeader, "Bearer ") {
			return nil, &vgirpc.RpcError{
				Type:    "ValueError",
				Message: "Authorization header must use Bearer scheme",
			}
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		token, err := jwt.Parse(tokenStr, k.KeyfuncCtx(r.Context()), parserOpts...)
		if err != nil {
			return nil, &vgirpc.RpcError{
				Type:    "ValueError",
				Message: fmt.Sprintf("Invalid token: %v", err),
			}
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			return nil, &vgirpc.RpcError{
				Type:    "ValueError",
				Message: "Failed to parse token claims",
			}
		}

		// Extract principal
		principal := ""
		if v, ok := claims[cfg.PrincipalClaim]; ok {
			principal = fmt.Sprintf("%v", v)
		}

		// Convert claims to map[string]any
		claimsMap := make(map[string]any, len(claims))
		for ck, cv := range claims {
			claimsMap[ck] = cv
		}

		return &vgirpc.AuthContext{
			Domain:        cfg.Domain,
			Authenticated: true,
			Principal:     principal,
			Claims:        claimsMap,
		}, nil
	}

	return authFunc, cleanup, nil
}
