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
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/golang-jwt/jwt/v5"
)

// JWTAuthConfig configures JWT-based authentication.
type JWTAuthConfig struct {
	// Issuer is the expected "iss" claim. Required.
	Issuer string
	// Audience is the list of accepted "aud" values. A token is valid if its
	// audience claim contains ANY of these values. Required (at least one).
	Audience []string
	// JWKSURI is the JWKS endpoint URL. Required.
	JWKSURI string
	// PrincipalClaim is the JWT claim used as the principal identity.
	// Defaults to "sub" if empty.
	PrincipalClaim string
	// Domain is the authentication domain name. Defaults to "jwt" if empty.
	Domain string

	// The options below are opt-in hardening. Setting any of them selects
	// the hardened verifier, which differs from the default one in three
	// ways: rejections are *vgirpc.AuthFailure values naming their reason
	// (missing_credential, invalid_credential, expired_credential), which end
	// a [vgirpc.ChainAuthenticate] chain rather than passing to the next
	// authenticator, so put it last; claims keep their JSON types (integers
	// are int64, not float64); and JWKS keys are held by a fail-closed cache
	// instead of keyfunc's background refresh.

	// Algorithms is the JWS alg allow-list. nil keeps the RS*/ES* default.
	// "none" and HS* are refused.
	Algorithms []string
	// RequiredTyp, when set (e.g. "at+jwt" for RFC 9068 access tokens),
	// requires the JOSE header typ to match, case-insensitively and with the
	// "application/" prefix optional.
	RequiredTyp string
	// RequireExp rejects a token without exp. An exp that is present is
	// always checked.
	RequireExp bool
	// SingleAudience requires the token's aud to name exactly one value. A
	// token naming several is refused even if one matches.
	SingleAudience bool
	// Leeway is the clock skew allowed on exp, nbf and iat.
	Leeway time.Duration
	// JWKSMaxAge is the hard upper bound on how long fetched keys are used.
	// A shorter Cache-Control max-age on the JWKS response wins. When it
	// passes the keys are discarded; if the re-fetch fails, verification
	// fails (AuthUnavailableError, HTTP 503) rather than using stale keys.
	// Zero keeps keys until an unknown kid triggers a re-fetch.
	JWKSMaxAge time.Duration
	// JWKSRefreshMinInterval is the minimum time between JWKS fetches
	// triggered by an unknown kid. Within it, a token whose kid is unknown is
	// rejected without a fetch. Zero re-fetches on every unknown kid.
	JWKSRefreshMinInterval time.Duration
	// JWKSFetchTimeout bounds each JWKS request. Zero means no timeout
	// beyond HTTPClient's.
	JWKSFetchTimeout time.Duration
	// HTTPClient fetches the JWKS. nil uses http.DefaultClient.
	HTTPClient *http.Client
}

// NewAuthenticateFunc creates a [vgirpc.AuthenticateFunc] that validates JWT
// Bearer tokens using JWKS. It returns the auth callback, a cleanup function
// that should be called when the server shuts down (to stop background JWKS
// refresh), and any initialization error.
func NewAuthenticateFunc(cfg JWTAuthConfig) (vgirpc.AuthenticateFunc, func(), error) {
	if cfg.Issuer == "" {
		return nil, nil, fmt.Errorf("jwtauth: issuer is required")
	}
	if len(cfg.Audience) == 0 {
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
	if cfg.hardened() {
		return newHardenedAuthenticateFunc(cfg)
	}

	// Create JWKS keyfunc with auto-refresh; cancel stops background goroutine.
	//
	// NewDefaultCtx configures unknown-KID refresh via a token-bucket
	// rate limiter (rate.Every(5*time.Minute), burst 1), which means the
	// "refetch stampede during key rotation" failure mode the Python port
	// had to patch (vgi-rpc ccf4058) is already prevented here: N
	// concurrent unknown-KID JWTs cannot trigger N concurrent JWKS GETs.
	ctx, cancel := context.WithCancel(context.Background())
	k, err := keyfunc.NewDefaultCtx(ctx, []string{cfg.JWKSURI})
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("jwtauth: failed to create JWKS keyfunc: %w", err)
	}

	cleanup := func() {
		cancel()
	}

	acceptedAudiences := cfg.Audience

	parserOpts := []jwt.ParserOption{
		jwt.WithIssuer(cfg.Issuer),
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

		// Validate audience: token aud must contain at least one accepted audience
		tokenAudiences, err := claims.GetAudience()
		if err != nil || !audienceIntersects(tokenAudiences, acceptedAudiences) {
			return nil, &vgirpc.RpcError{
				Type:    "ValueError",
				Message: "Invalid token: token audience does not match any accepted audience",
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

// audienceIntersects returns true if any element in tokenAud appears in accepted.
func audienceIntersects(tokenAud, accepted []string) bool {
	for _, ta := range tokenAud {
		for _, aa := range accepted {
			if ta == aa {
				return true
			}
		}
	}
	return false
}
