# Authentication

vgi-rpc-go supports optional HTTP request authentication. When configured, each HTTP request passes through an authentication callback that extracts an `AuthContext`. The auth context is then available to method handlers via `CallContext.Auth` and to dispatch hooks via `DispatchInfo.Auth`.

## Overview

1. Register an `AuthenticateFunc` callback on the `HttpServer` via `SetAuthenticate`.
2. On each HTTP request, the callback receives the `*http.Request` and returns an `*AuthContext` or an error.
3. If the callback returns an error, the request is rejected (401 or 500). If it returns an `*AuthContext`, the context flows into `CallContext` and `DispatchInfo`.
4. When no callback is registered, all requests receive `Anonymous()`.

## AuthContext

```go
type AuthContext struct {
    Domain        string         // auth scheme: "bearer", "jwt", etc.
    Authenticated bool           // true when successfully authenticated
    Principal     string         // caller identity
    Claims        map[string]any // arbitrary claims from auth token
}
```

### Anonymous()

`vgirpc.Anonymous()` returns a shared `*AuthContext` with all zero values (`Authenticated: false`). It is used for unauthenticated requests and all stdio transport requests.

### RequireAuthenticated()

```go
func (a *AuthContext) RequireAuthenticated() error
```

Returns `nil` if `Authenticated` is true, or a `*RpcError{Type: "PermissionError", Message: "Authentication required"}` otherwise. Use this in handlers for a quick auth gate.

## Setting up authentication

```go
httpServer := vgirpc.NewHttpServer(server)

httpServer.SetAuthenticate(func(r *http.Request) (*vgirpc.AuthContext, error) {
    auth := r.Header.Get("Authorization")
    if !strings.HasPrefix(auth, "Bearer ") {
        return nil, &vgirpc.RpcError{
            Type:    "ValueError",
            Message: "Missing or invalid Authorization header",
        }
    }
    token := strings.TrimPrefix(auth, "Bearer ")

    // Validate the token (e.g. JWT verification, database lookup, etc.)
    claims, err := validateToken(token)
    if err != nil {
        return nil, &vgirpc.RpcError{
            Type:    "PermissionError",
            Message: "Invalid or expired token",
        }
    }

    return &vgirpc.AuthContext{
        Domain:        "bearer",
        Authenticated: true,
        Principal:     claims.Subject,
        Claims:        map[string]any{"roles": claims.Roles},
    }, nil
})
```

## Accessing auth in handlers

The `AuthContext` is available on `CallContext.Auth`:

```go
vgirpc.Unary(server, "get_profile", func(ctx context.Context, callCtx *vgirpc.CallContext, p ProfileParams) (Profile, error) {
    // Require authentication
    if err := callCtx.Auth.RequireAuthenticated(); err != nil {
        return Profile{}, err
    }

    // Use the principal identity
    user := callCtx.Auth.Principal

    // Check claims
    if roles, ok := callCtx.Auth.Claims["roles"].([]string); ok {
        // ...
    }

    return fetchProfile(user)
})
```

## Error handling

The `AuthenticateFunc` callback controls how auth failures are reported:

| Error type | HTTP status |
|---|---|
| `*RpcError{Type: "ValueError"}` | 401 Unauthorized |
| `*RpcError{Type: "PermissionError"}` | 401 Unauthorized |
| Any other error | 500 Internal Server Error |

This matches the Python vgi-rpc implementation where `ValueError` and `PermissionError` map to 401.

For 500 errors, only "Internal server error" is sent to the client; the actual error is logged server-side via `slog.Error`.

## Streaming

Authentication is performed once per HTTP request, not per streaming tick/exchange. The `AuthContext` from the initial request is available in:

- The **init handler** `CallContext.Auth` (via `/init` endpoint)
- Each **Produce/Exchange call** `CallContext.Auth` (via `/exchange` endpoint, re-authenticated on each HTTP request)

Auth state is **not** serialized into state tokens. Each HTTP request authenticates independently, which means exchange continuations re-authenticate on every round-trip.

## Built-in auth helpers

The `vgirpc` package provides several ready-made `AuthenticateFunc` factories that cover common authentication patterns.

### Bearer Token Authentication

`BearerAuthenticate` extracts a `Bearer` token from the `Authorization` header and passes it to a user-supplied validation function:

```go
httpServer.SetAuthenticate(vgirpc.BearerAuthenticate(func(token string) (*vgirpc.AuthContext, error) {
    user, err := myTokenDB.Lookup(token)
    if err != nil {
        return nil, &vgirpc.RpcError{Type: "PermissionError", Message: "invalid token"}
    }
    return &vgirpc.AuthContext{
        Domain:        "bearer",
        Authenticated: true,
        Principal:     user.Email,
    }, nil
}))
```

`BearerAuthenticateStatic` is a convenience wrapper for a fixed set of tokens:

```go
httpServer.SetAuthenticate(vgirpc.BearerAuthenticateStatic(map[string]*vgirpc.AuthContext{
    "secret-token-1": {Domain: "bearer", Authenticated: true, Principal: "alice"},
    "secret-token-2": {Domain: "bearer", Authenticated: true, Principal: "bob"},
}))
```

### Chain Authenticate

`ChainAuthenticate` tries multiple authenticators in order. A `ValueError` from one authenticator falls through to the next; a `PermissionError` or non-RPC error propagates immediately:

```go
jwtAuth, cleanup, _ := jwtauth.NewAuthenticateFunc(jwtauth.JWTAuthConfig{...})
defer cleanup()

staticAuth := vgirpc.BearerAuthenticateStatic(map[string]*vgirpc.AuthContext{
    "dev-token": {Domain: "bearer", Authenticated: true, Principal: "developer"},
})

httpServer.SetAuthenticate(vgirpc.ChainAuthenticate(jwtAuth, staticAuth))
```

### Mutual TLS (mTLS) Authentication

vgi-rpc-go supports mTLS authentication for services behind TLS-terminating proxies. The proxy verifies client certificates and forwards certificate information as HTTP headers. vgi-rpc provides factories that extract identity from these headers.

> **Warning:** The reverse proxy **MUST** strip client-supplied `X-SSL-Client-Cert` / `x-forwarded-client-cert` headers before forwarding. Failure to do so allows clients to forge certificate identity. These factories trust the header unconditionally — certificate chain validation is the proxy's responsibility.

Two header conventions are supported:

| Convention | Proxies | Header | Go deps |
|---|---|---|---|
| **PEM-in-header** | nginx, AWS ALB, Cloudflare | `X-SSL-Client-Cert` (configurable) | stdlib only |
| **XFCC** | Envoy | `x-forwarded-client-cert` | stdlib only |

#### MtlsAuthenticate

Generic factory with full control over certificate validation. Parses a URL-encoded PEM certificate from a proxy header and delegates to a user-supplied `Validate` callback:

```go
auth, err := vgirpc.MtlsAuthenticate(vgirpc.MtlsAuthenticateConfig{
    Validate: func(cert *x509.Certificate) (*vgirpc.AuthContext, error) {
        cn := cert.Subject.CommonName
        if cn == "" {
            return nil, &vgirpc.RpcError{Type: "ValueError", Message: "missing CN"}
        }
        return &vgirpc.AuthContext{
            Domain:        "mtls",
            Authenticated: true,
            Principal:     cn,
            Claims:        map[string]any{"serial": fmt.Sprintf("%x", cert.SerialNumber)},
        }, nil
    },
    // Header: "X-SSL-Client-Cert",  // default
    // CheckExpiry: false,            // default
})
if err != nil {
    log.Fatal(err)
}
httpServer.SetAuthenticate(auth)
```

Common header names by proxy:

| Proxy | Header |
|---|---|
| nginx | `X-SSL-Client-Cert` (default) |
| AWS ALB | `X-Amzn-Mtls-Clientcert` |
| Cloudflare | `X-SSL-Client-Cert` |

#### MtlsAuthenticateFingerprint

Convenience factory that looks up certificates by SHA-256 fingerprint (lowercase hex, no colons):

```go
auth, err := vgirpc.MtlsAuthenticateFingerprint(vgirpc.MtlsAuthenticateFingerprintConfig{
    Fingerprints: map[string]*vgirpc.AuthContext{
        "a1b2c3d4...": {Domain: "mtls", Authenticated: true, Principal: "service-a"},
        "f6e5d4c3...": {Domain: "mtls", Authenticated: true, Principal: "service-b"},
    },
    // Algorithm: "sha256",  // default; also "sha1", "sha384", "sha512"
})
if err != nil {
    log.Fatal(err)
}
httpServer.SetAuthenticate(auth)
```

Get a fingerprint with: `openssl x509 -fingerprint -sha256 -noout -in cert.pem | sed 's/.*=//; s/://g' | tr '[:upper:]' '[:lower:]'`

#### MtlsAuthenticateSubject

Extracts the Subject Common Name as principal and populates claims with certificate metadata:

```go
auth, err := vgirpc.MtlsAuthenticateSubject(vgirpc.MtlsAuthenticateSubjectConfig{
    AllowedSubjects: map[string]struct{}{
        "frontend":    {},
        "batch-worker": {},
    },
    CheckExpiry: true,
})
if err != nil {
    log.Fatal(err)
}
httpServer.SetAuthenticate(auth)
```

The returned `AuthContext.Claims` contains:

| Claim | Description |
|---|---|
| `subject_dn` | Full Distinguished Name (Go `pkix.Name.String()` format) |
| `serial` | Certificate serial number (hex) |
| `not_valid_after` | Expiry timestamp (RFC 3339) |

#### MtlsAuthenticateXfcc

Parses the Envoy `x-forwarded-client-cert` header. No certificate parsing needed — identity is extracted from the structured text header:

```go
// Default: extract CN from Subject field
auth, err := vgirpc.MtlsAuthenticateXfcc(vgirpc.MtlsAuthenticateXfccConfig{})
if err != nil {
    log.Fatal(err)
}
httpServer.SetAuthenticate(auth)

// Custom validation (e.g. SPIFFE ID)
auth, err = vgirpc.MtlsAuthenticateXfcc(vgirpc.MtlsAuthenticateXfccConfig{
    Validate: func(elem vgirpc.XfccElement) (*vgirpc.AuthContext, error) {
        if elem.URI == "" || !strings.HasPrefix(elem.URI, "spiffe://") {
            return nil, &vgirpc.RpcError{Type: "ValueError", Message: "Missing SPIFFE ID"}
        }
        return &vgirpc.AuthContext{
            Domain:        "spiffe",
            Authenticated: true,
            Principal:     elem.URI,
        }, nil
    },
    SelectElement: "first", // "first" (original client) or "last" (nearest proxy)
})
if err != nil {
    log.Fatal(err)
}
httpServer.SetAuthenticate(auth)
```

#### Combining mTLS with other authenticators

Use `ChainAuthenticate` to accept mTLS or bearer tokens:

```go
mtlsAuth, err := vgirpc.MtlsAuthenticateSubject(vgirpc.MtlsAuthenticateSubjectConfig{
    AllowedSubjects: map[string]struct{}{"backend-svc": {}},
})
if err != nil {
    log.Fatal(err)
}
apiKeyAuth := vgirpc.BearerAuthenticateStatic(map[string]*vgirpc.AuthContext{
    "sk-ci-bot": {Domain: "apikey", Authenticated: true, Principal: "ci-bot"},
})
httpServer.SetAuthenticate(vgirpc.ChainAuthenticate(mtlsAuth, apiKeyAuth))
```

### Summary of auth factories

| Factory | Package | Description |
|---|---|---|
| `BearerAuthenticate` | `vgirpc` | Bearer token with custom validation |
| `BearerAuthenticateStatic` | `vgirpc` | Bearer token with fixed token map |
| `MtlsAuthenticate` | `vgirpc` | PEM certificate with custom validation |
| `MtlsAuthenticateFingerprint` | `vgirpc` | PEM certificate fingerprint lookup |
| `MtlsAuthenticateSubject` | `vgirpc` | PEM certificate subject CN extraction |
| `MtlsAuthenticateXfcc` | `vgirpc` | Envoy XFCC header parsing |
| `ChainAuthenticate` | `vgirpc` | Try multiple authenticators in order |
| `jwtauth.NewAuthenticateFunc` | `vgirpc/jwtauth` | JWT validation via JWKS |

## OAuth Protected Resource Metadata (RFC 9728)

vgi-rpc-go can expose [RFC 9728](https://www.rfc-editor.org/rfc/rfc9728.html) OAuth Protected Resource Metadata, which tells clients how to authenticate with the service's authorization server.

### Setting up metadata

```go
if err := httpServer.SetOAuthResourceMetadata(&vgirpc.OAuthResourceMetadata{
    Resource:             "https://api.example.com/vgi",
    AuthorizationServers: []string{"https://auth.example.com"},
    ScopesSupported:      []string{"read", "write"},
    ClientID:             "my-app-client-id",
    UseIDTokenAsBearer:   true,
}); err != nil {
    log.Fatal(err)
}
```

This configures two things:

1. A well-known endpoint at `/.well-known/oauth-protected-resource/vgi` serving the metadata JSON.
2. A `WWW-Authenticate` header on 401 responses:
   ```
   Bearer resource_metadata="https://api.example.com/.well-known/oauth-protected-resource/vgi", client_id="my-app-client-id"
   ```

### ClientID

The `ClientID` field is a custom RFC 9728 extension that tells OAuth clients which `client_id` to use when authenticating with the authorization server. This is useful when the resource server knows the expected client application identity. When set, it is included both in the metadata JSON and in the `WWW-Authenticate` challenge header.

The value must contain only URL-safe characters (`A-Z`, `a-z`, `0-9`, `-`, `.`, `_`, `~`). An empty string omits the field.

### UseIDTokenAsBearer

The `UseIDTokenAsBearer` field is a custom RFC 9728 extension that signals OAuth clients to use the OIDC `id_token` (instead of the `access_token`) as the Bearer token. When set to `true`, it is included both in the metadata JSON and in the `WWW-Authenticate` challenge header as `use_id_token_as_bearer="true"`.

### Client helpers

The `vgirpc` package provides helpers for clients to parse OAuth metadata:

```go
// Fetch metadata from a service's well-known endpoint
meta, err := vgirpc.FetchOAuthResourceMetadata("https://api.example.com/vgi")

// Extract resource_metadata URL from a 401 WWW-Authenticate header
metaURL := vgirpc.ParseResourceMetadataURL(wwwAuth)

// Extract client_id from a 401 WWW-Authenticate header
clientID := vgirpc.ParseClientID(wwwAuth)

// Check if the server wants clients to use the id_token as Bearer
useIDToken := vgirpc.ParseUseIDTokenAsBearer(wwwAuth)
```

## Stdio transport

The stdio transport (`server.RunStdio()` / `server.Serve()`) always sets `CallContext.Auth` to `Anonymous()`. Authentication over stdio is not supported since there is no HTTP request to inspect.

Transport metadata from IPC custom metadata (`req.Metadata`) is still available on `CallContext.TransportMetadata`.

## Observability

`DispatchInfo.Auth` carries the auth context for each dispatch. Use this in a `DispatchHook` to add auth-related attributes to traces or metrics:

```go
func (h *myHook) OnDispatchStart(ctx context.Context, info vgirpc.DispatchInfo) (context.Context, vgirpc.HookToken) {
    if info.Auth.Authenticated {
        // Add principal to span, metric labels, etc.
        _ = info.Auth.Principal
    }
    // ...
}
```
