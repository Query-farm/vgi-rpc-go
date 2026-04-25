// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	arrowContentType   = "application/vnd.apache.arrow.stream"
	rpcErrorHeader     = "X-VGI-RPC-Error"
	hmacLen            = 32
	defaultTokenTTL    = 5 * time.Minute
	defaultMaxBodySize = 64 << 20 // 64 MB
)

// HttpServer serves RPC requests over HTTP. It wraps a [Server] and exposes
// URL routes under a configurable prefix (default ""):
//
//	POST /{method}           — unary RPC call
//	POST /{method}/init      — stream initialization (producer or exchange)
//	POST /{method}/exchange  — exchange continuation with state token
//	GET  /                   — landing page (HTML)
//	GET  /describe           — API reference page (HTML)
//
// HttpServer implements [http.Handler] and can be used directly with
// [http.ListenAndServe] or mounted on an existing [http.ServeMux].
type HttpServer struct {
	server      *Server
	signingKey  []byte
	tokenTTL    time.Duration
	maxBodySize int64
	prefix      string
	mux         *http.ServeMux
	zstdEncoder *zstd.Encoder // non-nil when response compression is enabled

	rehydrateFunc      RehydrateFunc    // called after unpacking state tokens
	producerBatchLimit int              // max data batches per producer response; 0 = unlimited
	authenticateFunc   AuthenticateFunc // optional auth callback; nil = anonymous

	// OAuth Protected Resource Metadata (RFC 9728)
	oauthMetadata     *OAuthResourceMetadata
	oauthMetadataJSON []byte // pre-rendered JSON
	wwwAuthenticate   string // pre-built WWW-Authenticate header value

	// Pre-rendered HTML pages (built by initPages)
	landingHTML  []byte
	describeHTML []byte
	notFoundHTML []byte

	// Page configuration
	protocolName       string
	repoURL            string
	enableLandingPage  bool
	enableDescribePage bool
	enableNotFoundPage bool

	corsOrigins string // CORS allowed origins; empty = disabled
	corsMaxAge  string // Access-Control-Max-Age value; empty = omit

	pkce *oauthPkceState // non-nil when OAuth PKCE browser login is enabled
}

// NewHttpServer creates a new HTTP server wrapping an RPC server.
func NewHttpServer(server *Server) *HttpServer {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Sprintf("vgirpc: failed to generate signing key: %v", err))
	}
	h := &HttpServer{
		server:      server,
		signingKey:  key,
		tokenTTL:    defaultTokenTTL,
		maxBodySize: defaultMaxBodySize,
		prefix:      "",
		corsMaxAge:  "7200",

		enableLandingPage:  true,
		enableDescribePage: true,
		enableNotFoundPage: true,
	}
	h.initRoutes()
	return h
}

// NewHttpServerWithKey creates a new HTTP server with a caller-provided signing key.
// The key must be at least 16 bytes long.
func NewHttpServerWithKey(server *Server, signingKey []byte) (*HttpServer, error) {
	if len(signingKey) < 16 {
		return nil, fmt.Errorf("vgirpc: signing key must be at least 16 bytes")
	}
	h := &HttpServer{
		server:      server,
		signingKey:  signingKey,
		tokenTTL:    defaultTokenTTL,
		maxBodySize: defaultMaxBodySize,
		prefix:      "",
		corsMaxAge:  "7200",

		enableLandingPage:  true,
		enableDescribePage: true,
		enableNotFoundPage: true,
	}
	h.initRoutes()
	return h, nil
}

// SetProtocolName sets the protocol name displayed on HTML pages.
// If not set, the server's service name is used, falling back to "vgi-rpc Service".
func (h *HttpServer) SetProtocolName(name string) {
	h.protocolName = name
}

// SetPrefix sets the URL path prefix under which RPC routes are mounted
// (e.g. "/vgi"). The prefix must start with "/" or be empty. Must be called
// before the server handles any request, since it rebuilds the route table.
func (h *HttpServer) SetPrefix(prefix string) {
	h.prefix = prefix
	h.initRoutes()
}

// SetRepoURL sets a source repository URL shown on the landing and describe pages.
func (h *HttpServer) SetRepoURL(url string) {
	h.repoURL = url
}

// SetEnableLandingPage controls whether GET requests to the prefix serve an
// HTML landing page. Enabled by default.
func (h *HttpServer) SetEnableLandingPage(enabled bool) {
	h.enableLandingPage = enabled
}

// SetEnableDescribePage controls whether GET {prefix}/describe serves an
// HTML API reference page. Enabled by default.
func (h *HttpServer) SetEnableDescribePage(enabled bool) {
	h.enableDescribePage = enabled
}

// SetEnableNotFoundPage controls whether unmatched routes return a friendly
// HTML 404 page. Enabled by default.
func (h *HttpServer) SetEnableNotFoundPage(enabled bool) {
	h.enableNotFoundPage = enabled
}

// SetCorsOrigins sets the allowed origins for CORS. Pass "*" to allow all
// origins, or a specific origin string like "https://example.com". An empty
// string (the default) disables CORS headers.
func (h *HttpServer) SetCorsOrigins(origins string) {
	h.corsOrigins = origins
}

// SetCorsMaxAge sets the Access-Control-Max-Age header value (in seconds)
// for preflight OPTIONS responses. Pass 0 to omit the header. The default
// is 7200 (2 hours).
func (h *HttpServer) SetCorsMaxAge(seconds int) {
	if seconds > 0 {
		h.corsMaxAge = strconv.Itoa(seconds)
	} else {
		h.corsMaxAge = ""
	}
}

// addCorsHeaders adds CORS response headers when cors is enabled.
// When isOptions is true, the Access-Control-Max-Age header is included.
func (h *HttpServer) addCorsHeaders(w http.ResponseWriter, isOptions bool) {
	if h.corsOrigins != "" {
		w.Header().Set("Access-Control-Allow-Origin", h.corsOrigins)
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Expose-Headers", "WWW-Authenticate, X-Request-ID, X-VGI-Content-Encoding, X-VGI-RPC-Error")
		if isOptions && h.corsMaxAge != "" {
			w.Header().Set("Access-Control-Max-Age", h.corsMaxAge)
		}
	}
}

// initRoutes registers POST routes for RPC and builds the mux. Call once
// from the constructor. HTML GET routes are added lazily by initPages.
func (h *HttpServer) initRoutes() {
	h.mux = http.NewServeMux()
	h.mux.HandleFunc(fmt.Sprintf("POST %s/{method}/init", h.prefix), h.handleStreamInit)
	h.mux.HandleFunc(fmt.Sprintf("POST %s/{method}/exchange", h.prefix), h.handleStreamExchange)
	h.mux.HandleFunc(fmt.Sprintf("POST %s/{method}", h.prefix), h.handleUnary)
	h.mux.HandleFunc(fmt.Sprintf("GET %s", wellKnownURL(h.prefix)), h.handleOAuthWellKnown)
	// Health is registered at /health (root) regardless of the RPC prefix so
	// load balancers don't need to know the application path layout. When a
	// non-empty prefix is configured, also expose it at {prefix}/health for
	// callers that scope all routes under one mount point.
	h.mux.HandleFunc("GET /health", h.handleHealth)
	if h.prefix != "" {
		h.mux.HandleFunc(fmt.Sprintf("GET %s/health", h.prefix), h.handleHealth)
	}
}

// handleHealth serves a JSON health probe at GET {prefix}/health. The endpoint
// bypasses authentication so load balancers and orchestrators (Kubernetes,
// AWS ALB, etc.) can verify liveness without holding credentials. The response
// body has the shape {status, server_id, protocol}.
func (h *HttpServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	protocol := h.protocolName
	if protocol == "" {
		protocol = h.server.serviceName
	}
	body := fmt.Sprintf(`{"status":"ok","server_id":%q,"protocol":%q}`,
		h.server.serverID, protocol)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(body)); err != nil {
		slog.Debug("http: response write failed", "err", err)
	}
}

// handleOAuthWellKnown serves the OAuth Protected Resource Metadata document.
func (h *HttpServer) handleOAuthWellKnown(w http.ResponseWriter, r *http.Request) {
	if h.oauthMetadata == nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=60")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(h.oauthMetadataJSON); err != nil {
		slog.Debug("http: response write failed", "err", err)
	}
}

// InitPages pre-renders the HTML pages and registers GET routes. This must be
// called after all methods are registered on the server and after any
// Set* configuration calls (SetProtocolName, SetRepoURL, etc.).
//
// If not called explicitly, pages are initialized automatically on the first
// HTTP request.
func (h *HttpServer) InitPages() {
	name := h.protocolName
	if name == "" {
		name = h.server.serviceName
	}
	if name == "" {
		name = "vgi-rpc Service"
	}

	serverID := h.server.serverID

	// Register OAuth PKCE routes when enabled
	if h.pkce != nil {
		h.mux.HandleFunc(fmt.Sprintf("GET %s/_oauth/callback", h.prefix), h.handleOAuthCallback)
		h.mux.HandleFunc(fmt.Sprintf("GET %s/_oauth/logout", h.prefix), h.handleOAuthLogout)
	}

	if h.enableDescribePage {
		h.describeHTML = buildDescribeHTML(h.server, h.prefix, name, h.repoURL)
		if h.pkce != nil {
			h.describeHTML = bytes.Replace(h.describeHTML, []byte("</body>"),
				append(append([]byte{}, h.pkce.userInfoHTML...), []byte("\n</body>")...), 1)
		}
		describeHandler := h.handleDescribePage
		if h.pkce != nil {
			describeHandler = h.wrapPageWithPkce(h.handleDescribePage)
		}
		h.mux.HandleFunc(fmt.Sprintf("GET %s/describe", h.prefix), describeHandler)
	}

	if h.enableLandingPage {
		describePath := ""
		if h.enableDescribePage {
			describePath = h.prefix + "/describe"
		}
		h.landingHTML = buildLandingHTML(h.prefix, name, serverID, describePath, h.repoURL)
		if h.pkce != nil {
			h.landingHTML = bytes.Replace(h.landingHTML, []byte("</body>"),
				append(append([]byte{}, h.pkce.userInfoHTML...), []byte("\n</body>")...), 1)
		}
		landingPattern := fmt.Sprintf("GET %s", h.prefix)
		if h.prefix == "" {
			landingPattern = "GET /{$}"
		}
		landingHandler := h.handleLandingPage
		if h.pkce != nil {
			landingHandler = h.wrapPageWithPkce(h.handleLandingPage)
		}
		h.mux.HandleFunc(landingPattern, landingHandler)
	}

	if h.enableNotFoundPage {
		h.notFoundHTML = buildNotFoundHTML(h.prefix, name)
		h.mux.HandleFunc("/", h.handleNotFound)
	}
}

// SetTokenTTL sets the maximum age for state tokens.
func (h *HttpServer) SetTokenTTL(d time.Duration) {
	h.tokenTTL = d
}

// SetMaxBodySize sets the maximum allowed HTTP request body size in bytes.
// The limit applies to both raw and decompressed bodies. Set to 0 to disable
// the limit (not recommended for production).
func (h *HttpServer) SetMaxBodySize(n int64) {
	h.maxBodySize = n
}

// SetCompressionLevel enables zstd compression of response bodies at the
// given level (1–11). When enabled, responses are compressed if the client
// sends an Accept-Encoding header containing "zstd". Pass 0 to disable
// response compression (the default).
// exchange and producer continuation requests.
func (h *HttpServer) SetRehydrateFunc(fn RehydrateFunc) {
	h.rehydrateFunc = fn
}

// SetProducerBatchLimit sets the maximum number of data batches a producer
// emits per HTTP response. When the limit is reached, the server serializes
// the producer state into a continuation token appended to the response.
// The client sends the token back via /exchange to resume production.
// Set to 0 (default) for unlimited batches per response.
func (h *HttpServer) SetProducerBatchLimit(limit int) {
	h.producerBatchLimit = limit
}

// SetAuthenticate registers a callback that extracts authentication
// information from each HTTP request. If the callback returns a non-nil
// error, the request is rejected (see [AuthenticateFunc] for status code
// mapping). When no callback is registered, all requests receive [Anonymous].
func (h *HttpServer) SetAuthenticate(fn AuthenticateFunc) {
	h.authenticateFunc = fn
}

// SetOAuthResourceMetadata configures OAuth Protected Resource Metadata
// (RFC 9728). When set, the server exposes a well-known endpoint and includes
// a WWW-Authenticate header on 401 responses.
func (h *HttpServer) SetOAuthResourceMetadata(m *OAuthResourceMetadata) error {
	if err := m.Validate(); err != nil {
		return fmt.Errorf("vgirpc: %w", err)
	}
	data, err := m.ToJSON()
	if err != nil {
		return fmt.Errorf("vgirpc: failed to marshal oauth metadata: %w", err)
	}
	metaURL, err := resourceMetadataURLFromResource(m.Resource)
	if err != nil {
		return fmt.Errorf("vgirpc: %w", err)
	}
	h.oauthMetadata = m
	h.oauthMetadataJSON = data
	h.wwwAuthenticate = buildWWWAuthenticate(metaURL, m)
	return nil
}

// SetOAuthPkce enables browser-based OAuth PKCE login. When both authenticate
// and OAuthResourceMetadata (with a ClientID) are configured, browser GET
// requests that fail authentication are redirected to the authorization
// server's login page instead of returning a 401.
//
// Returns an error if authenticateFunc or oauthMetadata are not set, if
// ClientID is empty, or if the resource URL is invalid.
func (h *HttpServer) SetOAuthPkce(config OAuthPkceConfig) error {
	if h.authenticateFunc == nil {
		return fmt.Errorf("vgirpc: SetOAuthPkce requires SetAuthenticate to be called first")
	}
	if h.oauthMetadata == nil {
		return fmt.Errorf("vgirpc: SetOAuthPkce requires SetOAuthResourceMetadata to be called first")
	}
	if h.oauthMetadata.ClientID == "" {
		return fmt.Errorf("vgirpc: SetOAuthPkce requires OAuthResourceMetadata.ClientID to be set")
	}
	if len(h.oauthMetadata.AuthorizationServers) == 0 {
		return fmt.Errorf("vgirpc: SetOAuthPkce requires at least one authorization server")
	}

	// Derive session key from signing key
	sessKey := deriveSessionKey(h.signingKey)

	// Parse resource URL for scheme/host → secureCookie, redirectURI
	resourceURL, err := url.Parse(h.oauthMetadata.Resource)
	if err != nil {
		return fmt.Errorf("vgirpc: invalid resource URL: %w", err)
	}
	secureCookie := resourceURL.Scheme == "https"
	redirectURI := fmt.Sprintf("%s://%s%s/_oauth/callback", resourceURL.Scheme, resourceURL.Host, h.prefix)

	// Build OIDC discovery for first authorization server
	oidcDiscovery := createOIDCDiscovery(h.oauthMetadata.AuthorizationServers[0])

	// Scope — prefer the resource metadata's advertised scopes so that
	// authorization requests match what the server publishes. Fall back to
	// the caller-provided config.Scope, and finally to "openid email".
	scope := pkceScopeFromMetadata(h.oauthMetadata, config.Scope)

	// Build allowed origins map
	allowedOrigins := make(map[string]bool)
	allowedOrigins[defaultAllowedReturnOrigin] = true
	for _, origin := range config.AllowedReturnOrigins {
		allowedOrigins[origin] = true
	}

	// Build user info HTML
	userInfoHTML := buildUserInfoHTML(h.prefix)

	// Chain cookie authenticate with the original auth func
	origAuth := h.authenticateFunc
	h.authenticateFunc = ChainAuthenticate(origAuth, CookieAuthenticate(origAuth, authCookieName))

	h.pkce = &oauthPkceState{
		sessionKey:           sessKey,
		oidcDiscovery:        oidcDiscovery,
		clientID:             h.oauthMetadata.ClientID,
		clientSecret:         h.oauthMetadata.ClientSecret,
		useIDToken:           h.oauthMetadata.UseIDTokenAsBearer,
		secureCookie:         secureCookie,
		redirectURI:          redirectURI,
		prefix:               h.prefix,
		scope:                scope,
		allowedReturnOrigins: allowedOrigins,
		userInfoHTML:         userInfoHTML,
	}
	return nil
}

// authenticate runs the registered AuthenticateFunc (if any) and writes
// an error response on failure. Returns nil when auth fails (caller should
// return immediately).
func (h *HttpServer) authenticate(w http.ResponseWriter, r *http.Request) *AuthContext {
	if h.authenticateFunc == nil {
		return Anonymous()
	}
	auth, err := h.authenticateFunc(r)
	if err != nil {
		if rpcErr, ok := err.(*RpcError); ok &&
			(rpcErr.Type == "ValueError" || rpcErr.Type == "PermissionError") {
			if h.wwwAuthenticate != "" {
				w.Header().Set("WWW-Authenticate", h.wwwAuthenticate)
			}
			http.Error(w, rpcErr.Message, http.StatusUnauthorized)
		} else {
			slog.Error("authenticate callback error", "err", err, "remote_addr", r.RemoteAddr)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return nil
	}
	return auth
}

// ServeHTTP implements http.Handler.
func (h *HttpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Auto-initialize pages on first request if not done explicitly.
	if h.landingHTML == nil && h.describeHTML == nil && h.notFoundHTML == nil {
		h.InitPages()
	}

	// CORS preflight — respond before auth or dispatch.
	if r.Method == http.MethodOptions {
		h.addCorsHeaders(w, true)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Add CORS headers to all non-OPTIONS responses.
	h.addCorsHeaders(w, false)

	if h.zstdEncoder != nil && strings.Contains(r.Header.Get("Accept-Encoding"), "zstd") {
		cw := &compressResponseWriter{ResponseWriter: w, encoder: h.zstdEncoder}
		defer cw.finish()
		h.mux.ServeHTTP(cw, r)
		return
	}
	h.mux.ServeHTTP(w, r)
}

// compressResponseWriter buffers the response body and compresses it with zstd
