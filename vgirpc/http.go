// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	arrowContentType   = "application/vnd.apache.arrow.stream"
	rpcErrorHeader     = "X-VGI-RPC-Error"
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
	server                  *Server
	tokenKey                []byte
	tokenTTL                time.Duration
	maxBodySize             int64
	maxDecompressedBodySize int64 // 0 = derive as maxBodySize*16
	prefix                  string
	mux                     *http.ServeMux
	zstdEncoderLevel        int // > 0 when response compression is enabled

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

	// Server-vended client externalization (capability advertisement +
	// __upload_url__/init route + 413 enforcement on inline POSTs).
	uploadURLProvider UploadURLProvider
	maxRequestBytes   int64 // 0 = no limit / not advertised
	maxUploadBytes    int64 // 0 = not advertised

	// Response caps (advertised + enforced). 0 = unbounded.
	maxResponseBytes             int64
	maxExternalizedResponseBytes int64

	// Sticky sessions (HTTP-only, opt-in). nil unless EnableSticky was called.
	// Mirrors Python make_wsgi_app(enable_sticky=True). The registry's reaper
	// goroutine is started lazily on the first sticky-aware request so it
	// runs in the serve goroutine rather than at construction time.
	stickyRegistry    *sessionRegistry
	stickyEchoHeaders map[string]string
}

// NewHttpServer creates a new HTTP server wrapping an RPC server.
func NewHttpServer(server *Server) *HttpServer {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Sprintf("vgirpc: failed to generate token key: %v", err))
	}
	h := &HttpServer{
		server:      server,
		tokenKey:    key,
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

// NewHttpServerWithKey creates a new HTTP server with a caller-provided
// token key. The key must be at least 16 bytes; shorter inputs are rejected.
// The state-token AEAD construction (XChaCha20-Poly1305) requires exactly
// 32 bytes — keys of any other length are normalized via SHA-256 inside
// the pack/unpack helpers so operator-supplied keys of any reasonable
// length work.
func NewHttpServerWithKey(server *Server, tokenKey []byte) (*HttpServer, error) {
	if len(tokenKey) < 16 {
		return nil, fmt.Errorf("vgirpc: token key must be at least 16 bytes")
	}
	h := &HttpServer{
		server:      server,
		tokenKey:    tokenKey,
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

// Capability advertisement header names. Mirror the Python reference
// (vgi_rpc/http/_common.py) so cross-implementation clients agree.
const (
	maxRequestBytesHeader    = "VGI-Max-Request-Bytes"
	uploadURLHeader          = "VGI-Upload-URL-Support"
	maxUploadBytesHeader     = "VGI-Max-Upload-Bytes"
	supportedEncodingsHeader = "VGI-Supported-Encodings"
	capabilityCacheMaxAge    = 300 // seconds; OPTIONS Cache-Control max-age
)

// addCapabilityHeaders writes the advertised capability headers (when
// configured) on every response. On OPTIONS responses an additional
// Cache-Control: public, max-age=N header is set so clients can cache
// the discovered values for the advertised TTL.
func (h *HttpServer) addCapabilityHeaders(w http.ResponseWriter, isOptions bool) {
	// Advertise the codec set the server can decode on requests AND
	// produce on responses. Capability-aware clients cache this header
	// and pick a codec from the intersection; absence is interpreted as
	// {zstd} per the Python reference, so emitting both keeps gzip-only
	// clients (e.g. environments without zstandard installed) working.
	w.Header().Set(supportedEncodingsHeader, strings.Join(supportedEncodings, ", "))
	if h.maxRequestBytes > 0 {
		w.Header().Set(maxRequestBytesHeader, strconv.FormatInt(h.maxRequestBytes, 10))
	}
	if h.maxResponseBytes > 0 {
		w.Header().Set(maxResponseBytesHeader, strconv.FormatInt(h.maxResponseBytes, 10))
	}
	if h.maxExternalizedResponseBytes > 0 {
		w.Header().Set(maxExternalizedResponseBytesHeader, strconv.FormatInt(h.maxExternalizedResponseBytes, 10))
	}
	// Always present so capability-aware clients can decide whether to
	// expect externalised payloads.
	if h.server.externalConfig != nil && h.server.externalConfig.Storage != nil {
		w.Header().Set(externalizationEnabledHeader, "true")
	} else {
		w.Header().Set(externalizationEnabledHeader, "false")
	}
	if h.uploadURLProvider != nil {
		w.Header().Set(uploadURLHeader, "true")
		if h.maxUploadBytes > 0 {
			w.Header().Set(maxUploadBytesHeader, strconv.FormatInt(h.maxUploadBytes, 10))
		}
	}
	h.addStickyCapabilityHeaders(w)
	if isOptions {
		w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", capabilityCacheMaxAge))
	}
}

// isMaxBytesExempt returns true for routes that should bypass the
// max_request_bytes enforcement: __upload_url__ (its payloads are
// intrinsically small) and health checks.
func (h *HttpServer) isMaxBytesExempt(path string) bool {
	for _, base := range []string{
		h.prefix + "/__upload_url__",
		h.prefix + "/health",
		"/health",
	} {
		if path == base || strings.HasPrefix(path, base+"/") {
			return true
		}
	}
	return false
}

// addCorsHeaders adds CORS response headers when cors is enabled.
// When isOptions is true, the Access-Control-Max-Age header is included.
//
// Access-Control-Allow-Headers echoes the preflight's
// Access-Control-Request-Headers when present (so any client request header is
// permitted under a wildcard origin), falling back to the common pair. This
// matches the Python vgi-rpc worker; r may be nil for non-preflight responses.
func (h *HttpServer) addCorsHeaders(w http.ResponseWriter, r *http.Request, isOptions bool) {
	if h.corsOrigins != "" {
		w.Header().Set("Access-Control-Allow-Origin", h.corsOrigins)
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		allowHeaders := "Content-Type, Authorization"
		if r != nil {
			if requested := r.Header.Get("Access-Control-Request-Headers"); requested != "" {
				allowHeaders = requested
			}
		}
		w.Header().Set("Access-Control-Allow-Headers", allowHeaders)
		w.Header().Set("Access-Control-Expose-Headers", "WWW-Authenticate, X-Request-ID, X-VGI-Content-Encoding, X-VGI-RPC-Error, "+maxResponseBytesHeader+", "+maxExternalizedResponseBytesHeader+", "+externalizationEnabledHeader+", "+supportedEncodingsHeader+", "+stickyEnabledHeader+", "+stickyDefaultTTLHeader+", "+stickyEchoHeadersHeader+", "+stickySessionHeader+", "+stickySessionCloseHeader)
		// Opt responses into cross-origin embedding so the service is usable
		// from cross-origin-isolated pages (COEP: require-corp), e.g. browsers
		// running multithreaded WASM (DuckDB-WASM) against this worker.
		w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
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
	h.mux.HandleFunc(fmt.Sprintf("POST %s/__upload_url__/init", h.prefix), h.handleUploadURLInit)
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
		// Token-exchange proxy: lets SPA PKCE clients (which cannot safely
		// hold a client_secret) complete authorization_code/refresh_token
		// exchanges against IdPs that require a client_secret (e.g. Google).
		h.mux.HandleFunc(fmt.Sprintf("POST %s/_oauth/token", h.prefix), h.handleOAuthTokenProxy)
		h.mux.HandleFunc(fmt.Sprintf("OPTIONS %s/_oauth/token", h.prefix), h.handleOAuthTokenProxy)
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

// SetUploadURLProvider configures a provider that issues pre-signed
// upload/download URL pairs for server-vended client externalization.
// When set, the server registers POST {prefix}/__upload_url__/init and
// advertises VGI-Upload-URL-Support: true on every response.
func (h *HttpServer) SetUploadURLProvider(p UploadURLProvider) {
	h.uploadURLProvider = p
	h.initRoutes()
}

// SetMaxRequestBytes sets the maximum inline request body size in bytes
// the server will accept on RPC routes. Requests with a Content-Length
// exceeding this limit receive HTTP 413. The value is also advertised
// via the VGI-Max-Request-Bytes capability header. Set to 0 to disable
// (no advertisement, no enforcement). The /__upload_url__ and /health
// routes are exempt from enforcement.
func (h *HttpServer) SetMaxRequestBytes(n int64) {
	h.maxRequestBytes = n
}

// SetMaxUploadBytes sets the maximum upload size advertised via
// VGI-Max-Upload-Bytes when an upload-URL provider is configured.
// Set to 0 to omit the advertisement.
func (h *HttpServer) SetMaxUploadBytes(n int64) {
	h.maxUploadBytes = n
}

// SetMaxBodySize sets the maximum allowed HTTP wire request body size in
// bytes. The decompressed cap defaults to 16x this value unless explicitly
// overridden via SetMaxDecompressedBodySize. Set to 0 to disable the limit
// (not recommended for production).
func (h *HttpServer) SetMaxBodySize(n int64) {
	h.maxBodySize = n
}

// SetMaxDecompressedBodySize sets the maximum allowed decompressed HTTP
// request body size in bytes. When unset (0), the cap is derived as
// maxBodySize*16 — generous enough for normal zstd ratios on Arrow IPC
// bodies, tight enough that a tiny compressed body cannot inflate to
// hundreds of MB. Set to a positive value to override; set to a negative
// value to disable the cap entirely (matches Python's None).
func (h *HttpServer) SetMaxDecompressedBodySize(n int64) {
	h.maxDecompressedBodySize = n
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
	sessKey := deriveSessionKey(h.tokenKey)

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

	// When a client_secret is configured, advertise the token-proxy URL in the
	// resource metadata so SPA PKCE clients can complete token exchanges
	// without holding the secret themselves. Re-marshal the JSON with the
	// extra field merged in.
	if h.oauthMetadata.ClientSecret != "" {
		proxyURL := fmt.Sprintf("%s://%s%s/_oauth/token", resourceURL.Scheme, resourceURL.Host, h.prefix)
		merged, err := mergeTokenEndpointIntoMetadata(h.oauthMetadataJSON, proxyURL)
		if err != nil {
			return fmt.Errorf("vgirpc: advertising token proxy URL: %w", err)
		}
		h.oauthMetadataJSON = merged
	}
	return nil
}

// mergeTokenEndpointIntoMetadata adds a "token_endpoint" field to the
// pre-rendered resource metadata JSON. Re-marshaling preserves all original
// fields without requiring the OAuthResourceMetadata struct to carry a
// proxy-specific field that callers shouldn't have to populate.
func mergeTokenEndpointIntoMetadata(original []byte, tokenEndpoint string) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(original, &m); err != nil {
		return nil, err
	}
	m["token_endpoint"] = tokenEndpoint
	return json.Marshal(m)
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
	// Fire the on_serve_start hook lazily on the first request so pre-fork
	// servers wire each child correctly. notifyTransport is idempotent for
	// repeat calls with the same kind. If the hook fails, refuse the
	// request — the transport binding has not been committed and a retry
	// will re-fire the hook.
	if err := h.server.notifyTransport(TransportKindHTTP, nil); err != nil {
		http.Error(w, fmt.Sprintf("server startup hook failed: %v", err), http.StatusServiceUnavailable)
		return
	}

	// Auto-initialize pages on first request if not done explicitly.
	if h.landingHTML == nil && h.describeHTML == nil && h.notFoundHTML == nil {
		h.InitPages()
	}

	// Capability headers go on every response so clients can probe via
	// any verb (typically OPTIONS /health). On OPTIONS we also add a
	// Cache-Control so well-behaved clients honour the advertised TTL.
	h.addCapabilityHeaders(w, r.Method == http.MethodOptions)

	// CORS preflight — respond before auth or dispatch.
	if r.Method == http.MethodOptions {
		// The OAuth token-proxy needs Origin-allowlist CORS that's tighter
		// than the global SetCorsOrigins value (which is typically "*"),
		// so route its preflight through the dedicated handler.
		if h.pkce != nil && r.URL.Path == h.prefix+"/_oauth/token" {
			h.handleOAuthTokenProxy(w, r)
			return
		}
		h.addCorsHeaders(w, r, true)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Add CORS headers to all non-OPTIONS responses.
	h.addCorsHeaders(w, r, false)

	// Enforce the advertised max_request_bytes cap on RPC routes (the
	// upload-URL and health routes are exempt because their payloads
	// are intrinsically tiny and not user-controlled).
	if h.maxRequestBytes > 0 && r.ContentLength > h.maxRequestBytes && !h.isMaxBytesExempt(r.URL.Path) {
		http.Error(w, fmt.Sprintf(
			"Request body of %d bytes exceeds max_request_bytes=%d. Use the upload-URL flow (__upload_url__/init) to externalize large inputs.",
			r.ContentLength, h.maxRequestBytes,
		), http.StatusRequestEntityTooLarge)
		return
	}

	if h.zstdEncoderLevel > 0 {
		if enc := chooseResponseEncoding(r.Header.Get("Accept-Encoding")); enc != "" {
			cw := &compressResponseWriter{ResponseWriter: w, encoderLevel: h.zstdEncoderLevel, encoding: enc}
			defer cw.finish()
			h.mux.ServeHTTP(cw, r)
			return
		}
	}
	h.mux.ServeHTTP(w, r)
}

// compressResponseWriter buffers the response body and compresses it with zstd
