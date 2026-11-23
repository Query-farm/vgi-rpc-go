// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// Blob-vendor client: region-pinned, single-object pre-signed URLs.
//
// The blob vendor is a small HTTP service that holds the object-storage
// credentials so workers never do. A worker asks it for a pre-signed URL for
// one object of a declared size and SHA-256, uploads straight to storage with
// that URL, and hands a download URL (or an object id) to whoever needs the
// bytes. The storage enforces the signed Content-Length and
// x-amz-checksum-sha256, so whatever sits at the object's key is exactly the
// declared bytes.
//
//   - [VendorStorage] implements both [ExternalStorage] (server-side
//     externalization of large results) and [UploadURLProvider] (vending
//     upload URLs to clients for large requests).
//   - [VendorBlobClient] puts, gets and deletes raw byte blobs, for callers
//     that keep their own references (value-level externalization in a
//     key-value store).
//   - [PinnedHostsValidator] and [VendorURLValidator] accept only URLs on the
//     vendor's storage hosts, for ExternalLocationConfig.URLValidator.
//   - [ExternalConfigFromEnv] / [ClientExternalConfigFromEnv] build configs
//     from the environment for hosted deployments.
//
// Every vendor request carries "Authorization: Bearer <token>". Signed-URL
// requests go through a separate bare client and carry exactly the headers the
// grant names: never the token, a caller's cookies or a caller's transport
// headers. Neither the token nor a signed query string is ever logged or put in
// an error: diagnostics name a URL by scheme, host, port and path only.
//
// Mirrors vgi-rpc-python's vgi_rpc.vendor; the wire format is the vendor's
// (see the vgi extension's docs/testing/blob-test-catalog.md).

// Environment variables read by the *FromEnv helpers.
const (
	// EnvBlobVendorURL is the vendor's base URL. When unset, the *FromEnv
	// helpers return nil.
	EnvBlobVendorURL = "VGI_BLOB_VENDOR_URL"
	// EnvBlobVendorToken is the vendor's bearer token.
	EnvBlobVendorToken = "VGI_BLOB_VENDOR_TOKEN"
	// EnvBlobVendorTokenFallback is consulted when EnvBlobVendorToken is
	// unset: the vendor shares the storage service's per-deployment keys.
	EnvBlobVendorTokenFallback = "VGI_CF_DO_TOKEN"
	// EnvExternalizeThreshold is the result-batch size at or above which the
	// server externalizes (default 1 MiB).
	EnvExternalizeThreshold = "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES"
	// EnvExternalCompression is "none" (default) or "zstd", optionally with a
	// level ("zstd:3").
	EnvExternalCompression = "VGI_RPC_EXTERNAL_COMPRESSION"
	// EnvMaxUploadBytes is the largest client upload the server vends a URL
	// for, advertised as VGI-Max-Upload-Bytes. Unset means "the inbound
	// fetch cap" (EnvMaxFetchBytes), so a server that vends upload URLs
	// always advertises a limit and a client is refused at
	// __upload_url__/init rather than after uploading.
	EnvMaxUploadBytes = "VGI_RPC_MAX_UPLOAD_BYTES"
	// EnvMaxFetchBytes is the largest request the server will fetch from an
	// inbound pointer (ExternalLocationConfig.MaxFetchBytes). Defaults to 256
	// MiB, raised to EnvMaxUploadBytes when that is set higher; both set with
	// the upload size above the fetch cap is a startup error.
	EnvMaxFetchBytes = "VGI_RPC_MAX_FETCH_BYTES"
	// EnvMaxRequestBytes is the inline request cap a server advertises and
	// enforces; larger requests go up through upload URLs.
	EnvMaxRequestBytes = "VGI_RPC_MAX_REQUEST_BYTES"
)

// BlobPurpose is a vendor object's purpose: [BlobPurposeRPC] objects get a
// download URL at put time and a short lifecycle; [BlobPurposeFS] objects are
// fetched with a fresh URL from get.
type BlobPurpose string

// The vendor's object purposes.
const (
	BlobPurposeRPC BlobPurpose = "rpc"
	BlobPurposeFS  BlobPurpose = "fs"
)

// Error codes set by [VendorBlobClient.GetBytes] itself, as opposed to the
// codes the vendor or the storage return.
const (
	VendorCodeSizeMismatch   = "size_mismatch"
	VendorCodeSHA256Mismatch = "sha256_mismatch"
)

const (
	vendorDefaultThreshold = 1_048_576
	vendorDefaultTimeout   = 30 * time.Second
	vendorDefaultInfoTTL   = 600 * time.Second
	vendorDefaultAttempts  = 3
	vendorDefaultBackoff   = 250 * time.Millisecond
	vendorMaxBackoff       = 5 * time.Second
	// vendorMaxDeleteIDs is the vendor's per-call cap on /v1/blobs/delete.
	vendorMaxDeleteIDs = 1000
	// vendorDefaultMaxFetch caps a blob GetBytes will download (the vendor's
	// per-object cap).
	vendorDefaultMaxFetch int64 = 4 << 30
)

// VendorError is a failed blob-vendor or signed-URL request.
//
// Its message never contains the bearer token or a signed query string.
type VendorError struct {
	// Message is the (already redacted) diagnostic.
	Message string
	// Status is the HTTP status of the failing response, or 0 for a
	// transport failure or a malformed response.
	Status int
	// Code is the vendor's machine-readable error code ("not_found",
	// "not_uploaded", "quota_exceeded", "idempotency_conflict"), the storage's
	// XML error code ("BadDigest", "NoSuchKey"), or one of
	// [VendorCodeSizeMismatch] / [VendorCodeSHA256Mismatch]; "" when there
	// was none.
	Code string
}

// Error returns the redacted message.
func (e *VendorError) Error() string { return e.Message }

// BlobScope is the execution a blob belongs to, for scoped clean-up. The
// vendor accepts 1-128 characters of [A-Za-z0-9._:-] in each field, so encode
// binary ids as hex.
type BlobScope struct {
	Shard       string
	ExecutionID string
}

// BlobRef references one vendored blob; small enough to store inline.
type BlobRef struct {
	ObjectID string
	// Size is the exact byte length of the blob.
	Size int64
	// SHA256 is the lowercase hex SHA-256 of the blob bytes.
	SHA256 string
}

// PutGrant is the vendor's answer to put: where and how to upload one object.
type PutGrant struct {
	ObjectID  string
	UploadURL string
	// RequiredHeaders must be sent on the PUT exactly as given (the
	// signature covers them).
	RequiredHeaders map[string]string
	// DownloadURL is set for rpc objects only.
	DownloadURL       string
	ExpiresAt         time.Time
	Region            string
	DownloadExpiresAt time.Time
}

// BlobLocation is the vendor's answer to get: where one object can be downloaded.
type BlobLocation struct {
	DownloadURL     string
	Size            int64
	SHA256B64       string
	ContentEncoding string
	ExpiresAt       time.Time
}

// VendorInfo is the vendor's /v1/info: the storage hosts of the tenant's region.
type VendorInfo struct {
	Region        string
	Tenant        string
	Jurisdiction  string
	UploadHosts   []string
	DownloadHosts []string
	// URLPrefixes are the URL prefixes this tenant's objects live under.
	URLPrefixes []string
	Limits      map[string]int64
	TTLSeconds  map[string]int64
}

// VendorOptions tunes a vendor client. The zero value is the defaults.
type VendorOptions struct {
	// Timeout is the per-request timeout. Default 30s.
	Timeout time.Duration
	// HTTPClient carries vendor API requests (its transport only matters:
	// requests carry exactly the vendor headers, and redirects are never
	// followed). Signed-URL requests always use a separate bare client.
	HTTPClient *http.Client
	// MaxAttempts per request; transport errors, 429 and 5xx are retried
	// with exponential backoff. Default 3.
	MaxAttempts int
	// Backoff is the base delay between attempts. Default 250ms.
	Backoff time.Duration
	// InfoTTL is how long the /v1/info host pin is cached. Default 10m.
	InfoTTL time.Duration
	// MaxFetchBytes caps a blob [VendorBlobClient.GetBytes] downloads.
	// Default 4 GiB.
	MaxFetchBytes int64
	// Logger receives debug diagnostics (never a token or signed query).
	// Default slog.Default().
	Logger *slog.Logger
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func b64SHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func defaultPortFor(scheme string) int {
	if scheme == "https" {
		return 443
	}
	return 80
}

type hostKey struct {
	host string
	port int
}

func newHostKey(host string, port int, scheme string) hostKey {
	if port == 0 {
		port = defaultPortFor(scheme)
	}
	return hostKey{host: strings.ToLower(strings.Trim(host, "[]")), port: port}
}

// parsePort returns the URL's explicit port, 0 when none, or an error.
func parsePort(u *url.URL) (int, error) {
	p := u.Port()
	if p == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(p)
	if err != nil || n < 1 || n > 65535 {
		return 0, fmt.Errorf("invalid port")
	}
	return n, nil
}

func parseVendorTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case float64:
		secs := v
		if v > 1e11 {
			secs = v / 1000
		}
		return time.Unix(0, int64(secs*float64(time.Second))).UTC(), nil
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05"} {
			if t, err := time.Parse(layout, v); err == nil {
				return t.UTC(), nil
			}
		}
	}
	return time.Time{}, errors.New("malformed time")
}

// ---------------------------------------------------------------------------
// URL pinning
// ---------------------------------------------------------------------------

// PinnedHostsValidator builds a URL validator that accepts only URLs on hosts.
//
// A URL passes when its scheme is https (or http with allowHTTP), it carries
// no userinfo, and its host and port are exactly one of hosts. An entry is
// "host" or "host:port" ("[v6]:port" for IPv6); an entry without a port
// matches the scheme's default port only. Subdomains do not match. With
// prefixes, the URL without its query must also start with one of them (e.g.
// a region's bucket path) and its path may not contain a ".." segment.
//
// allowHTTP is only for loopback test vendors; never enable it for a real
// deployment.
func PinnedHostsValidator(hosts []string, allowHTTP bool, prefixes []string) (func(string) error, error) {
	schemes := []string{"https"}
	if allowHTTP {
		schemes = append(schemes, "http")
	}
	allowed := map[hostKey]struct{}{}
	for _, entry := range hosts {
		u, err := url.Parse("//" + strings.TrimSpace(entry))
		if err != nil || u.Hostname() == "" || u.Path != "" || u.RawQuery != "" || u.User != nil {
			return nil, fmt.Errorf("invalid pinned host entry: %q", entry)
		}
		port, err := parsePort(u)
		if err != nil {
			return nil, fmt.Errorf("invalid pinned host entry: %q", entry)
		}
		if port == 0 {
			for _, s := range schemes {
				allowed[newHostKey(u.Hostname(), 0, s)] = struct{}{}
			}
		} else {
			allowed[newHostKey(u.Hostname(), port, "https")] = struct{}{}
		}
	}
	if len(allowed) == 0 {
		return nil, errors.New("PinnedHostsValidator needs at least one host")
	}
	var urlPrefixes []string
	for _, p := range prefixes {
		if p != "" {
			urlPrefixes = append(urlPrefixes, p)
		}
	}
	return func(raw string) error {
		u, err := url.Parse(raw)
		if err != nil {
			return errors.New("URL is malformed")
		}
		port, err := parsePort(u)
		if err != nil {
			return errors.New("URL is malformed")
		}
		schemeOK := false
		for _, s := range schemes {
			if u.Scheme == s {
				schemeOK = true
			}
		}
		if !schemeOK {
			return fmt.Errorf("URL scheme %q not allowed (only %s)", u.Scheme, strings.Join(schemes, " or "))
		}
		if u.User != nil {
			return errors.New("URL userinfo is not allowed")
		}
		if _, ok := allowed[newHostKey(u.Hostname(), port, u.Scheme)]; u.Hostname() == "" || !ok {
			return fmt.Errorf("URL host is not a pinned storage host: %s", redactExternalURL(raw))
		}
		if len(urlPrefixes) > 0 {
			bare := u.Scheme + "://" + u.Host + u.EscapedPath()
			dotdot := false
			for _, seg := range strings.Split(u.EscapedPath(), "/") {
				if seg == ".." {
					dotdot = true
				}
			}
			matched := false
			for _, p := range urlPrefixes {
				if strings.HasPrefix(bare, p) {
					matched = true
				}
			}
			if dotdot || !matched {
				return fmt.Errorf("URL is outside the pinned storage prefixes: %s", redactExternalURL(raw))
			}
		}
		return nil
	}, nil
}

// vendorHostsValidator pins URLs to the vendor's /v1/info hosts, cached.
type vendorHostsValidator struct {
	api       *vendorAPI
	ttl       time.Duration
	mu        sync.Mutex
	validator func(string) error
	fetchedAt time.Time
}

func (v *vendorHostsValidator) current() (func(string) error, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	now := time.Now()
	if v.validator != nil && now.Sub(v.fetchedAt) < v.ttl {
		return v.validator, nil
	}
	info, err := v.api.info(context.Background())
	var validator func(string) error
	if err == nil {
		hosts := append(append([]string(nil), info.UploadHosts...), info.DownloadHosts...)
		validator, err = PinnedHostsValidator(hosts, v.api.loopbackHTTP, info.URLPrefixes)
	}
	if err != nil {
		if v.validator != nil {
			// Keep the last known-good pin through a vendor blip.
			v.api.logger.Warn("blob vendor info refresh failed; keeping cached hosts", "error", err.Error())
			v.fetchedAt = now
			return v.validator, nil
		}
		return nil, fmt.Errorf("cannot pin storage hosts: %w", err)
	}
	v.validator = validator
	v.fetchedAt = now
	return validator, nil
}

func (v *vendorHostsValidator) validate(raw string) error {
	validator, err := v.current()
	if err != nil {
		return err
	}
	return validator(raw)
}

// ---------------------------------------------------------------------------
// HTTP + wire mapping (the one place that knows the vendor's JSON shapes)
// ---------------------------------------------------------------------------

const (
	vendorPutPath         = "/v1/blobs/put"
	vendorGetPath         = "/v1/blobs/get"
	vendorDeletePath      = "/v1/blobs/delete"
	vendorDeleteScopePath = "/v1/blobs/delete_scope"
	vendorInfoPath        = "/v1/info"
)

// ClientUploadOrigin is the advisory "origin" a [VendorStorage] sends on the
// /v1/blobs/put grant it requests for a client to upload a request
// ([VendorStorage.GenerateUploadURL]); the server's own writes send none.
// The vendor ignores the field; test fakes count and fault client uploads by
// it. Matches vgi-rpc-python's CLIENT_UPLOAD_ORIGIN.
const ClientUploadOrigin = "client_upload"

// StateRefOrigin is the advisory origin on a /v1/blobs/put grant for a stored
// StateRef object ([VendorStorage.PutObject]). The vendor ignores it; the
// fake vendor counts those grants (puts_state_ref) and their reads.
const StateRefOrigin = "state_ref"

// noRedirects stops a client following a redirect: one could carry the bearer
// token or a signed URL to a host nobody vetted.
func noRedirects(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// newStorageHTTPClient is the bare client for signed-URL requests: no jar, no
// caller transport, no redirects.
func newStorageHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, CheckRedirect: noRedirects}
}

type vendorAPI struct {
	baseURL      string
	token        string
	loopbackHTTP bool
	timeout      time.Duration
	attempts     int
	backoff      time.Duration
	client       *http.Client
	storage      *http.Client
	logger       *slog.Logger
}

func newVendorAPI(rawURL, token string, opts VendorOptions) (*vendorAPI, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" {
		return nil, fmt.Errorf("blob vendor URL must be an absolute https URL: %s", redactExternalURL(rawURL))
	}
	loopback := u.Scheme == "http"
	if loopback && !isLoopbackHost(u.Hostname()) {
		// The bearer token would cross the network in plain text.
		return nil, fmt.Errorf("blob vendor URL must use https (http is allowed only for loopback): %s", redactExternalURL(rawURL))
	}
	if token == "" {
		return nil, errors.New("blob vendor token is empty")
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = vendorDefaultTimeout
	}
	attempts := opts.MaxAttempts
	if attempts <= 0 {
		attempts = vendorDefaultAttempts
	}
	backoff := opts.Backoff
	if backoff <= 0 {
		backoff = vendorDefaultBackoff
	}
	client := &http.Client{Timeout: timeout, CheckRedirect: noRedirects}
	if opts.HTTPClient != nil && opts.HTTPClient.Transport != nil {
		// The caller's transport only: no jar, and never its redirect policy.
		client.Transport = opts.HTTPClient.Transport
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &vendorAPI{
		baseURL:      strings.TrimRight(rawURL, "/"),
		token:        token,
		loopbackHTTP: loopback,
		timeout:      timeout,
		attempts:     attempts,
		backoff:      backoff,
		client:       client,
		storage:      newStorageHTTPClient(timeout),
		logger:       logger,
	}, nil
}

func (a *vendorAPI) close() {
	a.client.CloseIdleConnections()
	a.storage.CloseIdleConnections()
}

// send issues one request, retrying transport errors, 429 and 5xx with
// backoff. The request carries exactly headers. The caller closes the body of
// the returned response.
func (a *vendorAPI) send(ctx context.Context, method, rawURL, op string, headers map[string]string, body []byte, storage bool) (*http.Response, error) {
	client := a.client
	if storage {
		client = a.storage
	}
	lastErr := ""
	status := 0
	for attempt := 0; attempt < a.attempts; attempt++ {
		if attempt > 0 {
			delay := min(a.backoff*time.Duration(1<<(attempt-1)), vendorMaxBackoff)
			delay = time.Duration(float64(delay) * (0.5 + rand.Float64()/2))
			select {
			case <-ctx.Done():
				return nil, &VendorError{Message: fmt.Sprintf("blob vendor %s cancelled: %s", op, redactExternalURL(rawURL))}
			case <-time.After(delay):
			}
		}
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, rawURL, reader)
		if err != nil {
			return nil, &VendorError{Message: fmt.Sprintf("blob vendor %s: cannot build request for %s", op, redactExternalURL(rawURL))}
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		if body != nil {
			req.ContentLength = int64(len(body))
		}
		resp, err := client.Do(req)
		if err != nil {
			// Never err.Error(): a *url.Error embeds the full (signed) URL.
			status = 0
			lastErr = "transport error"
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				lastErr = "timeout"
			}
			a.logger.Debug("blob vendor transport error", "op", op, "attempt", attempt+1, "of", a.attempts, "error", lastErr, "url", redactExternalURL(rawURL))
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			status = resp.StatusCode
			lastErr = fmt.Sprintf("HTTP %d", resp.StatusCode)
			a.logger.Debug("blob vendor retryable status", "op", op, "attempt", attempt+1, "of", a.attempts, "status", resp.StatusCode, "url", redactExternalURL(rawURL))
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
			_ = resp.Body.Close()
			continue
		}
		return resp, nil
	}
	return nil, &VendorError{
		Message: fmt.Sprintf("blob vendor %s failed after %d attempts (%s): %s", op, a.attempts, lastErr, redactExternalURL(rawURL)),
		Status:  status,
	}
}

func (a *vendorAPI) call(ctx context.Context, method, path, op string, payload any) (map[string]any, error) {
	rawURL := a.baseURL + path
	headers := map[string]string{"Authorization": "Bearer " + a.token, "Accept": "application/json"}
	var body []byte
	if payload != nil {
		var err error
		body, err = json.Marshal(payload)
		if err != nil {
			return nil, &VendorError{Message: fmt.Sprintf("blob vendor %s: cannot encode request", op)}
		}
		headers["Content-Type"] = "application/json"
	}
	resp, err := a.send(ctx, method, rawURL, op, headers, body, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, &VendorError{Message: fmt.Sprintf("blob vendor %s: reading the response failed", op), Status: resp.StatusCode}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		code, detail := vendorErrorDetail(data)
		return nil, &VendorError{
			Message: fmt.Sprintf("blob vendor %s failed: HTTP %d%s", op, resp.StatusCode, detail),
			Status:  resp.StatusCode,
			Code:    code,
		}
	}
	if len(data) == 0 {
		return map[string]any{}, nil
	}
	var parsed any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&parsed); err != nil {
		return nil, &VendorError{Message: fmt.Sprintf("blob vendor %s returned non-JSON", op), Status: resp.StatusCode}
	}
	obj, ok := parsed.(map[string]any)
	if !ok {
		return nil, &VendorError{Message: fmt.Sprintf("blob vendor %s returned a non-object JSON body", op), Status: resp.StatusCode}
	}
	return obj, nil
}

func vendorErrorDetail(body []byte) (string, string) {
	var parsed map[string]any
	if json.Unmarshal(body, &parsed) != nil {
		return "", ""
	}
	code, _ := parsed["error"].(string)
	if len(code) > 64 {
		code = code[:64]
	}
	message, _ := parsed["message"].(string)
	if len(message) > 200 {
		message = message[:200]
	}
	var parts []string
	for _, p := range []string{code, message} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) == 0 {
		return code, ""
	}
	return code, ": " + strings.Join(parts, ": ")
}

func s3ErrorCode(body []byte) string {
	start := bytes.Index(body, []byte("<Code>"))
	if start < 0 {
		return ""
	}
	end := bytes.Index(body[start:], []byte("</Code>"))
	if end < 0 {
		return ""
	}
	code := string(body[start+6 : start+end])
	if code == "" || len(code) > 64 {
		return ""
	}
	for _, r := range code {
		isAlnum := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
		if !isAlnum {
			return ""
		}
	}
	return code
}

// Typed field readers. Each reports a malformed response as a *VendorError.

func vendorMalformed(op, key string) error {
	return &VendorError{Message: fmt.Sprintf("blob vendor %s response has a malformed %q", op, key)}
}

func vStr(obj map[string]any, key, op string) (string, error) {
	raw, ok := obj[key]
	if !ok {
		return "", &VendorError{Message: fmt.Sprintf("blob vendor %s response is missing %q", op, key)}
	}
	s, ok := raw.(string)
	if !ok || s == "" {
		return "", vendorMalformed(op, key)
	}
	return s, nil
}

func vOptStr(obj map[string]any, key, op string) (string, error) {
	raw, ok := obj[key]
	if !ok || raw == nil {
		return "", nil
	}
	s, ok := raw.(string)
	if !ok {
		return "", vendorMalformed(op, key)
	}
	return s, nil
}

func vInt(obj map[string]any, key, op string) (int64, error) {
	raw, ok := obj[key]
	if !ok {
		return 0, &VendorError{Message: fmt.Sprintf("blob vendor %s response is missing %q", op, key)}
	}
	n, ok := raw.(json.Number)
	if !ok {
		return 0, vendorMalformed(op, key)
	}
	v, err := n.Int64()
	if err != nil || v < 0 {
		return 0, vendorMalformed(op, key)
	}
	return v, nil
}

func vTime(obj map[string]any, key, op string) (time.Time, error) {
	raw, ok := obj[key]
	if !ok {
		return time.Time{}, &VendorError{Message: fmt.Sprintf("blob vendor %s response is missing %q", op, key)}
	}
	if n, isNum := raw.(json.Number); isNum {
		f, err := n.Float64()
		if err != nil {
			return time.Time{}, &VendorError{Message: "blob vendor returned a malformed expires_at"}
		}
		raw = f
	}
	t, err := parseVendorTime(raw)
	if err != nil {
		return time.Time{}, &VendorError{Message: "blob vendor returned a malformed expires_at"}
	}
	return t, nil
}

func vStrList(obj map[string]any, key, op string) ([]string, error) {
	raw, ok := obj[key]
	if !ok || raw == nil {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, vendorMalformed(op, key)
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		s, ok := item.(string)
		if !ok || s == "" {
			return nil, vendorMalformed(op, key)
		}
		out = append(out, s)
	}
	return out, nil
}

func vIntMap(obj map[string]any, key string) map[string]int64 {
	out := map[string]int64{}
	m, ok := obj[key].(map[string]any)
	if !ok {
		return out
	}
	for k, v := range m {
		if n, ok := v.(json.Number); ok {
			if i, err := n.Int64(); err == nil {
				out[k] = i
			}
		}
	}
	return out
}

func vDeleted(obj map[string]any) int {
	if n, ok := obj["deleted"].(json.Number); ok {
		if i, err := n.Int64(); err == nil {
			return int(i)
		}
	}
	return 0
}

type vendorPutRequest struct {
	Size            int64
	SHA256B64       string
	Purpose         BlobPurpose
	Scope           *BlobScope
	ContentEncoding string
	IdempotencyKey  string
	// Origin is the advisory grant origin ([ClientUploadOrigin]); empty sends none.
	Origin string
}

func (a *vendorAPI) put(ctx context.Context, req vendorPutRequest) (*PutGrant, error) {
	body := map[string]any{"size": req.Size, "sha256_b64": req.SHA256B64, "purpose": string(req.Purpose)}
	if req.Scope != nil {
		body["scope"] = map[string]string{"shard": req.Scope.Shard, "execution_id": req.Scope.ExecutionID}
	}
	if req.ContentEncoding != "" {
		body["content_encoding"] = req.ContentEncoding
	}
	if req.IdempotencyKey != "" {
		body["idempotency_key"] = req.IdempotencyKey
	}
	if req.Origin != "" {
		body["origin"] = req.Origin
	}
	resp, err := a.call(ctx, http.MethodPost, vendorPutPath, "put", body)
	if err != nil {
		return nil, err
	}
	if method, ok := resp["upload_method"]; ok && method != "PUT" {
		return nil, &VendorError{Message: fmt.Sprintf("blob vendor put asked for unsupported upload_method %v", method)}
	}
	rawHeaders, ok := resp["required_headers"].(map[string]any)
	if !ok {
		return nil, &VendorError{Message: "blob vendor put response has malformed 'required_headers'"}
	}
	headers := make(map[string]string, len(rawHeaders))
	for k, v := range rawHeaders {
		s, ok := v.(string)
		if !ok {
			return nil, &VendorError{Message: "blob vendor put response has malformed 'required_headers'"}
		}
		headers[k] = s
	}
	grant := &PutGrant{RequiredHeaders: headers}
	if grant.ObjectID, err = vStr(resp, "object_id", "put"); err != nil {
		return nil, err
	}
	if grant.UploadURL, err = vStr(resp, "upload_url", "put"); err != nil {
		return nil, err
	}
	if grant.DownloadURL, err = vOptStr(resp, "download_url", "put"); err != nil {
		return nil, err
	}
	if grant.ExpiresAt, err = vTime(resp, "expires_at", "put"); err != nil {
		return nil, err
	}
	if grant.Region, err = vOptStr(resp, "region", "put"); err != nil {
		return nil, err
	}
	if v, ok := resp["download_expires_at"]; ok && v != nil {
		if grant.DownloadExpiresAt, err = vTime(resp, "download_expires_at", "put"); err != nil {
			return nil, err
		}
	}
	return grant, nil
}

func (a *vendorAPI) get(ctx context.Context, objectID string) (*BlobLocation, error) {
	resp, err := a.call(ctx, http.MethodPost, vendorGetPath, "get", map[string]string{"object_id": objectID})
	if err != nil {
		return nil, err
	}
	loc := &BlobLocation{}
	if loc.DownloadURL, err = vStr(resp, "download_url", "get"); err != nil {
		return nil, err
	}
	if loc.Size, err = vInt(resp, "size", "get"); err != nil {
		return nil, err
	}
	if loc.SHA256B64, err = vStr(resp, "sha256_b64", "get"); err != nil {
		return nil, err
	}
	if loc.ContentEncoding, err = vOptStr(resp, "content_encoding", "get"); err != nil {
		return nil, err
	}
	if loc.ExpiresAt, err = vTime(resp, "expires_at", "get"); err != nil {
		return nil, err
	}
	return loc, nil
}

func (a *vendorAPI) delete(ctx context.Context, ids []string) (int, error) {
	resp, err := a.call(ctx, http.MethodPost, vendorDeletePath, "delete", map[string]any{"object_ids": ids})
	if err != nil {
		return 0, err
	}
	return vDeleted(resp), nil
}

func (a *vendorAPI) deleteScope(ctx context.Context, scope BlobScope) (int, error) {
	resp, err := a.call(ctx, http.MethodPost, vendorDeleteScopePath, "delete_scope",
		map[string]string{"shard": scope.Shard, "execution_id": scope.ExecutionID})
	if err != nil {
		return 0, err
	}
	return vDeleted(resp), nil
}

func (a *vendorAPI) info(ctx context.Context) (*VendorInfo, error) {
	resp, err := a.call(ctx, http.MethodGet, vendorInfoPath, "info", nil)
	if err != nil {
		return nil, err
	}
	info := &VendorInfo{Limits: vIntMap(resp, "limits"), TTLSeconds: vIntMap(resp, "ttl_seconds")}
	info.Region, _ = resp["region"].(string)
	info.Tenant, _ = resp["tenant"].(string)
	if info.Jurisdiction, err = vOptStr(resp, "jurisdiction", "info"); err != nil {
		return nil, err
	}
	if info.UploadHosts, err = vStrList(resp, "upload_hosts", "info"); err != nil {
		return nil, err
	}
	if info.DownloadHosts, err = vStrList(resp, "download_hosts", "info"); err != nil {
		return nil, err
	}
	if info.URLPrefixes, err = vStrList(resp, "url_prefixes", "info"); err != nil {
		return nil, err
	}
	return info, nil
}

// putObject PUTs data to a granted URL with exactly its required headers, on
// the bare storage client.
func (a *vendorAPI) putObject(ctx context.Context, grant *PutGrant, data []byte, contentEncoding string) error {
	headers := make(map[string]string, len(grant.RequiredHeaders)+1)
	hasEncoding := false
	for k, v := range grant.RequiredHeaders {
		switch strings.ToLower(k) {
		case "content-length":
			if v != strconv.Itoa(len(data)) {
				return &VendorError{Message: fmt.Sprintf("blob vendor granted Content-Length %s for a %d-byte object", v, len(data))}
			}
			// net/http sets Content-Length from the body itself.
			continue
		case "content-encoding":
			hasEncoding = true
		}
		headers[k] = v
	}
	if contentEncoding != "" && !hasEncoding {
		headers["Content-Encoding"] = contentEncoding
	}
	resp, err := a.send(ctx, http.MethodPut, grant.UploadURL, "upload", headers, data, true)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		code := s3ErrorCode(body)
		suffix := ""
		if code != "" {
			suffix = " " + code
		}
		return &VendorError{
			Message: fmt.Sprintf("signed upload failed: HTTP %d%s: %s", resp.StatusCode, suffix, redactExternalURL(grant.UploadURL)),
			Status:  resp.StatusCode,
			Code:    code,
		}
	}
	return nil
}

// download GETs a signed URL on the bare storage client, pinned and capped.
func (a *vendorAPI) download(ctx context.Context, rawURL string, validate func(string) error, maxBytes int64) ([]byte, error) {
	if err := validate(rawURL); err != nil {
		return nil, &VendorError{Message: fmt.Sprintf("download rejected: %v", err)}
	}
	resp, err := a.send(ctx, http.MethodGet, rawURL, "download", nil, nil, true)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		return nil, &VendorError{
			Message: fmt.Sprintf("download failed: HTTP %d: %s", resp.StatusCode, redactExternalURL(rawURL)),
			Status:  resp.StatusCode,
			Code:    s3ErrorCode(body),
		}
	}
	if resp.ContentLength > maxBytes {
		return nil, &VendorError{Message: fmt.Sprintf("download of %d bytes exceeds the %d-byte cap", resp.ContentLength, maxBytes)}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, &VendorError{Message: fmt.Sprintf("download failed while reading: %s", redactExternalURL(rawURL))}
	}
	if int64(len(data)) > maxBytes {
		return nil, &VendorError{Message: fmt.Sprintf("download exceeds the %d-byte cap", maxBytes)}
	}
	return data, nil
}

// ---------------------------------------------------------------------------
// Shared base
// ---------------------------------------------------------------------------

type vendorClientBase struct {
	api       *vendorAPI
	validator *vendorHostsValidator
}

func newVendorClientBase(rawURL, token string, opts VendorOptions) (vendorClientBase, error) {
	api, err := newVendorAPI(rawURL, token, opts)
	if err != nil {
		return vendorClientBase{}, err
	}
	ttl := opts.InfoTTL
	if ttl <= 0 {
		ttl = vendorDefaultInfoTTL
	}
	return vendorClientBase{api: api, validator: &vendorHostsValidator{api: api, ttl: ttl}}, nil
}

// URLValidator is a validator pinned to the vendor's storage hosts
// (/v1/info, cached), for ExternalLocationConfig.URLValidator.
func (b vendorClientBase) URLValidator() func(string) error { return b.validator.validate }

// Info fetches the vendor's region and storage hosts (uncached).
func (b vendorClientBase) Info(ctx context.Context) (*VendorInfo, error) { return b.api.info(ctx) }

// Close releases idle connections.
func (b vendorClientBase) Close() { b.api.close() }

func (b vendorClientBase) check(raw string) error {
	if err := b.validator.validate(raw); err != nil {
		return &VendorError{Message: fmt.Sprintf("blob vendor returned a URL off its pinned hosts: %v", err)}
	}
	return nil
}

func (b vendorClientBase) requestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Duration(b.api.attempts)*(b.api.timeout+vendorMaxBackoff))
}

func vendorEnvURLAndToken(getenv func(string) string) (string, string, error) {
	rawURL := strings.TrimSpace(getenv(EnvBlobVendorURL))
	if rawURL == "" {
		return "", "", fmt.Errorf("%s is not set", EnvBlobVendorURL)
	}
	token := strings.TrimSpace(getenv(EnvBlobVendorToken))
	if token == "" {
		token = strings.TrimSpace(getenv(EnvBlobVendorTokenFallback))
	}
	if token == "" {
		return "", "", fmt.Errorf("%s is set but neither %s nor %s is", EnvBlobVendorURL, EnvBlobVendorToken, EnvBlobVendorTokenFallback)
	}
	return rawURL, token, nil
}

func envLookup(getenv func(string) string) func(string) string {
	if getenv == nil {
		return os.Getenv
	}
	return getenv
}

// ---------------------------------------------------------------------------
// VendorStorage
// ---------------------------------------------------------------------------

// VendorStorage is an [ExternalStorage] and [UploadURLProvider] backed by the
// blob vendor.
//
// Upload (server-side externalization) asks the vendor for a sized,
// checksummed grant, PUTs the bytes with exactly the grant's required headers,
// and returns the grant's download URL. GenerateUploadURL (client uploads)
// asks the same sized endpoint (purpose rpc) for a PUT signed for the
// client's exact size and SHA-256, so storage refuses any other body. Vended
// URLs are checked against the vendor's pinned hosts before use.
//
// Safe for concurrent use. Objects are cleaned up by the vendor; vgi-rpc never
// deletes them.
type VendorStorage struct {
	vendorClientBase
	purpose BlobPurpose
	pin     bool
}

// NewVendorStorage creates a vendor-backed storage for purpose (default
// [BlobPurposeRPC]). rawURL must be https (http only for loopback).
func NewVendorStorage(rawURL, token string, purpose BlobPurpose, opts VendorOptions) (*VendorStorage, error) {
	base, err := newVendorClientBase(rawURL, token, opts)
	if err != nil {
		return nil, err
	}
	if purpose == "" {
		purpose = BlobPurposeRPC
	}
	return &VendorStorage{vendorClientBase: base, purpose: purpose, pin: true}, nil
}

// VendorStorageFromEnv builds a [VendorStorage] from VGI_BLOB_VENDOR_URL and
// the vendor token variables. getenv defaults to os.Getenv.
func VendorStorageFromEnv(purpose BlobPurpose, getenv func(string) string) (*VendorStorage, error) {
	rawURL, token, err := vendorEnvURLAndToken(envLookup(getenv))
	if err != nil {
		return nil, err
	}
	return NewVendorStorage(rawURL, token, purpose, VendorOptions{})
}

// Upload vendors data (already compressed when contentEncoding is set) and
// returns its pre-signed download URL. Implements [ExternalStorage].
func (v *VendorStorage) Upload(data []byte, _ *arrow.Schema, contentEncoding string) (string, error) {
	ctx, cancel := v.requestContext()
	defer cancel()
	grant, err := v.api.put(ctx, vendorPutRequest{
		Size: int64(len(data)), SHA256B64: b64SHA256(data), Purpose: v.purpose, ContentEncoding: contentEncoding,
	})
	if err != nil {
		return "", err
	}
	if v.pin {
		if err := v.check(grant.UploadURL); err != nil {
			return "", err
		}
	}
	if err := v.api.putObject(ctx, grant, data, contentEncoding); err != nil {
		return "", err
	}
	downloadURL := grant.DownloadURL
	if downloadURL == "" {
		loc, err := v.api.get(ctx, grant.ObjectID)
		if err != nil {
			return "", err
		}
		downloadURL = loc.DownloadURL
	}
	if v.pin {
		if err := v.check(downloadURL); err != nil {
			return "", err
		}
	}
	v.api.logger.Debug("vendored external payload", "bytes", len(data), "url", redactExternalURL(downloadURL))
	return downloadURL, nil
}

// GenerateUploadURL asks the vendor for one sized PUT/GET pair (POST
// /v1/blobs/put, purpose rpc) for a body of exactly size bytes hashing to
// sha256Hex. The grant's required headers (x-amz-checksum-sha256 and anything
// else the signature covers, minus Content-Length) come back as the
// [UploadURL.Headers] the client must send. The grant request carries the
// advisory origin [ClientUploadOrigin], which the vendor ignores and test
// fakes use to tell client uploads from the server's own writes. Implements
// [UploadURLProvider].
func (v *VendorStorage) GenerateUploadURL(size int64, sha256Hex string) (UploadURL, error) {
	digest, err := hex.DecodeString(sha256Hex)
	if err != nil || len(digest) != sha256.Size {
		return UploadURL{}, fmt.Errorf("upload sha256 must be 64 hex characters")
	}
	ctx, cancel := v.requestContext()
	defer cancel()
	grant, err := v.api.put(ctx, vendorPutRequest{
		Size: size, SHA256B64: base64.StdEncoding.EncodeToString(digest), Purpose: BlobPurposeRPC,
		Origin: ClientUploadOrigin,
	})
	if err != nil {
		return UploadURL{}, err
	}
	downloadURL := grant.DownloadURL
	if downloadURL == "" {
		return UploadURL{}, &VendorError{Message: "blob vendor put response for an rpc object has no 'download_url'"}
	}
	if v.pin {
		if err := v.check(grant.UploadURL); err != nil {
			return UploadURL{}, err
		}
		if err := v.check(downloadURL); err != nil {
			return UploadURL{}, err
		}
	}
	headers := make(map[string]string, len(grant.RequiredHeaders))
	for name, value := range grant.RequiredHeaders {
		if strings.EqualFold(name, "Content-Length") {
			if value != strconv.FormatInt(size, 10) {
				return UploadURL{}, &VendorError{Message: fmt.Sprintf("blob vendor granted Content-Length %s for a %d-byte upload", value, size)}
			}
			continue
		}
		headers[name] = value
	}
	expires := grant.ExpiresAt
	if !grant.DownloadExpiresAt.IsZero() && grant.DownloadExpiresAt.Before(expires) {
		expires = grant.DownloadExpiresAt
	}
	return UploadURL{UploadURL: grant.UploadURL, DownloadURL: downloadURL, ExpiresAt: expires, Headers: headers,
		ObjectID: grant.ObjectID}, nil
}

// PutObject stores data as one rpc object and returns its object_id
// (implements [ObjectStore]). A sized, checksummed grant (/v1/blobs/put,
// purpose rpc, advisory origin [StateRefOrigin]) and a PUT with exactly its
// required headers, so storage verifies the body. Objects carry no TTL; the
// bucket lifecycle removes them.
func (v *VendorStorage) PutObject(data []byte) (string, error) {
	ctx, cancel := v.requestContext()
	defer cancel()
	grant, err := v.api.put(ctx, vendorPutRequest{
		Size: int64(len(data)), SHA256B64: b64SHA256(data), Purpose: BlobPurposeRPC, Origin: StateRefOrigin,
	})
	if err != nil {
		return "", err
	}
	if v.pin {
		if err := v.check(grant.UploadURL); err != nil {
			return "", err
		}
	}
	if err := v.api.putObject(ctx, grant, data, ""); err != nil {
		return "", err
	}
	return grant.ObjectID, nil
}

// Get fetches an object by id (implements [ObjectStore]): the download is
// minted server-side (/v1/blobs/get), pinned to the vendor's download hosts
// and capped at maxBytes (<= 0: 256 MiB). The vendor's not_found /
// not_uploaded and a storage 404 are [ErrObjectMissing]. The caller verifies
// size and digest.
func (v *VendorStorage) Get(objectID string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = defaultMaxFetchBytes
	}
	ctx, cancel := v.requestContext()
	defer cancel()
	loc, err := v.api.get(ctx, objectID)
	if err != nil {
		var ve *VendorError
		if errors.As(err, &ve) && (ve.Code == "not_found" || ve.Code == "not_uploaded" || ve.Status == http.StatusNotFound) {
			return nil, fmt.Errorf("blob vendor object is gone (%s): %w", vendorMissingDetail(ve), ErrObjectMissing)
		}
		return nil, err
	}
	if loc.Size > maxBytes {
		return nil, &VendorError{Message: fmt.Sprintf("blob vendor object is %d bytes, over max_fetch_bytes (%d)", loc.Size, maxBytes)}
	}
	if loc.ContentEncoding != "" && loc.ContentEncoding != "identity" {
		return nil, &VendorError{Message: fmt.Sprintf("blob vendor object has unsupported content_encoding %q", loc.ContentEncoding)}
	}
	validate := func(string) error { return nil }
	if v.pin {
		validate = v.validator.validate
	}
	data, err := v.api.download(ctx, loc.DownloadURL, validate, maxBytes)
	if err != nil {
		var ve *VendorError
		if errors.As(err, &ve) && ve.Status == http.StatusNotFound {
			return nil, fmt.Errorf("blob vendor object is gone (storage 404): %w", ErrObjectMissing)
		}
		return nil, err
	}
	return data, nil
}

func vendorMissingDetail(ve *VendorError) string {
	if ve.Code != "" {
		return ve.Code
	}
	return strconv.Itoa(ve.Status)
}

// ---------------------------------------------------------------------------
// VendorBlobClient
// ---------------------------------------------------------------------------

// VendorBlobClient stores raw byte blobs through the vendor, referenced by
// [BlobRef], for callers that store the references themselves (a key-value
// store that externalizes large values). Objects use purpose
// [BlobPurposeFS] by default: no download URL at put; GetBytes asks the vendor
// for a fresh one. Safe for concurrent use.
type VendorBlobClient struct {
	vendorClientBase
	purpose  BlobPurpose
	maxFetch int64
}

// NewVendorBlobClient creates a blob client for purpose (default
// [BlobPurposeFS]).
func NewVendorBlobClient(rawURL, token string, purpose BlobPurpose, opts VendorOptions) (*VendorBlobClient, error) {
	base, err := newVendorClientBase(rawURL, token, opts)
	if err != nil {
		return nil, err
	}
	if purpose == "" {
		purpose = BlobPurposeFS
	}
	maxFetch := opts.MaxFetchBytes
	if maxFetch <= 0 {
		maxFetch = vendorDefaultMaxFetch
	}
	return &VendorBlobClient{vendorClientBase: base, purpose: purpose, maxFetch: maxFetch}, nil
}

// VendorBlobClientFromEnv builds a [VendorBlobClient] from
// VGI_BLOB_VENDOR_URL and the vendor token variables. getenv defaults to
// os.Getenv.
func VendorBlobClientFromEnv(purpose BlobPurpose, getenv func(string) string) (*VendorBlobClient, error) {
	rawURL, token, err := vendorEnvURLAndToken(envLookup(getenv))
	if err != nil {
		return nil, err
	}
	return NewVendorBlobClient(rawURL, token, purpose, VendorOptions{})
}

// PutBytes stores data and returns its reference.
//
// scope (optional) names the execution the blob belongs to, for DeleteScope.
// idempotencyKey (optional) makes a retried logical write reuse the same
// object: the same key with the same bytes returns the same object id; with
// different bytes the vendor refuses (Code "idempotency_conflict").
func (c *VendorBlobClient) PutBytes(ctx context.Context, data []byte, scope *BlobScope, idempotencyKey string) (BlobRef, error) {
	sum := sha256.Sum256(data)
	grant, err := c.api.put(ctx, vendorPutRequest{
		Size:           int64(len(data)),
		SHA256B64:      base64.StdEncoding.EncodeToString(sum[:]),
		Purpose:        c.purpose,
		Scope:          scope,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return BlobRef{}, err
	}
	if err := c.check(grant.UploadURL); err != nil {
		return BlobRef{}, err
	}
	if err := c.api.putObject(ctx, grant, data, ""); err != nil {
		return BlobRef{}, err
	}
	return BlobRef{ObjectID: grant.ObjectID, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}, nil
}

// GetBytes fetches and verifies the blob ref names.
//
// The download is pinned to the vendor's storage hosts, capped at
// MaxFetchBytes, and checked against the size and SHA-256 in ref (not the
// vendor's own report): a mismatch is a *VendorError with Code
// [VendorCodeSizeMismatch] or [VendorCodeSHA256Mismatch].
func (c *VendorBlobClient) GetBytes(ctx context.Context, ref BlobRef) ([]byte, error) {
	if ref.Size > c.maxFetch {
		return nil, &VendorError{Message: fmt.Sprintf("blob %s is %d bytes, over the %d-byte fetch cap", ref.ObjectID, ref.Size, c.maxFetch)}
	}
	loc, err := c.api.get(ctx, ref.ObjectID)
	if err != nil {
		return nil, err
	}
	if loc.ContentEncoding != "" && loc.ContentEncoding != "identity" {
		return nil, &VendorError{Message: fmt.Sprintf("blob %s has unsupported content_encoding %q", ref.ObjectID, loc.ContentEncoding)}
	}
	if loc.Size != ref.Size {
		return nil, &VendorError{
			Message: fmt.Sprintf("blob %s size mismatch: vendor reports %d, ref has %d", ref.ObjectID, loc.Size, ref.Size),
			Code:    VendorCodeSizeMismatch,
		}
	}
	data, err := c.api.download(ctx, loc.DownloadURL, c.validator.validate, c.maxFetch)
	if err != nil {
		var ve *VendorError
		if errors.As(err, &ve) {
			return nil, &VendorError{Message: fmt.Sprintf("blob %s %s", ref.ObjectID, ve.Message), Status: ve.Status, Code: ve.Code}
		}
		return nil, err
	}
	if int64(len(data)) != ref.Size {
		return nil, &VendorError{
			Message: fmt.Sprintf("blob %s size mismatch: expected %d, got %d", ref.ObjectID, ref.Size, len(data)),
			Code:    VendorCodeSizeMismatch,
		}
	}
	sum := sha256.Sum256(data)
	if actual := hex.EncodeToString(sum[:]); actual != strings.ToLower(ref.SHA256) {
		return nil, &VendorError{
			Message: fmt.Sprintf("blob %s SHA-256 mismatch: expected %s, got %s", ref.ObjectID, ref.SHA256, actual),
			Code:    VendorCodeSHA256Mismatch,
		}
	}
	return data, nil
}

// Delete deletes objects by id (missing objects are ignored) and returns how
// many the vendor deleted.
func (c *VendorBlobClient) Delete(ctx context.Context, objectIDs []string) (int, error) {
	total := 0
	for start := 0; start < len(objectIDs); start += vendorMaxDeleteIDs {
		end := min(start+vendorMaxDeleteIDs, len(objectIDs))
		n, err := c.api.delete(ctx, objectIDs[start:end])
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// DeleteScope deletes every object put under (shard, executionID).
func (c *VendorBlobClient) DeleteScope(ctx context.Context, shard, executionID string) (int, error) {
	return c.api.deleteScope(ctx, BlobScope{Shard: shard, ExecutionID: executionID})
}

// ---------------------------------------------------------------------------
// Env-driven configs
// ---------------------------------------------------------------------------

// VendorURLValidator builds a URL validator pinned to the vendor's storage
// hosts. The hosts come from the vendor's GET /v1/info, fetched on first use
// and cached for ttl (default 10m); a failed refresh keeps the last good list.
// Only https URLs pass (http only when the vendor itself is a loopback http
// URL, i.e. a local test vendor).
func VendorURLValidator(rawURL, token string, ttl time.Duration) (func(string) error, error) {
	base, err := newVendorClientBase(rawURL, token, VendorOptions{InfoTTL: ttl})
	if err != nil {
		return nil, err
	}
	return base.URLValidator(), nil
}

func positiveIntEnv(getenv func(string) string, name string) (int64, error) {
	raw := strings.TrimSpace(getenv(name))
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer, got %q", name, raw)
	}
	if v <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %d", name, v)
	}
	return v, nil
}

func parseExternalCompression(raw string) (*Compression, error) {
	text := strings.ToLower(strings.TrimSpace(raw))
	switch text {
	case "", "none", "off", "identity":
		return nil, nil
	}
	name, levelText, _ := strings.Cut(text, ":")
	if name != "zstd" {
		// The Python reference also accepts gzip; vgi-rpc-go externalizes
		// with zstd only.
		return nil, fmt.Errorf("%s must be none or zstd (optionally ':<level>'), got %q", EnvExternalCompression, raw)
	}
	if levelText == "" {
		return &Compression{Algorithm: "zstd"}, nil
	}
	level, err := strconv.Atoi(levelText)
	if err != nil {
		return nil, fmt.Errorf("%s level must be an integer, got %q", EnvExternalCompression, raw)
	}
	return &Compression{Algorithm: "zstd", Level: level}, nil
}

// ExternalConfigFromEnv is the server external-location config from the
// blob-vendor environment, or nil when VGI_BLOB_VENDOR_URL is unset.
//
// It uploads through a [VendorStorage] (config.Storage; pass it to
// [HttpServer.SetUploadURLProvider] to also vend client upload URLs), pins
// inbound pointer URLs to the vendor's hosts, and reads
// VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES (default 1 MiB) and
// VGI_RPC_EXTERNAL_COMPRESSION (default none). getenv defaults to os.Getenv.
func ExternalConfigFromEnv(getenv func(string) string) (*ExternalLocationConfig, error) {
	getenv = envLookup(getenv)
	if strings.TrimSpace(getenv(EnvBlobVendorURL)) == "" {
		return nil, nil
	}
	storage, err := VendorStorageFromEnv(BlobPurposeRPC, getenv)
	if err != nil {
		return nil, err
	}
	threshold, err := positiveIntEnv(getenv, EnvExternalizeThreshold)
	if err != nil {
		return nil, err
	}
	if threshold == 0 {
		threshold = vendorDefaultThreshold
	}
	compression, err := parseExternalCompression(getenv(EnvExternalCompression))
	if err != nil {
		return nil, err
	}
	maxFetch, err := MaxFetchBytesFromEnv(getenv)
	if err != nil {
		return nil, err
	}
	config := DefaultExternalLocationConfig(storage)
	config.ExternalizeThresholdBytes = threshold
	config.Compression = compression
	config.MaxFetchBytes = maxFetch
	config.fetchCapExplicit = strings.TrimSpace(getenv(EnvMaxFetchBytes)) != ""
	config.URLValidator = storage.URLValidator()
	// Fetching an inbound pointer is a storage-host request too: a bare
	// client, never one carrying credentials.
	config.HTTPClient = &http.Client{Timeout: 5 * time.Minute}
	return config, nil
}

// ClientExternalConfigFromEnv is a client external-location config whose
// pointer and upload URLs are pinned to the blob vendor's storage hosts, for
// clients that hold a vendor token (a worker calling another worker); nil
// when VGI_BLOB_VENDOR_URL is unset. getenv defaults to os.Getenv.
func ClientExternalConfigFromEnv(getenv func(string) string) (*ExternalLocationConfig, error) {
	getenv = envLookup(getenv)
	if strings.TrimSpace(getenv(EnvBlobVendorURL)) == "" {
		return nil, nil
	}
	rawURL, token, err := vendorEnvURLAndToken(getenv)
	if err != nil {
		return nil, err
	}
	validator, err := VendorURLValidator(rawURL, token, 0)
	if err != nil {
		return nil, err
	}
	config := DefaultExternalLocationConfig(nil)
	config.URLValidator = validator
	return config, nil
}

// MaxUploadBytesFromEnv reads VGI_RPC_MAX_UPLOAD_BYTES (the advertised
// client-upload size); 0 when unset. getenv defaults to os.Getenv.
func MaxUploadBytesFromEnv(getenv func(string) string) (int64, error) {
	return positiveIntEnv(envLookup(getenv), EnvMaxUploadBytes)
}

// MaxFetchBytesFromEnv reads VGI_RPC_MAX_FETCH_BYTES, the inbound fetch cap.
// Unset, it is 256 MiB raised to VGI_RPC_MAX_UPLOAD_BYTES when that is larger:
// setting the upload size is how an operator says "accept requests this
// large". Both set with the upload size above the fetch cap is an error, since
// it is an explicit contradiction (a client would upload requests the server
// then refuses to fetch). getenv defaults to os.Getenv.
func MaxFetchBytesFromEnv(getenv func(string) string) (int64, error) {
	getenv = envLookup(getenv)
	fetch, err := positiveIntEnv(getenv, EnvMaxFetchBytes)
	if err != nil {
		return 0, err
	}
	upload, err := positiveIntEnv(getenv, EnvMaxUploadBytes)
	if err != nil {
		return 0, err
	}
	if fetch == 0 {
		return max(defaultMaxFetchBytes, upload), nil
	}
	if upload > fetch {
		return 0, fmt.Errorf("%s (%d) exceeds %s (%d): clients would upload requests the server then refuses "+
			"to fetch. Raise %s or lower %s", EnvMaxUploadBytes, upload, EnvMaxFetchBytes, fetch,
			EnvMaxFetchBytes, EnvMaxUploadBytes)
	}
	return fetch, nil
}

// MaxRequestBytesFromEnv reads VGI_RPC_MAX_REQUEST_BYTES (the inline request
// cap a server advertises and enforces); 0 when unset. getenv defaults to
// os.Getenv.
func MaxRequestBytesFromEnv(getenv func(string) string) (int64, error) {
	return positiveIntEnv(envLookup(getenv), EnvMaxRequestBytes)
}

// ConfigureHTTPServerFromEnv applies the blob-vendor HTTP settings to h:
// VGI_RPC_MAX_REQUEST_BYTES is advertised and enforced whenever it is set, and
// VGI_RPC_STATE_REF_INLINE_BYTES sets the StateRef inline limit; with
// VGI_BLOB_VENDOR_URL, large results are externalized through the vendor
// ([ExternalConfigFromEnv]), client upload URLs are vended from it, and
// VGI-Max-Upload-Bytes is advertised: VGI_RPC_MAX_UPLOAD_BYTES, else the
// inbound fetch cap (VGI_RPC_MAX_FETCH_BYTES; see [MaxFetchBytesFromEnv]).
// Returns the external config (nil without a vendor). Any malformed or
// contradictory value is an error. getenv defaults to os.Getenv.
func ConfigureHTTPServerFromEnv(h *HttpServer, getenv func(string) string) (*ExternalLocationConfig, error) {
	getenv = envLookup(getenv)
	maxRequest, err := MaxRequestBytesFromEnv(getenv)
	if err != nil {
		return nil, err
	}
	inline, err := StateRefInlineBytesFromEnv(getenv)
	if err != nil {
		return nil, err
	}
	config, err := ExternalConfigFromEnv(getenv)
	if err != nil {
		return nil, err
	}
	if err := h.SetStateRefInlineBytes(inline); err != nil {
		return nil, err
	}
	if maxRequest > 0 {
		h.SetMaxRequestBytes(maxRequest)
	}
	if config == nil {
		return nil, nil
	}
	maxUpload, err := MaxUploadBytesFromEnv(getenv)
	if err != nil {
		return nil, err
	}
	if maxUpload > config.maxFetchBytes() {
		// Unreachable through ExternalConfigFromEnv (MaxFetchBytesFromEnv
		// refuses it); kept so the invariant cannot rot.
		return nil, fmt.Errorf("%s (%d) exceeds the inbound fetch cap (%d)", EnvMaxUploadBytes, maxUpload, config.maxFetchBytes())
	}
	h.server.SetExternalLocation(config)
	if provider, ok := config.Storage.(UploadURLProvider); ok {
		h.SetUploadURLProvider(provider)
	}
	// 0 (unset) advertises the fetch cap itself.
	h.SetMaxUploadBytes(maxUpload)
	return config, nil
}

// Compile-time checks: VendorStorage is every storage role protocol 3.4 asks of a backend.
var (
	_ ExternalStorage   = (*VendorStorage)(nil)
	_ UploadURLProvider = (*VendorStorage)(nil)
	_ ObjectStore       = (*VendorStorage)(nil)
)
