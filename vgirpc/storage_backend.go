// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"fmt"
	"net/url"
	"strings"
)

// Direct object storage for self-hosted workers (direct-storage contract v1):
// a worker may keep large values in the operator's own bucket (S3, an
// S3-compatible service, or GCS) instead of the blob vendor. This core module
// only selects the backend and wires a storage the caller built into an
// [HttpServer]; the S3 and GCS backends live in the vgirpc/s3 and vgirpc/gcs
// modules (each reads its own VGI_STORAGE_S3_* / VGI_STORAGE_GCS_* settings).
const (
	// EnvStorageBackend selects the storage backend: "vendor", "s3", "gcs"
	// or "none". Unset, it is inferred (see [StorageBackendFromEnv]).
	EnvStorageBackend = "VGI_STORAGE_BACKEND"
	// EnvStorageS3Bucket is the S3 bucket; setting it selects "s3" when
	// EnvStorageBackend is unset.
	EnvStorageS3Bucket = "VGI_STORAGE_S3_BUCKET"
	// EnvStorageGCSBucket is the GCS bucket; setting it selects "gcs" when
	// EnvStorageBackend is unset.
	EnvStorageGCSBucket = "VGI_STORAGE_GCS_BUCKET"
	// EnvStorageAllowPlainHTTP=1 lets the worker fetch inbound pointers from
	// plain http:// URLs on its configured storage hosts (an in-network MinIO
	// or Ceph endpoint). It mirrors the extension's vgi_storage_allow_plain_http
	// setting. A pre-signed URL carries its credential in its query string,
	// which then crosses the network in the clear.
	EnvStorageAllowPlainHTTP = "VGI_STORAGE_ALLOW_PLAIN_HTTP"
)

// Storage backend names ([EnvStorageBackend] values).
const (
	StorageBackendVendor = "vendor"
	StorageBackendS3     = "s3"
	StorageBackendGCS    = "gcs"
	StorageBackendNone   = "none"
)

// StorageBackendFromEnv resolves VGI_STORAGE_BACKEND. Explicit values are
// "vendor", "s3", "gcs" and "none" (case-insensitive). Unset, the backend is
// "vendor" when VGI_BLOB_VENDOR_URL is set, else "s3" when
// VGI_STORAGE_S3_BUCKET is set, else "gcs" when VGI_STORAGE_GCS_BUCKET is set,
// else "none" (every value stays inline). Setting more than one of those three
// without an explicit backend is an error naming them. getenv defaults to
// os.Getenv.
func StorageBackendFromEnv(getenv func(string) string) (string, error) {
	getenv = envLookup(getenv)
	if raw := strings.ToLower(strings.TrimSpace(getenv(EnvStorageBackend))); raw != "" {
		switch raw {
		case StorageBackendVendor, StorageBackendS3, StorageBackendGCS, StorageBackendNone:
			return raw, nil
		}
		return "", fmt.Errorf("%s=%q: expected vendor, s3, gcs or none", EnvStorageBackend, getenv(EnvStorageBackend))
	}
	var set []string
	for _, name := range []string{EnvBlobVendorURL, EnvStorageS3Bucket, EnvStorageGCSBucket} {
		if strings.TrimSpace(getenv(name)) != "" {
			set = append(set, name)
		}
	}
	if len(set) > 1 {
		return "", fmt.Errorf("%s are all set: set %s to vendor, s3 or gcs to choose one",
			strings.Join(set, " and "), EnvStorageBackend)
	}
	if len(set) == 0 {
		return StorageBackendNone, nil
	}
	switch set[0] {
	case EnvBlobVendorURL:
		return StorageBackendVendor, nil
	case EnvStorageS3Bucket:
		return StorageBackendS3, nil
	default:
		return StorageBackendGCS, nil
	}
}

// StorageAllowPlainHTTPFromEnv reports VGI_STORAGE_ALLOW_PLAIN_HTTP (1, true,
// yes or on). getenv defaults to os.Getenv.
func StorageAllowPlainHTTPFromEnv(getenv func(string) string) bool {
	switch strings.ToLower(strings.TrimSpace(envLookup(getenv)(EnvStorageAllowPlainHTTP))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// StorageEndpointValidator returns a URL validator that accepts only URLs on
// the scheme, host and port of one of endpoints (absolute URLs such as
// "https://s3.example.com" or "http://minio:9000"; a path is ignored).
// https:// endpoints are always allowed; an http:// endpoint only with
// allowPlainHTTP, and only on that endpoint's host and port. Userinfo is
// refused. Hosts compare case-insensitively and a missing port means the
// scheme's default.
func StorageEndpointValidator(endpoints []string, allowPlainHTTP bool) (func(string) error, error) {
	type key struct{ scheme, host string }
	allowed := map[key]struct{}{}
	norm := func(u *url.URL) (key, error) {
		port, err := parsePort(u)
		if err != nil {
			return key{}, err
		}
		if port == 0 {
			port = defaultPortFor(u.Scheme)
		}
		return key{u.Scheme, fmt.Sprintf("%s:%d", strings.ToLower(u.Hostname()), port)}, nil
	}
	for _, raw := range endpoints {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") {
			return nil, fmt.Errorf("invalid storage endpoint %q: want http(s)://host[:port]", raw)
		}
		if u.Scheme == "http" && !allowPlainHTTP {
			continue
		}
		k, err := norm(u)
		if err != nil {
			return nil, fmt.Errorf("invalid storage endpoint %q: %w", raw, err)
		}
		allowed[k] = struct{}{}
	}
	return func(raw string) error {
		u, err := url.Parse(raw)
		if err != nil || u.Hostname() == "" {
			return fmt.Errorf("invalid URL: %s", redactExternalURL(raw))
		}
		if u.User != nil {
			return fmt.Errorf("URL userinfo is not allowed: %s", redactExternalURL(raw))
		}
		if u.Scheme == "http" && !allowPlainHTTP {
			return fmt.Errorf("plain http storage URL refused (set %s=1 to allow it for the configured endpoint): %s",
				EnvStorageAllowPlainHTTP, redactExternalURL(raw))
		}
		if u.Scheme != "https" && u.Scheme != "http" {
			return fmt.Errorf("external location URL must use HTTPS, got %q", u.Scheme)
		}
		k, err := norm(u)
		if err != nil {
			return fmt.Errorf("invalid URL: %s", redactExternalURL(raw))
		}
		if _, ok := allowed[k]; !ok {
			return fmt.Errorf("URL host is not the configured storage endpoint: %s", redactExternalURL(raw))
		}
		return nil
	}, nil
}

// storageURLValidator is implemented by storage backends that know which
// hosts their pre-signed URLs use (vgirpc/s3, vgirpc/gcs); the server then
// fetches inbound pointers only from those hosts.
type storageURLValidator interface {
	ValidateStorageURL(rawURL string) error
}

// uploadGrantsGate is implemented by storage backends that can turn upload
// grants off at run time (vgirpc/s3 with VGI_STORAGE_S3_CHECKSUM=none, for
// services that do not enforce x-amz-checksum-sha256). When it reports false
// the server vends no upload URLs and advertises neither
// VGI-Upload-URL-Support nor VGI-Max-Upload-Bytes, though the storage still
// externalizes results and stores objects.
type uploadGrantsGate interface {
	UploadGrantsEnabled() bool
}

// StorageVendsUploadGrants reports whether storage can vend upload grants: it
// is an [UploadURLProvider] and has not turned them off (a backend with an
// UploadGrantsEnabled() bool method that returns false, such as vgirpc/s3
// with VGI_STORAGE_S3_CHECKSUM=none).
func StorageVendsUploadGrants(storage ExternalStorage) bool {
	if _, ok := storage.(UploadURLProvider); !ok {
		return false
	}
	if g, ok := storage.(uploadGrantsGate); ok && !g.UploadGrantsEnabled() {
		return false
	}
	return true
}

// ExternalConfigForStorage builds a server external-location config around a
// storage the caller constructed (an S3 or GCS backend): the externalize
// threshold (VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES, default 1 MiB), compression
// (VGI_RPC_EXTERNAL_COMPRESSION), the inbound fetch cap
// (VGI_RPC_MAX_FETCH_BYTES), and a fetch client with the storage connect and
// stall timeouts. Inbound pointers are fetched only from the storage's own
// hosts when it reports them (a ValidateStorageURL(string) error method),
// else from any https:// URL. getenv defaults to os.Getenv.
func ExternalConfigForStorage(storage ExternalStorage, getenv func(string) string) (*ExternalLocationConfig, error) {
	if storage == nil {
		return nil, fmt.Errorf("ExternalConfigForStorage: storage is nil")
	}
	getenv = envLookup(getenv)
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
	if v, ok := storage.(storageURLValidator); ok {
		config.URLValidator = v.ValidateStorageURL
	}
	config.HTTPClient = newStorageFetchClient(NewStorageTransport(StorageStallTimeoutFromEnv(getenv)))
	return config, nil
}

// ConfigureHTTPServerWithStorage is [ConfigureHTTPServerFromEnv] for a storage
// the caller built (an S3 or GCS backend selected by VGI_STORAGE_BACKEND):
// results over the externalize threshold go to storage, VGI_RPC_MAX_REQUEST_BYTES
// and VGI_RPC_STATE_REF_INLINE_BYTES apply, and, when the storage vends upload
// grants ([StorageVendsUploadGrants]), clients get upload URLs and
// VGI-Max-Upload-Bytes is advertised. getenv defaults to os.Getenv.
func ConfigureHTTPServerWithStorage(h *HttpServer, storage ExternalStorage, getenv func(string) string) (*ExternalLocationConfig, error) {
	getenv = envLookup(getenv)
	config, err := ExternalConfigForStorage(storage, getenv)
	if err != nil {
		return nil, err
	}
	return configureHTTPServer(h, config, getenv)
}
