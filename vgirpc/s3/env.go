// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgis3

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Environment variables of the direct-storage contract v1 (S3).
const (
	EnvBucket          = "VGI_STORAGE_S3_BUCKET"
	EnvPrefix          = "VGI_STORAGE_S3_PREFIX"
	EnvEndpoint        = "VGI_STORAGE_S3_ENDPOINT"
	EnvRegion          = "VGI_STORAGE_S3_REGION"
	EnvForcePathStyle  = "VGI_STORAGE_S3_FORCE_PATH_STYLE"
	EnvPublicEndpoint  = "VGI_STORAGE_S3_PUBLIC_ENDPOINT"
	EnvChecksum        = "VGI_STORAGE_S3_CHECKSUM"
	EnvAccessKeyID     = "VGI_STORAGE_S3_ACCESS_KEY_ID"
	EnvSecretAccessKey = "VGI_STORAGE_S3_SECRET_ACCESS_KEY"
	EnvSessionToken    = "VGI_STORAGE_S3_SESSION_TOKEN"
	EnvSessionExpires  = "VGI_STORAGE_S3_SESSION_EXPIRES"
	EnvVerify          = "VGI_STORAGE_S3_VERIFY"
	// EnvURLTTLSeconds and EnvAllowPlainHTTP are shared with the GCS backend.
	EnvURLTTLSeconds  = "VGI_STORAGE_URL_TTL_SECONDS"
	EnvAllowPlainHTTP = "VGI_STORAGE_ALLOW_PLAIN_HTTP"
)

// ConfigFromEnv reads the VGI_STORAGE_S3_* settings: the bucket (required),
// prefix (default empty), endpoint, region (VGI_STORAGE_S3_REGION, then
// AWS_REGION, then AWS_DEFAULT_REGION, then us-east-1), path style (default
// true with an endpoint), public endpoint, checksum mode (sha256 or none),
// URL lifetime (VGI_STORAGE_URL_TTL_SECONDS, default 3600, at most 7 days),
// explicit keys and session token (else the AWS default chain), and
// VGI_STORAGE_ALLOW_PLAIN_HTTP. getenv defaults to os.Getenv.
func ConfigFromEnv(getenv func(string) string) (string, S3Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	get := func(k string) string { return strings.TrimSpace(getenv(k)) }
	bucket := get(EnvBucket)
	if bucket == "" {
		return "", S3Config{}, fmt.Errorf("%s is not set", EnvBucket)
	}
	cfg := S3Config{
		Prefix:            getenv(EnvPrefix),
		ExactPrefix:       true,
		EndpointURL:       strings.TrimRight(get(EnvEndpoint), "/"),
		PublicEndpointURL: strings.TrimRight(get(EnvPublicEndpoint), "/"),
		AccessKeyID:       get(EnvAccessKeyID),
		SecretAccessKey:   get(EnvSecretAccessKey),
		SessionToken:      get(EnvSessionToken),
		Checksum:          get(EnvChecksum),
	}
	for _, k := range []string{EnvRegion, "AWS_REGION", "AWS_DEFAULT_REGION"} {
		if v := get(k); v != "" {
			cfg.Region = v
			break
		}
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if raw := get(EnvForcePathStyle); raw != "" {
		v, err := parseBool(raw)
		if err != nil {
			return "", S3Config{}, fmt.Errorf("%s=%q: %w", EnvForcePathStyle, raw, err)
		}
		cfg.ForcePathStyle = v
		cfg.VirtualHostedStyle = !v
	}
	switch strings.ToLower(cfg.Checksum) {
	case "", ChecksumSHA256, ChecksumNone:
	default:
		return "", S3Config{}, fmt.Errorf("%s=%q: expected sha256 or none", EnvChecksum, cfg.Checksum)
	}
	if raw := get(EnvURLTTLSeconds); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return "", S3Config{}, fmt.Errorf("%s=%q: expected a positive number of seconds", EnvURLTTLSeconds, raw)
		}
		cfg.PresignExpirySeconds = n
	}
	if (cfg.AccessKeyID == "") != (cfg.SecretAccessKey == "") {
		return "", S3Config{}, fmt.Errorf("%s and %s must be set together", EnvAccessKeyID, EnvSecretAccessKey)
	}
	if cfg.SessionToken != "" && cfg.AccessKeyID == "" {
		return "", S3Config{}, fmt.Errorf("%s needs %s and %s", EnvSessionToken, EnvAccessKeyID, EnvSecretAccessKey)
	}
	if raw := get(EnvSessionExpires); raw != "" {
		t, err := ParseSessionExpires(raw)
		if err != nil {
			return "", S3Config{}, fmt.Errorf("%s=%q: %w", EnvSessionExpires, raw, err)
		}
		if cfg.SessionToken == "" {
			return "", S3Config{}, fmt.Errorf("%s needs %s", EnvSessionExpires, EnvSessionToken)
		}
		cfg.SessionExpires = t
	}
	if raw := get(EnvAllowPlainHTTP); raw != "" {
		v, err := parseBool(raw)
		if err != nil {
			return "", S3Config{}, fmt.Errorf("%s=%q: %w", EnvAllowPlainHTTP, raw, err)
		}
		cfg.AllowPlainHTTP = v
	}
	// A plain-http endpoint sends pre-signed URLs (their credential is the
	// query string) and object bytes in the clear: an explicit opt-in.
	for name, ep := range map[string]string{EnvEndpoint: cfg.EndpointURL, EnvPublicEndpoint: cfg.PublicEndpointURL} {
		if u, err := url.Parse(ep); err == nil && u.Scheme == "http" && !cfg.AllowPlainHTTP {
			return "", S3Config{}, fmt.Errorf("%s is plain http (%s://%s): set %s=1 to allow it (and the extension's vgi_storage_allow_plain_http)",
				name, u.Scheme, u.Host, EnvAllowPlainHTTP)
		}
	}
	return bucket, cfg, nil
}

// ParseSessionExpires reads VGI_STORAGE_S3_SESSION_EXPIRES: an RFC 3339
// timestamp or Unix epoch seconds.
func ParseSessionExpires(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
		return time.Unix(n, 0).UTC(), nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("expected an RFC 3339 timestamp or epoch seconds")
	}
	return t.UTC(), nil
}

// FromEnv builds the S3 storage from [ConfigFromEnv]. With
// VGI_STORAGE_S3_VERIFY=1 it checks the bucket now ([S3Storage.Verify]);
// otherwise the check runs on first use. getenv defaults to os.Getenv.
func FromEnv(getenv func(string) string) (*S3Storage, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	bucket, cfg, err := ConfigFromEnv(getenv)
	if err != nil {
		return nil, err
	}
	storage, err := NewS3Storage(bucket, cfg)
	if err != nil {
		return nil, err
	}
	if raw := strings.TrimSpace(getenv(EnvVerify)); raw != "" {
		verify, err := parseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("%s=%q: %w", EnvVerify, raw, err)
		}
		if verify {
			if err := storage.Verify(context.Background()); err != nil {
				return nil, err
			}
		}
	}
	return storage, nil
}

func parseBool(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	}
	return false, fmt.Errorf("expected true or false")
}
