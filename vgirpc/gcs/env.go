// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgigcs

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Environment variables of the direct-storage contract v1 (GCS).
const (
	EnvBucket          = "VGI_STORAGE_GCS_BUCKET"
	EnvPrefix          = "VGI_STORAGE_GCS_PREFIX"
	EnvEndpoint        = "VGI_STORAGE_GCS_ENDPOINT"
	EnvCredentialsFile = "VGI_STORAGE_GCS_CREDENTIALS_FILE"
	EnvVerify          = "VGI_STORAGE_GCS_VERIFY"
	// EnvURLTTLSeconds and EnvAllowPlainHTTP are shared with the S3 backend.
	EnvURLTTLSeconds  = "VGI_STORAGE_URL_TTL_SECONDS"
	EnvAllowPlainHTTP = "VGI_STORAGE_ALLOW_PLAIN_HTTP"
)

// ConfigFromEnv reads the VGI_STORAGE_GCS_* settings: the bucket (required),
// prefix (default empty), endpoint, credentials file (else ADC), URL lifetime
// (VGI_STORAGE_URL_TTL_SECONDS, default 3600, at most 7 days) and
// VGI_STORAGE_ALLOW_PLAIN_HTTP. getenv defaults to os.Getenv.
func ConfigFromEnv(getenv func(string) string) (string, GCSConfig, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	get := func(k string) string { return strings.TrimSpace(getenv(k)) }
	bucket := get(EnvBucket)
	if bucket == "" {
		return "", GCSConfig{}, fmt.Errorf("%s is not set", EnvBucket)
	}
	cfg := GCSConfig{
		Prefix:          getenv(EnvPrefix),
		ExactPrefix:     true,
		Endpoint:        strings.TrimRight(get(EnvEndpoint), "/"),
		CredentialsFile: get(EnvCredentialsFile),
	}
	if raw := get(EnvURLTTLSeconds); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return "", GCSConfig{}, fmt.Errorf("%s=%q: expected a positive number of seconds", EnvURLTTLSeconds, raw)
		}
		cfg.PresignExpirySeconds = n
	}
	switch strings.ToLower(get(EnvAllowPlainHTTP)) {
	case "", "0", "false", "no", "off":
	case "1", "true", "yes", "on":
		cfg.AllowPlainHTTP = true
	default:
		return "", GCSConfig{}, fmt.Errorf("%s=%q: expected true or false", EnvAllowPlainHTTP, getenv(EnvAllowPlainHTTP))
	}
	if u, err := url.Parse(cfg.Endpoint); err == nil && u.Scheme == "http" && !cfg.AllowPlainHTTP {
		return "", GCSConfig{}, fmt.Errorf("%s is plain http (%s://%s): set %s=1 to allow it (and the extension's vgi_storage_allow_plain_http)",
			EnvEndpoint, u.Scheme, u.Host, EnvAllowPlainHTTP)
	}
	return bucket, cfg, nil
}

// FromEnv builds the GCS storage from [ConfigFromEnv]. With
// VGI_STORAGE_GCS_VERIFY=1 it checks the bucket now ([GCSStorage.Verify]);
// otherwise the check runs on first use. getenv defaults to os.Getenv.
func FromEnv(getenv func(string) string) (*GCSStorage, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	bucket, cfg, err := ConfigFromEnv(getenv)
	if err != nil {
		return nil, err
	}
	storage, err := NewGCSStorage(bucket, cfg)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(strings.TrimSpace(getenv(EnvVerify))) {
	case "", "0", "false", "no", "off":
	case "1", "true", "yes", "on":
		if err := storage.Verify(context.Background()); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%s=%q: expected true or false", EnvVerify, getenv(EnvVerify))
	}
	return storage, nil
}
