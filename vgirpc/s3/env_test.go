// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgis3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func envOf(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestConfigFromEnv(t *testing.T) {
	bucket, cfg, err := ConfigFromEnv(envOf(map[string]string{
		EnvBucket: "b", EnvEndpoint: "http://minio:9000/", EnvPublicEndpoint: "https://s3.example.com",
		"AWS_DEFAULT_REGION": "eu-west-1", EnvChecksum: "none", EnvURLTTLSeconds: "900", EnvAllowPlainHTTP: "1",
		EnvAccessKeyID: "AK", EnvSecretAccessKey: "SK", EnvSessionToken: "ST",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if bucket != "b" || cfg.EndpointURL != "http://minio:9000" || cfg.PublicEndpointURL != "https://s3.example.com" ||
		cfg.Region != "eu-west-1" || cfg.Checksum != "none" || cfg.PresignExpirySeconds != 900 || !cfg.AllowPlainHTTP ||
		cfg.SessionToken != "ST" || cfg.Prefix != "" || !cfg.ExactPrefix || cfg.ForcePathStyle || cfg.VirtualHostedStyle {
		t.Fatalf("config %+v", cfg)
	}
	// Region precedence: VGI_STORAGE_S3_REGION, AWS_REGION, AWS_DEFAULT_REGION, us-east-1.
	for env, want := range map[string]string{EnvRegion: "auto", "AWS_REGION": "ap-south-1", "": "us-east-1"} {
		m := map[string]string{EnvBucket: "b", "AWS_DEFAULT_REGION": ""}
		if env != "" {
			m[env] = want
		}
		if _, c, _ := ConfigFromEnv(envOf(m)); c.Region != want {
			t.Errorf("%s: region %q want %q", env, c.Region, want)
		}
	}
	if _, c, _ := ConfigFromEnv(envOf(map[string]string{EnvBucket: "b", EnvForcePathStyle: "false", EnvEndpoint: "https://x"})); !c.VirtualHostedStyle {
		t.Error("FORCE_PATH_STYLE=false did not select virtual-hosted style")
	}
	for name, m := range map[string]map[string]string{
		"no bucket":      {},
		"bad checksum":   {EnvBucket: "b", EnvChecksum: "md5"},
		"bad ttl":        {EnvBucket: "b", EnvURLTTLSeconds: "-1"},
		"key no secret":  {EnvBucket: "b", EnvAccessKeyID: "AK"},
		"token no keys":  {EnvBucket: "b", EnvSessionToken: "ST"},
		"bad path style": {EnvBucket: "b", EnvForcePathStyle: "maybe"},
	} {
		if _, _, err := ConfigFromEnv(envOf(m)); err == nil {
			t.Errorf("%s: accepted", name)
		} else if strings.Contains(err.Error(), "SK") || strings.Contains(err.Error(), "ST\"") {
			t.Errorf("%s: error leaks a secret: %v", name, err)
		}
	}
}

func TestValidateStorageURL(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AK")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "SK")
	s, err := NewS3Storage("bkt", S3Config{EndpointURL: "http://minio:9000", PublicEndpointURL: "https://s3.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	for raw, ok := range map[string]bool{
		"https://s3.example.com/bkt/k":     true,
		"https://bkt.s3.example.com/k":     true,
		"https://evil.example.com/bkt/k":   false,
		"http://minio:9000/bkt/k":          false, // plain http not allowed
		"https://u:p@s3.example.com/bkt/k": false,
	} {
		if got := s.ValidateStorageURL(raw) == nil; got != ok {
			t.Errorf("%s: %v", raw, s.ValidateStorageURL(raw))
		}
	}
	s, _ = NewS3Storage("bkt", S3Config{EndpointURL: "http://minio:9000", AllowPlainHTTP: true})
	if err := s.ValidateStorageURL("http://minio:9000/bkt/k"); err != nil {
		t.Errorf("opted-in http: %v", err)
	}
	if err := s.ValidateStorageURL("http://minio:9001/bkt/k"); err == nil {
		t.Error("other port accepted")
	}
	s, _ = NewS3Storage("bkt", S3Config{Region: "us-west-2"})
	if err := s.ValidateStorageURL("https://bkt.s3.us-west-2.amazonaws.com/k"); err != nil {
		t.Errorf("AWS host: %v", err)
	}
	if err := s.ValidateStorageURL("https://bkt.s3.evil.com/k"); err == nil {
		t.Error("non-AWS host accepted without an endpoint")
	}
}

// TestVerifyWrongRegion: the bucket check names the region the service reports.
func TestVerifyWrongRegion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-amz-bucket-region", "eu-central-1")
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer srv.Close()
	t.Setenv("AWS_ACCESS_KEY_ID", "AK")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "SK")
	s, err := NewS3Storage("bkt", S3Config{EndpointURL: srv.URL, Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	err = s.Verify(context.Background())
	if err == nil || !strings.Contains(err.Error(), `region "eu-central-1"`) || !strings.Contains(err.Error(), "VGI_STORAGE_S3_REGION=eu-central-1") {
		t.Fatalf("Verify: %v", err)
	}
	// First use runs the same check and fails the call.
	if _, err := s.PutObject([]byte("x")); err == nil || !strings.Contains(err.Error(), "eu-central-1") {
		t.Fatalf("PutObject before a passing check: %v", err)
	}
}
