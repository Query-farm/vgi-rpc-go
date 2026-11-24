// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

func envMap(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestStorageBackendFromEnv(t *testing.T) {
	cases := []struct {
		env  map[string]string
		want string
		err  string
	}{
		{map[string]string{}, "none", ""},
		{map[string]string{EnvBlobVendorURL: "https://v"}, "vendor", ""},
		{map[string]string{EnvStorageS3Bucket: "b"}, "s3", ""},
		{map[string]string{EnvStorageGCSBucket: "b"}, "gcs", ""},
		{map[string]string{EnvBlobVendorURL: "https://v", EnvStorageS3Bucket: "b"}, "", "VGI_BLOB_VENDOR_URL and VGI_STORAGE_S3_BUCKET"},
		{map[string]string{EnvStorageS3Bucket: "b", EnvStorageGCSBucket: "g"}, "", "VGI_STORAGE_S3_BUCKET and VGI_STORAGE_GCS_BUCKET"},
		{map[string]string{EnvBlobVendorURL: "https://v", EnvStorageS3Bucket: "b", EnvStorageBackend: "S3"}, "s3", ""},
		{map[string]string{EnvBlobVendorURL: "https://v", EnvStorageBackend: "none"}, "none", ""},
		{map[string]string{EnvStorageBackend: "azure"}, "", "expected vendor, s3, gcs or none"},
	}
	for _, c := range cases {
		got, err := StorageBackendFromEnv(envMap(c.env))
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Errorf("%v: err=%v, want %q", c.env, err, c.err)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("%v: got %q, %v; want %q", c.env, got, err, c.want)
		}
	}
}

func TestStorageEndpointValidator(t *testing.T) {
	v, err := StorageEndpointValidator([]string{"http://minio:9000", "https://S3.example.com"}, false)
	if err != nil {
		t.Fatal(err)
	}
	for raw, ok := range map[string]bool{
		"https://s3.example.com/b/k?X-Amz-Signature=x": true,
		"https://s3.example.com:443/b/k":               true,
		"https://s3.example.com:8443/b/k":              false,
		"https://evil.example.com/b/k":                 false,
		"http://minio:9000/b/k":                        false, // plain http not opted in
		"https://user:pw@s3.example.com/b/k":           false,
		"ftp://s3.example.com/b/k":                     false,
	} {
		if got := v(raw) == nil; got != ok {
			t.Errorf("%s: allowed=%v want %v (%v)", raw, got, ok, v(raw))
		}
	}
	v, err = StorageEndpointValidator([]string{"http://minio:9000"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := v("http://minio:9000/b/k"); err != nil {
		t.Errorf("opted-in http endpoint refused: %v", err)
	}
	if err := v("http://minio:9001/b/k"); err == nil {
		t.Error("another port accepted")
	}
	if err := v("http://other:9000/b/k"); err == nil {
		t.Error("another host accepted")
	}
	if !StorageAllowPlainHTTPFromEnv(envMap(map[string]string{EnvStorageAllowPlainHTTP: "1"})) ||
		StorageAllowPlainHTTPFromEnv(envMap(map[string]string{})) {
		t.Error("StorageAllowPlainHTTPFromEnv")
	}
}

// fakeDirectStorage is a storage with all roles and a grants switch.
type fakeDirectStorage struct{ grants bool }

func (fakeDirectStorage) Upload([]byte, *arrow.Schema, string) (string, error) { return "", nil }
func (fakeDirectStorage) GenerateUploadURL(int64, string) (UploadURL, error) {
	return UploadURL{ObjectID: "o", ExpiresAt: time.Now()}, nil
}
func (fakeDirectStorage) PutObject([]byte) (string, error)  { return "o", nil }
func (fakeDirectStorage) Get(string, int64) ([]byte, error) { return nil, nil }
func (f fakeDirectStorage) UploadGrantsEnabled() bool       { return f.grants }
func (fakeDirectStorage) ValidateStorageURL(raw string) error {
	if strings.HasPrefix(raw, "https://store.example/") {
		return nil
	}
	return errNotStore
}

var errNotStore = &RpcError{Type: "ValueError", Message: "not the store"}

func TestConfigureHTTPServerWithStorage(t *testing.T) {
	for _, grants := range []bool{true, false} {
		h := NewHttpServer(NewServer())
		cfg, err := ConfigureHTTPServerWithStorage(h, fakeDirectStorage{grants: grants}, envMap(map[string]string{
			EnvMaxUploadBytes: "1000", EnvMaxRequestBytes: "4096",
		}))
		if err != nil {
			t.Fatal(err)
		}
		if (h.uploadURLProvider != nil) != grants {
			t.Errorf("grants=%v: upload provider set=%v", grants, h.uploadURLProvider != nil)
		}
		if cfg.URLValidator("https://store.example/k") != nil || cfg.URLValidator("https://other.example/k") == nil {
			t.Error("storage URL validator not used")
		}
		if h.maxRequestBytes != 4096 {
			t.Errorf("max request bytes %d", h.maxRequestBytes)
		}
	}
	if _, err := ConfigureHTTPServerFromEnv(NewHttpServer(NewServer()), envMap(map[string]string{EnvStorageS3Bucket: "b"})); err == nil ||
		!strings.Contains(err.Error(), "ConfigureHTTPServerWithStorage") {
		t.Errorf("s3 via ConfigureHTTPServerFromEnv: %v", err)
	}
	if _, err := ConfigureHTTPServerFromEnv(NewHttpServer(NewServer()), envMap(map[string]string{EnvStorageBackend: "vendor"})); err == nil {
		t.Error("explicit vendor without URL accepted")
	}
	if cfg, err := ConfigureHTTPServerFromEnv(NewHttpServer(NewServer()), envMap(map[string]string{
		EnvStorageBackend: "none", EnvBlobVendorURL: "https://vendor.example", EnvBlobVendorToken: "t",
	})); err != nil || cfg != nil {
		t.Errorf("explicit none: cfg=%v err=%v", cfg, err)
	}
}
