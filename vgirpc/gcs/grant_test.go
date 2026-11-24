// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgigcs

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeKeyFile writes a throwaway service-account key file.
func writeKeyFile(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, _ := x509.MarshalPKCS8PrivateKey(key)
	raw, _ := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "vgi-test",
		"private_key_id": "k1",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   "vgi@vgi-test.iam.gserviceaccount.com",
		"client_id":      "1",
		"token_uri":      "https://oauth2.googleapis.com/token",
	})
	path := filepath.Join(t.TempDir(), "sa.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestGenerateUploadURLOnCustomEndpoint: a sized V4 PUT signed with the key
// file, on the custom endpoint's host (path style), with the signed headers
// the client must send; the GET is on the same object.
func TestGenerateUploadURLOnCustomEndpoint(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	keyFile := writeKeyFile(t)
	bucket, cfg, err := ConfigFromEnv(func(k string) string {
		return map[string]string{
			EnvBucket: "bkt", EnvEndpoint: "http://gcs.internal:4443", EnvCredentialsFile: keyFile,
			EnvPrefix: "p/", EnvURLTTLSeconds: "900", EnvAllowPlainHTTP: "1",
		}[k]
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewGCSStorage(bucket, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.verified = true // no network: signing only
	grant, err := s.GenerateUploadURL(1234, strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	put, _ := url.Parse(grant.UploadURL)
	if put.Scheme != "http" || put.Host != "gcs.internal:4443" || !strings.HasPrefix(put.Path, "/bkt/p/") {
		t.Fatalf("PUT URL %s://%s%s", put.Scheme, put.Host, put.Path)
	}
	q := put.Query()
	if q.Get("X-Goog-Algorithm") != "GOOG4-RSA-SHA256" || (q.Get("X-Goog-Expires") != "900" && q.Get("X-Goog-Expires") != "899") ||
		!strings.HasPrefix(q.Get("X-Goog-Credential"), "vgi@vgi-test.iam.gserviceaccount.com/") {
		t.Fatalf("PUT query %v", q)
	}
	signed := q.Get("X-Goog-SignedHeaders")
	for _, h := range []string{"content-type", "host", "x-goog-content-length-range"} {
		if !strings.Contains(signed, h) {
			t.Errorf("signed headers %q lack %s", signed, h)
		}
	}
	if grant.Headers["x-goog-content-length-range"] != "1234,1234" || grant.Headers["Content-Type"] != "application/octet-stream" {
		t.Errorf("headers %v", grant.Headers)
	}
	get, _ := url.Parse(grant.DownloadURL)
	if get.Path != put.Path || grant.ObjectID != strings.TrimPrefix(put.Path, "/bkt/") {
		t.Errorf("GET %s / object %s vs PUT %s", get.Path, grant.ObjectID, put.Path)
	}
	if err := s.ValidateStorageURL(grant.DownloadURL); err != nil {
		t.Errorf("own URL refused: %v", err)
	}
	if err := s.ValidateStorageURL("http://other:4443/bkt/x"); err == nil {
		t.Error("other host accepted")
	}
	if _, err := s.GenerateUploadURL(1, "XYZ"); err == nil {
		t.Error("bad sha256 accepted")
	}
}

func TestGCSConfigFromEnvErrors(t *testing.T) {
	for name, m := range map[string]map[string]string{
		"no bucket": {},
		"bad ttl":   {EnvBucket: "b", EnvURLTTLSeconds: "x"},
		"bad http":  {EnvBucket: "b", EnvAllowPlainHTTP: "perhaps"},
	} {
		if _, _, err := ConfigFromEnv(func(k string) string { return m[k] }); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	// Default (no endpoint): only https://storage.googleapis.com validates.
	t.Setenv("STORAGE_EMULATOR_HOST", "localhost:1")
	s, err := NewGCSStorage("b", GCSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if s.ValidateStorageURL("https://storage.googleapis.com/b/k") != nil || s.ValidateStorageURL("https://evil.com/b/k") == nil {
		t.Error("default endpoint validation")
	}
}

func TestGCSKeyLayoutAndEnv(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "localhost:1")
	s, err := NewGCSStorage("b", GCSConfig{Prefix: "p/", ExactPrefix: true})
	if err != nil {
		t.Fatal(err)
	}
	if s.prefix != "p/rpc/" || s.FunctionStore().scopePrefix("att-00/s.1") != "p/fs/att-00%2Fs.1/" || s.FunctionStore().scopePrefix("") != "p/fs/" {
		t.Errorf("layout rpc=%q fs=%q", s.prefix, s.FunctionStore().scopePrefix("att-00/s.1"))
	}
	if _, _, err := ConfigFromEnv(func(k string) string {
		return map[string]string{EnvBucket: "b", EnvEndpoint: "http://gcs:4443"}[k]
	}); err == nil || !strings.Contains(err.Error(), EnvAllowPlainHTTP) {
		t.Errorf("plain-http endpoint without opt-in: %v", err)
	}
	if _, err := FromEnv(func(k string) string {
		return map[string]string{EnvBucket: "b", EnvVerify: "maybe"}[k]
	}); err == nil {
		t.Error("bad VGI_STORAGE_GCS_VERIFY accepted")
	}
}
