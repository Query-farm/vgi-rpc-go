// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgis3

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
)

// TestMinIOCustomEndpoint runs against a real S3-compatible server when
// VGI_S3_TEST_ENDPOINT (e.g. http://127.0.0.1:9000), VGI_S3_TEST_BUCKET,
// VGI_S3_TEST_ACCESS_KEY and VGI_S3_TEST_SECRET_KEY are set: a plain-http
// custom endpoint, path-style, static credentials, no region configured.
func TestMinIOCustomEndpoint(t *testing.T) {
	endpoint := os.Getenv("VGI_S3_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("VGI_S3_TEST_ENDPOINT not set")
	}
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
	storage, err := NewS3Storage(os.Getenv("VGI_S3_TEST_BUCKET"), S3Config{
		EndpointURL:     endpoint,
		AccessKeyID:     os.Getenv("VGI_S3_TEST_ACCESS_KEY"),
		SecretAccessKey: os.Getenv("VGI_S3_TEST_SECRET_KEY"),
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("minio"), 1<<20)

	key, err := storage.PutObject(payload)
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	got, err := storage.Get(key, 0)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("Get: err=%v equal=%v", err, bytes.Equal(got, payload))
	}

	presigned, err := storage.Upload(payload, nil, "")
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	pu, _ := url.Parse(presigned)
	eu, _ := url.Parse(endpoint)
	if pu.Host != eu.Host || !strings.HasPrefix(pu.Path, "/"+os.Getenv("VGI_S3_TEST_BUCKET")+"/") {
		t.Fatalf("presigned URL %s://%s%s is not path-style on the custom endpoint", pu.Scheme, pu.Host, pu.Path)
	}
	resp, err := http.Get(presigned)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, payload) {
		t.Fatalf("presigned GET: HTTP %d, equal=%v", resp.StatusCode, bytes.Equal(body, payload))
	}
	if _, err := storage.Get(key+"-missing", 0); err == nil || !strings.Contains(err.Error(), "gone") {
		t.Fatalf("missing key: %v", err)
	}
}
