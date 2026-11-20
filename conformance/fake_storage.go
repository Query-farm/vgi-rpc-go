// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// FakeStorage is an ExternalStorage implementation targeting the
// vgi_rpc.conformance.fake_storage Python fixture. The workflow is:
//
//	POST {baseURL}/_alloc          → {"object_url": "<full-url>"}
//	PUT  <full-url>                with the IPC bytes
//
// The returned URL is the exact value handed to the client in the
// vgi_rpc.location batch metadata.
type FakeStorage struct {
	BaseURL string
	Client  *http.Client
}

// NewFakeStorage constructs a FakeStorage wired against baseURL.
func NewFakeStorage(baseURL string) *FakeStorage {
	return &FakeStorage{BaseURL: baseURL, Client: http.DefaultClient}
}

// Upload allocates a blob slot, uploads the payload, and returns the
// object URL the client should fetch from.
func (f *FakeStorage) Upload(data []byte, _ *arrow.Schema, contentEncoding string) (string, error) {
	allocResp, err := f.Client.Post(f.BaseURL+"/alloc", "application/json", nil)
	if err != nil {
		return "", fmt.Errorf("fake storage alloc: %w", err)
	}
	defer allocResp.Body.Close()
	if allocResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(allocResp.Body)
		return "", fmt.Errorf("fake storage alloc returned %d: %s", allocResp.StatusCode, body)
	}
	var alloc struct {
		ObjectURL string `json:"object_url"`
	}
	if err := json.NewDecoder(allocResp.Body).Decode(&alloc); err != nil {
		return "", fmt.Errorf("fake storage alloc decode: %w", err)
	}

	req, err := http.NewRequest(http.MethodPut, alloc.ObjectURL, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("fake storage put build: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	putResp, err := f.Client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fake storage put: %w", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(putResp.Body)
		return "", fmt.Errorf("fake storage put returned %d: %s", putResp.StatusCode, body)
	}
	return alloc.ObjectURL, nil
}

// GenerateUploadURL allocates a new blob slot for a body of exactly size
// bytes hashing to sha256Hex and returns a URL pair the client can PUT to and
// the server can later GET from. The fake storage uses the same URL for both
// verbs (HTTP method dispatch in the service). The size and hash ride the
// alloc request so a fixture that verifies sized uploads can; any headers it
// wants on the PUT come back as upload_headers.
func (f *FakeStorage) GenerateUploadURL(size int64, sha256Hex string) (vgirpc.UploadURL, error) {
	digest, err := hex.DecodeString(sha256Hex)
	if err != nil || len(digest) != sha256.Size {
		return vgirpc.UploadURL{}, fmt.Errorf("upload sha256 must be 64 hex characters")
	}
	// A sized allocation: the fixture verifies the PUT against it and hands
	// back the header the PUT must carry.
	reqBody, err := json.Marshal(map[string]any{"size": size, "sha256_b64": base64.StdEncoding.EncodeToString(digest)})
	if err != nil {
		return vgirpc.UploadURL{}, err
	}
	allocResp, err := f.Client.Post(f.BaseURL+"/alloc", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return vgirpc.UploadURL{}, fmt.Errorf("fake storage alloc: %w", err)
	}
	defer allocResp.Body.Close()
	if allocResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(allocResp.Body)
		return vgirpc.UploadURL{}, fmt.Errorf("fake storage alloc returned %d: %s", allocResp.StatusCode, body)
	}
	var alloc struct {
		ObjectURL     string            `json:"object_url"`
		UploadURL     string            `json:"upload_url"`
		DownloadURL   string            `json:"download_url"`
		UploadHeaders map[string]string `json:"upload_headers"`
	}
	if err := json.NewDecoder(allocResp.Body).Decode(&alloc); err != nil {
		return vgirpc.UploadURL{}, fmt.Errorf("fake storage alloc decode: %w", err)
	}
	uploadURL := alloc.UploadURL
	if uploadURL == "" {
		uploadURL = alloc.ObjectURL
	}
	downloadURL := alloc.DownloadURL
	if downloadURL == "" {
		downloadURL = alloc.ObjectURL
	}
	headers := alloc.UploadHeaders
	if headers == nil {
		headers = map[string]string{}
	}
	return vgirpc.UploadURL{
		UploadURL:   uploadURL,
		DownloadURL: downloadURL,
		ExpiresAt:   time.Now().UTC().Add(1 * time.Hour),
		Headers:     headers,
	}, nil
}

// AllowAllValidator is a vgirpc.URLValidator that accepts every URL,
// matching the test client's permissive configuration.
func AllowAllValidator(string) error { return nil }

// Compile-time check that FakeStorage satisfies the ExternalStorage interface.
var _ vgirpc.ExternalStorage = (*FakeStorage)(nil)
var _ vgirpc.UploadURLProvider = (*FakeStorage)(nil)
