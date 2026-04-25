// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Query-farm/vgi-rpc/vgirpc"
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

// AllowAllValidator is a vgirpc.URLValidator that accepts every URL,
// matching the test client's permissive configuration.
func AllowAllValidator(string) error { return nil }

// Compile-time check that FakeStorage satisfies the ExternalStorage interface.
var _ vgirpc.ExternalStorage = (*FakeStorage)(nil)
