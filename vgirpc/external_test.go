// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/klauspost/compress/zstd"
)

var testSchema = arrow.NewSchema([]arrow.Field{
	{Name: "value", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// mockStorage is an in-memory ExternalStorage for testing.
type mockStorage struct {
	data                map[string][]byte
	counter             int
	lastContentEncoding string
}

func newMockStorage() *mockStorage {
	return &mockStorage{data: make(map[string][]byte)}
}

func (m *mockStorage) Upload(data []byte, schema *arrow.Schema, contentEncoding string) (string, error) {
	m.counter++
	m.lastContentEncoding = contentEncoding
	url := fmt.Sprintf("https://mock.storage/%d", m.counter)
	m.data[url] = data
	return url, nil
}

// makeBatch creates a test record batch with n int64 values.
func makeBatch(n int) arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for i := 0; i < n; i++ {
		b.Append(int64(i))
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(testSchema, []arrow.Array{col}, int64(n))
}

// serializeTestIPC serializes a batch to IPC format bytes.
func serializeTestIPC(batch arrow.RecordBatch) []byte {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()))
	w.Write(batch)
	w.Close()
	return buf.Bytes()
}

// ===========================================================================
// Detection tests
// ===========================================================================

func TestIsExternalLocationBatch_Positive(t *testing.T) {
	batch, meta := MakeExternalLocationBatch(testSchema, "https://mock/test")
	defer batch.Release()
	if !IsExternalLocationBatch(batch, meta) {
		t.Fatal("expected true for pointer batch")
	}
}

func TestIsExternalLocationBatch_NonZeroRows(t *testing.T) {
	batch := makeBatch(1)
	defer batch.Release()
	meta := arrow.NewMetadata([]string{MetaLocation}, []string{"https://mock/test"})
	if IsExternalLocationBatch(batch, meta) {
		t.Fatal("expected false for non-zero-row batch")
	}
}

func TestIsExternalLocationBatch_NoMetadata(t *testing.T) {
	batch, _ := MakeExternalLocationBatch(testSchema, "https://mock/test")
	defer batch.Release()
	if IsExternalLocationBatch(batch, arrow.Metadata{}) {
		t.Fatal("expected false for empty metadata")
	}
}

func TestIsExternalLocationBatch_LogBatch(t *testing.T) {
	batch, _ := MakeExternalLocationBatch(testSchema, "https://mock/test")
	defer batch.Release()
	meta := arrow.NewMetadata(
		[]string{MetaLocation, MetaLogLevel},
		[]string{"https://mock/test", "INFO"},
	)
	if IsExternalLocationBatch(batch, meta) {
		t.Fatal("expected false for log batch with location key")
	}
}

func TestIsExternalLocationBatch_NoLocationKey(t *testing.T) {
	batch, _ := MakeExternalLocationBatch(testSchema, "https://mock/test")
	defer batch.Release()
	meta := arrow.NewMetadata([]string{"other"}, []string{"value"})
	if IsExternalLocationBatch(batch, meta) {
		t.Fatal("expected false for batch without location key")
	}
}

// ===========================================================================
// Creation tests
// ===========================================================================

func TestMakeExternalLocationBatch_ZeroRows(t *testing.T) {
	batch, _ := MakeExternalLocationBatch(testSchema, "https://mock/test")
	defer batch.Release()
	if batch.NumRows() != 0 {
		t.Fatalf("expected 0 rows, got %d", batch.NumRows())
	}
}

func TestMakeExternalLocationBatch_HasLocationMeta(t *testing.T) {
	_, meta := MakeExternalLocationBatch(testSchema, "https://mock/test")
	url, ok := metaGet(meta, MetaLocation)
	if !ok {
		t.Fatal("expected MetaLocation key")
	}
	if url != "https://mock/test" {
		t.Fatalf("expected URL 'https://mock/test', got %q", url)
	}
}

func TestMakeExternalLocationBatch_IsDetected(t *testing.T) {
	batch, meta := MakeExternalLocationBatch(testSchema, "https://mock/test")
	defer batch.Release()
	if !IsExternalLocationBatch(batch, meta) {
		t.Fatal("created batch not detected as pointer")
	}
}

// ===========================================================================
// Externalization (write path) tests
// ===========================================================================

func TestMaybeExternalizeBatch_AboveThreshold(t *testing.T) {
	storage := newMockStorage()
	config := &ExternalLocationConfig{
		Storage:                   storage,
		ExternalizeThresholdBytes: 10,
	}
	batch := makeBatch(100)
	defer batch.Release()

	extBatch, extMeta, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer extBatch.Release()

	if extBatch.NumRows() != 0 {
		t.Fatalf("expected pointer batch (0 rows), got %d", extBatch.NumRows())
	}
	_, ok := metaGet(extMeta, MetaLocation)
	if !ok {
		t.Fatal("expected MetaLocation key on pointer batch")
	}
	if len(storage.data) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(storage.data))
	}
}

func TestMaybeExternalizeBatch_BelowThreshold(t *testing.T) {
	storage := newMockStorage()
	config := &ExternalLocationConfig{
		Storage:                   storage,
		ExternalizeThresholdBytes: 10_000_000,
	}
	batch := makeBatch(1)
	defer batch.Release()

	extBatch, _, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if extBatch.NumRows() != 1 {
		t.Fatalf("expected original batch (1 row), got %d", extBatch.NumRows())
	}
	if len(storage.data) != 0 {
		t.Fatalf("expected no uploads, got %d", len(storage.data))
	}
}

func TestMaybeExternalizeBatch_NoStorage(t *testing.T) {
	config := &ExternalLocationConfig{Storage: nil}
	batch := makeBatch(100)
	defer batch.Release()

	extBatch, _, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if extBatch.NumRows() != 100 {
		t.Fatalf("expected original batch, got %d rows", extBatch.NumRows())
	}
}

func TestMaybeExternalizeBatch_ZeroRowPassthrough(t *testing.T) {
	storage := newMockStorage()
	config := &ExternalLocationConfig{
		Storage:                   storage,
		ExternalizeThresholdBytes: 0,
	}
	batch, _ := MakeExternalLocationBatch(testSchema, "https://existing")
	defer batch.Release()

	extBatch, _, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if extBatch.NumRows() != 0 {
		t.Fatal("zero-row batch should pass through")
	}
	if len(storage.data) != 0 {
		t.Fatal("zero-row batch should not be uploaded")
	}
}

func TestMaybeExternalizeBatch_NilConfig(t *testing.T) {
	batch := makeBatch(100)
	defer batch.Release()

	extBatch, _, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if extBatch.NumRows() != 100 {
		t.Fatal("nil config should pass through")
	}
}

// ===========================================================================
// Resolution (read path) tests
// ===========================================================================

func TestResolveExternalLocation_BasicResolution(t *testing.T) {
	storage := newMockStorage()

	dataBatch := makeBatch(1)
	defer dataBatch.Release()
	ipcBytes := serializeTestIPC(dataBatch)
	url := "https://mock.storage/basic"
	storage.data[url] = ipcBytes

	// Create pointer batch (not used directly — we remap to test server URL below)
	pointer, _ := MakeExternalLocationBatch(testSchema, url)
	defer pointer.Release()

	// Start mock HTTP server
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := storage.data["https://mock.storage"+r.URL.Path]
		if !ok {
			w.WriteHeader(404)
			return
		}
		w.Write(body)
	}))
	defer server.Close()

	// Remap URL to test server
	testURL := server.URL + "/basic"
	pointer2, meta2 := MakeExternalLocationBatch(testSchema, testURL)
	defer pointer2.Release()

	config := &ExternalLocationConfig{
		URLValidator: nil, // disable for test server (not HTTPS with valid cert)
		HTTPClient:   server.Client(),
	}

	resolved, resolvedMeta, err := ResolveExternalLocation(pointer2, meta2, config)
	if err != nil {
		t.Fatal(err)
	}
	defer resolved.Release()

	if resolved.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", resolved.NumRows())
	}

	// Check fetch metadata
	fetchMs, ok := metaGet(resolvedMeta, MetaLocationFetchMs)
	if !ok {
		t.Fatal("missing fetch_ms metadata")
	}
	if fetchMs == "" {
		t.Fatal("empty fetch_ms")
	}
	source, ok := metaGet(resolvedMeta, MetaLocationSource)
	if !ok {
		t.Fatal("missing source metadata")
	}
	if source != testURL {
		t.Fatalf("expected source %q, got %q", testURL, source)
	}
}

func TestResolveExternalLocation_NonPointerPassthrough(t *testing.T) {
	batch := makeBatch(5)
	defer batch.Release()

	config := &ExternalLocationConfig{URLValidator: nil}
	resolved, _, err := ResolveExternalLocation(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.NumRows() != 5 {
		t.Fatal("non-pointer batch should pass through")
	}
}

func TestResolveExternalLocation_NilConfig(t *testing.T) {
	pointer, meta := MakeExternalLocationBatch(testSchema, "https://mock/test")
	defer pointer.Release()

	resolved, _, err := ResolveExternalLocation(pointer, meta, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.NumRows() != 0 {
		t.Fatal("nil config should pass through pointer batch unchanged")
	}
}

// ===========================================================================
// URL validation tests
// ===========================================================================

func TestHTTPSOnlyValidator_AcceptsHTTPS(t *testing.T) {
	if err := HTTPSOnlyValidator("https://example.com/data"); err != nil {
		t.Fatalf("should accept HTTPS: %v", err)
	}
}

func TestHTTPSOnlyValidator_RejectsHTTP(t *testing.T) {
	err := HTTPSOnlyValidator("http://example.com/data")
	if err == nil {
		t.Fatal("should reject HTTP")
	}
	if !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("error should mention HTTPS: %v", err)
	}
}

func TestHTTPSOnlyValidator_RejectsFTP(t *testing.T) {
	err := HTTPSOnlyValidator("ftp://example.com/data")
	if err == nil {
		t.Fatal("should reject FTP")
	}
}

func TestResolveExternalLocation_DefaultRejectsHTTP(t *testing.T) {
	pointer, meta := MakeExternalLocationBatch(testSchema, "http://insecure.example.com/data")
	defer pointer.Release()

	config := DefaultExternalLocationConfig(newMockStorage())
	_, _, err := ResolveExternalLocation(pointer, meta, config)
	if err == nil {
		t.Fatal("should reject HTTP URL with default validator")
	}
	if !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("error should mention HTTPS: %v", err)
	}
}

// ===========================================================================
// Compression tests
// ===========================================================================

func TestMaybeExternalizeBatch_WithCompression(t *testing.T) {
	storage := newMockStorage()
	config := &ExternalLocationConfig{
		Storage:                   storage,
		ExternalizeThresholdBytes: 10,
		Compression:               &Compression{Algorithm: "zstd", Level: 3},
	}
	batch := makeBatch(100)
	defer batch.Release()

	extBatch, _, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer extBatch.Release()

	if extBatch.NumRows() != 0 {
		t.Fatal("expected pointer batch")
	}
	if storage.lastContentEncoding != "zstd" {
		t.Fatalf("expected content encoding 'zstd', got %q", storage.lastContentEncoding)
	}

	// Verify uploaded data is zstd-compressed (magic number 0x28B52FFD)
	uploaded := storage.data[fmt.Sprintf("https://mock.storage/%d", storage.counter)]
	if len(uploaded) < 4 || uploaded[0] != 0x28 || uploaded[1] != 0xb5 || uploaded[2] != 0x2f || uploaded[3] != 0xfd {
		t.Fatal("uploaded data doesn't have zstd magic number")
	}
}

func TestMaybeExternalizeBatch_NoCompressionByDefault(t *testing.T) {
	storage := newMockStorage()
	config := &ExternalLocationConfig{
		Storage:                   storage,
		ExternalizeThresholdBytes: 10,
	}
	batch := makeBatch(100)
	defer batch.Release()

	_, _, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}

	uploaded := storage.data[fmt.Sprintf("https://mock.storage/%d", storage.counter)]
	// Not zstd compressed
	if len(uploaded) >= 4 && uploaded[0] == 0x28 && uploaded[1] == 0xb5 {
		t.Fatal("uploaded data should not be zstd-compressed by default")
	}
	if storage.lastContentEncoding != "" {
		t.Fatalf("expected empty content encoding, got %q", storage.lastContentEncoding)
	}
}

func TestCompressDecompressRoundtrip(t *testing.T) {
	storage := newMockStorage()
	config := &ExternalLocationConfig{
		Storage:                   storage,
		ExternalizeThresholdBytes: 10,
		Compression:               &Compression{Algorithm: "zstd", Level: 3},
		URLValidator:              nil,
	}
	batch := makeBatch(100)
	defer batch.Release()

	extBatch, extMeta, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer extBatch.Release()

	// Set up mock HTTP server serving compressed data
	locationURL, _ := metaGet(extMeta, MetaLocation)
	compressedData := storage.data[locationURL]

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "zstd")
		w.Write(compressedData)
	}))
	defer server.Close()

	// Resolve from test server
	pointer, meta := MakeExternalLocationBatch(testSchema, server.URL+"/data")
	defer pointer.Release()

	resolveConfig := &ExternalLocationConfig{
		URLValidator: nil,
	}
	resolved, _, err := ResolveExternalLocation(pointer, meta, resolveConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer resolved.Release()

	if resolved.NumRows() != 100 {
		t.Fatalf("expected 100 rows after decompress, got %d", resolved.NumRows())
	}
}

// ===========================================================================
// Full round-trip test (externalize → serve → resolve)
// ===========================================================================

func TestFullRoundtrip(t *testing.T) {
	storage := newMockStorage()

	// Start HTTP server that serves from mock storage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract the storage key from the request path
		for url, data := range storage.data {
			if strings.HasSuffix(url, r.URL.Path) {
				if storage.lastContentEncoding != "" {
					w.Header().Set("Content-Encoding", storage.lastContentEncoding)
				}
				w.Write(data)
				return
			}
		}
		w.WriteHeader(404)
	}))
	defer server.Close()

	// Override storage to use test server URLs
	testStorage := &redirectStorage{
		inner:      storage,
		serverURL:  server.URL,
	}

	config := &ExternalLocationConfig{
		Storage:                   testStorage,
		ExternalizeThresholdBytes: 10,
		URLValidator:              nil,
	}

	// Externalize a batch
	batch := makeBatch(50)
	defer batch.Release()

	extBatch, extMeta, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer extBatch.Release()

	if extBatch.NumRows() != 0 {
		t.Fatal("expected pointer batch")
	}

	// Resolve
	resolved, _, err := ResolveExternalLocation(extBatch, extMeta, config)
	if err != nil {
		t.Fatal(err)
	}
	defer resolved.Release()

	if resolved.NumRows() != 50 {
		t.Fatalf("expected 50 rows, got %d", resolved.NumRows())
	}
}

// redirectStorage wraps mockStorage to use a test server URL.
type redirectStorage struct {
	inner     *mockStorage
	serverURL string
}

func (r *redirectStorage) Upload(data []byte, schema *arrow.Schema, contentEncoding string) (string, error) {
	url, err := r.inner.Upload(data, schema, contentEncoding)
	if err != nil {
		return "", err
	}
	// Replace https://mock.storage/ with test server URL
	path := strings.TrimPrefix(url, "https://mock.storage")
	return r.serverURL + path, nil
}

// ===========================================================================
// SHA-256 checksum tests
// ===========================================================================

func TestMaybeExternalizeBatch_SHA256Present(t *testing.T) {
	storage := newMockStorage()
	config := &ExternalLocationConfig{
		Storage:                   storage,
		ExternalizeThresholdBytes: 10,
	}
	batch := makeBatch(100)
	defer batch.Release()

	_, extMeta, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}

	sha256Val, ok := metaGet(extMeta, MetaLocationSHA256)
	if !ok {
		t.Fatal("expected MetaLocationSHA256 key on pointer batch")
	}
	if len(sha256Val) != 64 {
		t.Fatalf("expected 64-char hex SHA-256, got %d chars: %q", len(sha256Val), sha256Val)
	}

	// Verify it matches the raw IPC bytes that were uploaded (no compression)
	locationURL, _ := metaGet(extMeta, MetaLocation)
	uploaded := storage.data[locationURL]
	hash := sha256.Sum256(uploaded)
	expectedHex := hex.EncodeToString(hash[:])
	if sha256Val != expectedHex {
		t.Fatalf("SHA-256 mismatch: metadata=%s, computed=%s", sha256Val, expectedHex)
	}
}

func TestResolveExternalLocation_SHA256Match(t *testing.T) {
	// Create a batch, serialize to IPC, compute SHA-256, serve it
	dataBatch := makeBatch(10)
	defer dataBatch.Release()
	ipcBytes := serializeTestIPC(dataBatch)

	hash := sha256.Sum256(ipcBytes)
	sha256Hex := hex.EncodeToString(hash[:])

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(ipcBytes)
	}))
	defer server.Close()

	pointer, meta := MakeExternalLocationBatch(testSchema, server.URL+"/data", sha256Hex)
	defer pointer.Release()

	config := &ExternalLocationConfig{URLValidator: nil}
	resolved, _, err := ResolveExternalLocation(pointer, meta, config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	defer resolved.Release()

	if resolved.NumRows() != 10 {
		t.Fatalf("expected 10 rows, got %d", resolved.NumRows())
	}
}

func TestResolveExternalLocation_SHA256Mismatch(t *testing.T) {
	dataBatch := makeBatch(10)
	defer dataBatch.Release()
	ipcBytes := serializeTestIPC(dataBatch)

	// Use a wrong SHA-256
	wrongSHA := "0000000000000000000000000000000000000000000000000000000000000000"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(ipcBytes)
	}))
	defer server.Close()

	pointer, meta := MakeExternalLocationBatch(testSchema, server.URL+"/data", wrongSHA)
	defer pointer.Release()

	config := &ExternalLocationConfig{URLValidator: nil}
	_, _, err := ResolveExternalLocation(pointer, meta, config)
	if err == nil {
		t.Fatal("expected checksum mismatch error")
	}
	if !strings.Contains(err.Error(), "SHA-256 checksum mismatch") {
		t.Fatalf("expected SHA-256 mismatch error, got: %v", err)
	}
}

func TestResolveExternalLocation_SHA256Absent(t *testing.T) {
	// Old-style pointer batch without SHA-256 metadata should still work
	dataBatch := makeBatch(10)
	defer dataBatch.Release()
	ipcBytes := serializeTestIPC(dataBatch)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(ipcBytes)
	}))
	defer server.Close()

	// No sha256Hex argument — old-style pointer
	pointer, meta := MakeExternalLocationBatch(testSchema, server.URL+"/data")
	defer pointer.Release()

	// Confirm no SHA-256 in metadata
	_, hasSHA := metaGet(meta, MetaLocationSHA256)
	if hasSHA {
		t.Fatal("expected no MetaLocationSHA256 on old-style pointer batch")
	}

	config := &ExternalLocationConfig{URLValidator: nil}
	resolved, _, err := ResolveExternalLocation(pointer, meta, config)
	if err != nil {
		t.Fatalf("expected no error for absent SHA-256, got: %v", err)
	}
	defer resolved.Release()

	if resolved.NumRows() != 10 {
		t.Fatalf("expected 10 rows, got %d", resolved.NumRows())
	}
}

func TestSHA256_RoundtripWithCompression(t *testing.T) {
	// SHA-256 is of raw IPC bytes (pre-compression), verified after fetch (post-decompression)
	storage := newMockStorage()
	config := &ExternalLocationConfig{
		Storage:                   storage,
		ExternalizeThresholdBytes: 10,
		Compression:              &Compression{Algorithm: "zstd", Level: 3},
		URLValidator:             nil,
	}
	batch := makeBatch(100)
	defer batch.Release()

	extBatch, extMeta, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer extBatch.Release()

	// Verify SHA-256 is present
	sha256Val, ok := metaGet(extMeta, MetaLocationSHA256)
	if !ok {
		t.Fatal("expected SHA-256 on compressed externalized batch")
	}
	if len(sha256Val) != 64 {
		t.Fatalf("expected 64-char hex, got %d", len(sha256Val))
	}

	// Serve compressed data with Content-Encoding: zstd
	locationURL, _ := metaGet(extMeta, MetaLocation)
	compressedData := storage.data[locationURL]

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "zstd")
		w.Write(compressedData)
	}))
	defer server.Close()

	// Resolve from test server
	pointer, meta := MakeExternalLocationBatch(testSchema, server.URL+"/data", sha256Val)
	defer pointer.Release()

	resolveConfig := &ExternalLocationConfig{URLValidator: nil}
	resolved, _, err := ResolveExternalLocation(pointer, meta, resolveConfig)
	if err != nil {
		t.Fatalf("round-trip with compression failed: %v", err)
	}
	defer resolved.Release()

	if resolved.NumRows() != 100 {
		t.Fatalf("expected 100 rows, got %d", resolved.NumRows())
	}
}

func TestSHA256_IsPreCompression(t *testing.T) {
	// SHA-256 should be of raw IPC bytes, NOT compressed bytes
	storage := newMockStorage()
	config := &ExternalLocationConfig{
		Storage:                   storage,
		ExternalizeThresholdBytes: 10,
		Compression:              &Compression{Algorithm: "zstd", Level: 3},
	}
	batch := makeBatch(100)
	defer batch.Release()

	_, extMeta, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}

	sha256Val, _ := metaGet(extMeta, MetaLocationSHA256)
	locationURL, _ := metaGet(extMeta, MetaLocation)
	compressedData := storage.data[locationURL]

	// SHA-256 should NOT match the compressed bytes
	compressedHash := sha256.Sum256(compressedData)
	compressedHex := hex.EncodeToString(compressedHash[:])
	if sha256Val == compressedHex {
		t.Fatal("SHA-256 should be of raw IPC, not compressed bytes")
	}

	// Decompress and verify SHA-256 matches the raw IPC
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	rawIPC, err := decoder.DecodeAll(compressedData, nil)
	if err != nil {
		t.Fatal(err)
	}
	rawHash := sha256.Sum256(rawIPC)
	rawHex := hex.EncodeToString(rawHash[:])
	if sha256Val != rawHex {
		t.Fatalf("SHA-256 mismatch: metadata=%s, raw IPC=%s", sha256Val, rawHex)
	}
}

func TestSHA256_FullExternalizeServeResolve(t *testing.T) {
	// Full round-trip: externalize → mock HTTP serve → resolve with checksum verification
	storage := newMockStorage()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for url, data := range storage.data {
			if strings.HasSuffix(url, r.URL.Path) {
				w.Write(data)
				return
			}
		}
		w.WriteHeader(404)
	}))
	defer server.Close()

	testStorage := &redirectStorage{inner: storage, serverURL: server.URL}
	config := &ExternalLocationConfig{
		Storage:                   testStorage,
		ExternalizeThresholdBytes: 10,
		URLValidator:              nil,
	}

	batch := makeBatch(50)
	defer batch.Release()

	extBatch, extMeta, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer extBatch.Release()

	// Verify SHA-256 is present
	_, hasSHA := metaGet(extMeta, MetaLocationSHA256)
	if !hasSHA {
		t.Fatal("missing SHA-256 on pointer batch")
	}

	// Resolve — should verify checksum automatically
	resolved, _, err := ResolveExternalLocation(extBatch, extMeta, config)
	if err != nil {
		t.Fatalf("full round-trip failed: %v", err)
	}
	defer resolved.Release()

	if resolved.NumRows() != 50 {
		t.Fatalf("expected 50 rows, got %d", resolved.NumRows())
	}
}

func TestSHA256_MismatchOnFullRoundtrip(t *testing.T) {
	// Full round-trip with tampered SHA-256 should fail
	storage := newMockStorage()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for url, data := range storage.data {
			if strings.HasSuffix(url, r.URL.Path) {
				w.Write(data)
				return
			}
		}
		w.WriteHeader(404)
	}))
	defer server.Close()

	testStorage := &redirectStorage{inner: storage, serverURL: server.URL}
	config := &ExternalLocationConfig{
		Storage:                   testStorage,
		ExternalizeThresholdBytes: 10,
		URLValidator:              nil,
	}

	batch := makeBatch(50)
	defer batch.Release()

	extBatch, extMeta, err := MaybeExternalizeBatch(batch, arrow.Metadata{}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer extBatch.Release()

	// Tamper with the SHA-256
	locationURL, _ := metaGet(extMeta, MetaLocation)
	tamperedMeta := arrow.NewMetadata(
		[]string{MetaLocation, MetaLocationSHA256},
		[]string{locationURL, "0000000000000000000000000000000000000000000000000000000000000000"},
	)

	_, _, err = ResolveExternalLocation(extBatch, tamperedMeta, config)
	if err == nil {
		t.Fatal("expected checksum mismatch error")
	}
	if !strings.Contains(err.Error(), "SHA-256 checksum mismatch") {
		t.Fatalf("expected SHA-256 mismatch error, got: %v", err)
	}
}

// Ensure interfaces are satisfied
var _ ExternalStorage = (*mockStorage)(nil)
var _ ExternalStorage = (*redirectStorage)(nil)

// Suppress unused import warnings
var _ = zstd.SpeedDefault
