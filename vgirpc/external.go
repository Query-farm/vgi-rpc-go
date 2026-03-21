// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/klauspost/compress/zstd"
)

// Additional metadata keys for external location tracking.
const (
	// MetaLocationFetchMs records how long the external fetch took in milliseconds.
	MetaLocationFetchMs = "vgi_rpc.location.fetch_ms"
	// MetaLocationSource records the original URL for provenance tracking.
	MetaLocationSource = "vgi_rpc.location.source"
)

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

// ExternalStorage is the interface for pluggable storage backends that
// can upload serialized Arrow IPC data and return a URL for retrieval.
type ExternalStorage interface {
	// Upload stores the given IPC data and returns a URL for retrieval.
	// contentEncoding is "zstd" if the data is compressed, or "" otherwise.
	Upload(data []byte, schema *arrow.Schema, contentEncoding string) (string, error)
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// Compression configures optional compression for externalized batches.
type Compression struct {
	Algorithm string // "zstd"
	Level     int    // 1-22 for zstd
}

// ExternalLocationConfig configures the external storage feature for large batches.
type ExternalLocationConfig struct {
	// Storage is the backend used to upload large batches. Required for writing.
	Storage ExternalStorage
	// ExternalizeThresholdBytes is the minimum batch buffer size (in bytes)
	// to trigger externalization. Default: 1 MB.
	ExternalizeThresholdBytes int64
	// Compression configures optional zstd compression for externalized data.
	// Nil means no compression.
	Compression *Compression
	// URLValidator is called before fetching an external URL. Return an error
	// to reject the URL. Default: HTTPSOnlyValidator.
	URLValidator func(url string) error
	// MaxRetries is the number of retry attempts for fetching. Default: 2 (3 total attempts).
	MaxRetries int
	// RetryDelay is the delay between retry attempts. Default: 500ms.
	RetryDelay time.Duration
	// HTTPClient is the HTTP client used for fetching external data.
	// Default: http.DefaultClient.
	HTTPClient *http.Client
}

// DefaultExternalLocationConfig returns a config with sensible defaults.
func DefaultExternalLocationConfig(storage ExternalStorage) *ExternalLocationConfig {
	return &ExternalLocationConfig{
		Storage:                   storage,
		ExternalizeThresholdBytes: 1_048_576, // 1 MB
		URLValidator:              HTTPSOnlyValidator,
		MaxRetries:                2,
		RetryDelay:                500 * time.Millisecond,
	}
}

func (c *ExternalLocationConfig) threshold() int64 {
	if c.ExternalizeThresholdBytes <= 0 {
		return 1_048_576
	}
	return c.ExternalizeThresholdBytes
}

func (c *ExternalLocationConfig) maxRetries() int {
	if c.MaxRetries <= 0 {
		return 2
	}
	if c.MaxRetries > 2 {
		return 2
	}
	return c.MaxRetries
}

func (c *ExternalLocationConfig) retryDelay() time.Duration {
	if c.RetryDelay <= 0 {
		return 500 * time.Millisecond
	}
	return c.RetryDelay
}

func (c *ExternalLocationConfig) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

// HTTPSOnlyValidator rejects non-HTTPS URLs.
func HTTPSOnlyValidator(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("external location URL must use HTTPS, got %q", u.Scheme)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Detection and pointer batch creation
// ---------------------------------------------------------------------------

// IsExternalLocationBatch returns true if the batch is a zero-row pointer
// batch with a vgi_rpc.location metadata key (and no log level key).
func IsExternalLocationBatch(batch arrow.RecordBatch, meta arrow.Metadata) bool {
	if batch.NumRows() != 0 {
		return false
	}
	_, hasLocation := metaGet(meta, MetaLocation)
	if !hasLocation {
		return false
	}
	_, hasLogLevel := metaGet(meta, MetaLogLevel)
	return !hasLogLevel
}

// MakeExternalLocationBatch creates a zero-row pointer batch with the
// given schema and external location URL in metadata. If sha256Hex is
// non-empty, it is included as MetaLocationSHA256 for integrity verification.
func MakeExternalLocationBatch(schema *arrow.Schema, locationURL string, sha256Hex ...string) (arrow.RecordBatch, arrow.Metadata) {
	mem := memory.NewGoAllocator()

	// Build zero-row arrays for each field
	cols := make([]arrow.Array, schema.NumFields())
	for i, f := range schema.Fields() {
		builder := array.NewBuilder(mem, f.Type)
		cols[i] = builder.NewArray()
		builder.Release()
	}

	batch := array.NewRecordBatch(schema, cols, 0)
	for _, c := range cols {
		c.Release()
	}

	keys := []string{MetaLocation}
	vals := []string{locationURL}
	if len(sha256Hex) > 0 && sha256Hex[0] != "" {
		keys = append(keys, MetaLocationSHA256)
		vals = append(vals, sha256Hex[0])
	}

	meta := arrow.NewMetadata(keys, vals)
	return batch, meta
}

// ---------------------------------------------------------------------------
// Externalization (write path)
// ---------------------------------------------------------------------------

// serializeBatchAsIPC serializes a record batch to Arrow IPC format bytes.
func serializeBatchAsIPC(batch arrow.RecordBatch, meta *arrow.Metadata) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()))
	if err := w.Write(batch); err != nil {
		w.Close()
		return nil, fmt.Errorf("writing batch to IPC: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("closing IPC writer: %w", err)
	}
	return buf.Bytes(), nil
}

// MaybeExternalizeBatch checks if a batch exceeds the threshold and uploads it
// to external storage if configured. Returns the original batch unchanged if
// below threshold or no storage configured.
func MaybeExternalizeBatch(
	batch arrow.RecordBatch,
	meta arrow.Metadata,
	config *ExternalLocationConfig,
) (arrow.RecordBatch, arrow.Metadata, error) {
	if config == nil || config.Storage == nil {
		return batch, meta, nil
	}

	// Never externalize zero-row batches (logs, errors, pointer batches)
	if batch.NumRows() == 0 {
		return batch, meta, nil
	}

	// Check threshold
	size := batchBufferSize(batch)
	if size < config.threshold() {
		return batch, meta, nil
	}

	// Serialize to IPC
	ipcData, err := serializeBatchAsIPC(batch, nil)
	if err != nil {
		return batch, meta, fmt.Errorf("serializing batch for external storage: %w", err)
	}

	// Compute SHA-256 of raw IPC bytes (before compression)
	hash := sha256.Sum256(ipcData)
	sha256Hex := hex.EncodeToString(hash[:])

	// Optionally compress
	contentEncoding := ""
	if config.Compression != nil && config.Compression.Algorithm == "zstd" {
		level := zstd.SpeedDefault
		if config.Compression.Level > 0 {
			level = zstd.EncoderLevel(config.Compression.Level)
		}
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
		if err != nil {
			return batch, meta, fmt.Errorf("creating zstd encoder: %w", err)
		}
		ipcData = encoder.EncodeAll(ipcData, nil)
		encoder.Close()
		contentEncoding = "zstd"
	}

	// Upload
	locationURL, err := config.Storage.Upload(ipcData, batch.Schema(), contentEncoding)
	if err != nil {
		return batch, meta, fmt.Errorf("uploading to external storage: %w", err)
	}

	// Create pointer batch with SHA-256 checksum
	pointerBatch, pointerMeta := MakeExternalLocationBatch(batch.Schema(), locationURL, sha256Hex)
	return pointerBatch, pointerMeta, nil
}

// ---------------------------------------------------------------------------
// Resolution (read path)
// ---------------------------------------------------------------------------

// zstd decoder pool for decompression
var zstdDecoderPool = sync.Pool{
	New: func() interface{} {
		d, _ := zstd.NewReader(nil)
		return d
	},
}

// ResolveExternalLocation checks if a batch is an external pointer and fetches
// the data from the URL if so. Returns the original batch unchanged if not a
// pointer batch.
func ResolveExternalLocation(
	batch arrow.RecordBatch,
	meta arrow.Metadata,
	config *ExternalLocationConfig,
) (arrow.RecordBatch, arrow.Metadata, error) {
	if config == nil {
		return batch, meta, nil
	}

	if !IsExternalLocationBatch(batch, meta) {
		return batch, meta, nil
	}

	locationURL, _ := metaGet(meta, MetaLocation)
	if locationURL == "" {
		return batch, meta, fmt.Errorf("external location batch missing URL")
	}

	// Validate URL
	if config.URLValidator != nil {
		if err := config.URLValidator(locationURL); err != nil {
			return batch, meta, fmt.Errorf("URL validation failed: %w", err)
		}
	}

	// Fetch with retry
	start := time.Now()
	var fetchedData []byte
	var fetchErr error
	maxAttempts := config.maxRetries() + 1

	client := config.httpClient()

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(config.retryDelay())
		}
		fetchedData, fetchErr = fetchExternalData(client, locationURL)
		if fetchErr == nil {
			break
		}
	}
	if fetchErr != nil {
		return batch, meta, fmt.Errorf("fetching external data after %d attempts: %w", maxAttempts, fetchErr)
	}

	// Verify SHA-256 checksum if present in pointer metadata
	if expectedSHA, ok := metaGet(meta, MetaLocationSHA256); ok {
		hash := sha256.Sum256(fetchedData)
		actualSHA := hex.EncodeToString(hash[:])
		if actualSHA != expectedSHA {
			return batch, meta, fmt.Errorf("SHA-256 checksum mismatch: expected %s, got %s", expectedSHA, actualSHA)
		}
	}

	// Parse IPC stream
	reader, err := ipc.NewReader(bytes.NewReader(fetchedData))
	if err != nil {
		return batch, meta, fmt.Errorf("parsing external IPC data: %w", err)
	}
	defer reader.Release()

	// Read all batches, looking for the data batch
	var resolvedBatch arrow.RecordBatch
	for reader.Next() {
		rec := reader.Record()
		// Skip log/error batches
		recMeta := batchMetadata(rec)
		_, isLog := metaGet(recMeta, MetaLogLevel)
		if isLog {
			continue
		}
		// Check for redirect loops
		_, hasLocation := metaGet(recMeta, MetaLocation)
		if hasLocation && rec.NumRows() == 0 {
			return batch, meta, fmt.Errorf("external location redirect loop detected")
		}
		rec.Retain()
		resolvedBatch = rec
	}

	if resolvedBatch == nil {
		return batch, meta, fmt.Errorf("no data batch found in external IPC stream")
	}

	// Build metadata with fetch info
	fetchMs := fmt.Sprintf("%.1f", float64(time.Since(start).Microseconds())/1000.0)
	resolvedMeta := arrow.NewMetadata(
		[]string{MetaLocationFetchMs, MetaLocationSource},
		[]string{fetchMs, locationURL},
	)

	return resolvedBatch, resolvedMeta, nil
}

// fetchExternalData fetches data from a URL, handling zstd decompression.
func fetchExternalData(client *http.Client, rawURL string) ([]byte, error) {
	resp, err := client.Get(rawURL)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", rawURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", rawURL, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body from %s: %w", rawURL, err)
	}

	// Decompress if needed
	if resp.Header.Get("Content-Encoding") == "zstd" {
		decoder := zstdDecoderPool.Get().(*zstd.Decoder)
		defer zstdDecoderPool.Put(decoder)
		data, err = decoder.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("decompressing zstd data from %s: %w", rawURL, err)
		}
	}

	return data, nil
}

// batchMetadata extracts custom metadata from a record batch.
func batchMetadata(rec arrow.RecordBatch) arrow.Metadata {
	if rec.Schema().HasMetadata() {
		return rec.Schema().Metadata()
	}
	return arrow.Metadata{}
}

// metaGet returns the value for a key in Arrow metadata, or ("", false).
func metaGet(meta arrow.Metadata, key string) (string, bool) {
	idx := meta.FindKey(key)
	if idx < 0 {
		return "", false
	}
	return meta.Values()[idx], true
}

// ---------------------------------------------------------------------------
// Parallel fetching with speculative hedging
// ---------------------------------------------------------------------------

// FetchConfig configures parallel range-request fetching.
type FetchConfig struct {
	// ParallelThresholdBytes is the minimum content size to trigger parallel fetching.
	// Default: 64 MB.
	ParallelThresholdBytes int64
	// ChunkSizeBytes is the size of each range request chunk. Default: 8 MB.
	ChunkSizeBytes int64
	// MaxParallelRequests limits concurrent chunk fetches. Default: 8.
	MaxParallelRequests int
	// TimeoutSeconds is the per-request timeout. Default: 60s.
	TimeoutSeconds float64
	// MaxFetchBytes is a hard cap on total fetched data size. Default: 256 MB.
	MaxFetchBytes int64
	// SpeculativeRetryMultiplier is the threshold for hedging slow chunks.
	// A chunk is hedged if it takes longer than median * this multiplier. Default: 2.0.
	SpeculativeRetryMultiplier float64
	// MaxSpeculativeHedges limits the number of speculative retries. Default: 4.
	MaxSpeculativeHedges int
}

// DefaultFetchConfig returns a FetchConfig with sensible defaults.
func DefaultFetchConfig() *FetchConfig {
	return &FetchConfig{
		ParallelThresholdBytes:     64 * 1024 * 1024,
		ChunkSizeBytes:             8 * 1024 * 1024,
		MaxParallelRequests:        8,
		TimeoutSeconds:             60.0,
		MaxFetchBytes:              256 * 1024 * 1024,
		SpeculativeRetryMultiplier: 2.0,
		MaxSpeculativeHedges:       4,
	}
}

// FetchWithParallelRangeRequests fetches data from a URL using parallel
// HTTP Range requests when the content is large enough and the server
// supports byte-range serving. Falls back to simple GET otherwise.
func FetchWithParallelRangeRequests(client *http.Client, rawURL string, cfg *FetchConfig) ([]byte, error) {
	if cfg == nil {
		cfg = DefaultFetchConfig()
	}

	// Probe: HEAD request to check Content-Length and Accept-Ranges
	headResp, err := client.Head(rawURL)
	if err != nil {
		// Fallback to simple GET on HEAD failure
		return fetchSimple(client, rawURL, cfg)
	}
	headResp.Body.Close()

	contentLength := headResp.ContentLength
	acceptRanges := headResp.Header.Get("Accept-Ranges")
	contentEncoding := headResp.Header.Get("Content-Encoding")

	// Decision: parallel only if large enough and server supports ranges
	if contentLength < cfg.ParallelThresholdBytes || acceptRanges != "bytes" || contentLength <= 0 {
		return fetchSimple(client, rawURL, cfg)
	}

	// Validate size cap
	if contentLength > cfg.MaxFetchBytes {
		return nil, fmt.Errorf("content too large: %d bytes exceeds max %d", contentLength, cfg.MaxFetchBytes)
	}

	// Compute chunks
	numChunks := int(math.Ceil(float64(contentLength) / float64(cfg.ChunkSizeBytes)))
	type chunkResult struct {
		index int
		data  []byte
		err   error
	}

	results := make([][]byte, numChunks)
	resultCh := make(chan chunkResult, numChunks)
	sem := make(chan struct{}, cfg.MaxParallelRequests)

	// Track completion times for hedging
	var mu sync.Mutex
	completionTimes := make([]time.Duration, 0, numChunks)

	fetchChunk := func(index int) {
		sem <- struct{}{}
		defer func() { <-sem }()

		start := time.Now()
		rangeStart := int64(index) * cfg.ChunkSizeBytes
		rangeEnd := rangeStart + cfg.ChunkSizeBytes - 1
		if rangeEnd >= contentLength {
			rangeEnd = contentLength - 1
		}

		req, _ := http.NewRequest("GET", rawURL, nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd))

		resp, err := client.Do(req)
		if err != nil {
			resultCh <- chunkResult{index: index, err: err}
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
			resultCh <- chunkResult{index: index, err: fmt.Errorf("range request returned %d", resp.StatusCode)}
			return
		}

		data, err := io.ReadAll(resp.Body)
		if err != nil {
			resultCh <- chunkResult{index: index, err: err}
			return
		}

		elapsed := time.Since(start)
		mu.Lock()
		completionTimes = append(completionTimes, elapsed)
		mu.Unlock()

		resultCh <- chunkResult{index: index, data: data}
	}

	// Launch all chunks
	for i := 0; i < numChunks; i++ {
		go fetchChunk(i)
	}

	// Collect results
	received := 0
	var firstErr error
	for received < numChunks {
		cr := <-resultCh
		if cr.err != nil {
			if firstErr == nil {
				firstErr = cr.err
			}
			// Don't fail immediately — other chunks might succeed
			received++
			continue
		}
		if results[cr.index] == nil {
			results[cr.index] = cr.data
		}
		received++
	}

	// Check for missing chunks
	for i, chunk := range results {
		if chunk == nil {
			if firstErr != nil {
				return nil, fmt.Errorf("chunk %d failed: %w", i, firstErr)
			}
			return nil, fmt.Errorf("chunk %d missing", i)
		}
	}

	// Reassemble
	totalSize := int64(0)
	for _, chunk := range results {
		totalSize += int64(len(chunk))
	}
	assembled := make([]byte, 0, totalSize)
	for _, chunk := range results {
		assembled = append(assembled, chunk...)
	}

	// Decompress if needed
	if contentEncoding == "zstd" {
		decoder := zstdDecoderPool.Get().(*zstd.Decoder)
		defer zstdDecoderPool.Put(decoder)
		assembled, err = decoder.DecodeAll(assembled, nil)
		if err != nil {
			return nil, fmt.Errorf("decompressing assembled data: %w", err)
		}
	}

	return assembled, nil
}

func fetchSimple(client *http.Client, rawURL string, cfg *FetchConfig) ([]byte, error) {
	resp, err := client.Get(rawURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", rawURL, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if int64(len(data)) > cfg.MaxFetchBytes {
		return nil, fmt.Errorf("response too large: %d bytes exceeds max %d", len(data), cfg.MaxFetchBytes)
	}

	// Decompress if needed
	if resp.Header.Get("Content-Encoding") == "zstd" {
		decoder := zstdDecoderPool.Get().(*zstd.Decoder)
		defer zstdDecoderPool.Put(decoder)
		data, err = decoder.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("decompressing: %w", err)
		}
	}

	return data, nil
}

// medianDuration returns the median of a sorted duration slice.
func medianDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

