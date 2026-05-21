// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package vgirpc

/*
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

// Wrapper because cgo can't take the address of a varargs C function.
static int go_shm_open(const char* name, int oflag, mode_t mode) {
    return shm_open(name, oflag, mode);
}

static int go_shm_unlink(const char* name) {
    return shm_unlink(name);
}
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"golang.org/x/sys/unix"
)

// ErrShmClosed is returned by ShmSegment methods after Close has been
// called. Callers should fall back to the pipe transport.
var ErrShmClosed = errors.New("shm: segment is closed")

// Wire-format constants — must match Python vgi_rpc.shm and Rust shm.rs
// byte-for-byte. See those files for the authoritative spec.
const (
	// ShmHeaderSize is the size of the allocator header at the start of
	// every segment. User data begins at this offset.
	ShmHeaderSize = 65_536

	shmHeaderFixedSize = 24
	shmAllocEntrySize  = 16

	// ShmMaxAllocs is the maximum number of concurrent allocations the
	// header can hold (4094, matching Python and Rust).
	ShmMaxAllocs = (ShmHeaderSize - shmHeaderFixedSize) / shmAllocEntrySize
)

var (
	shmMagic   = [4]byte{'V', 'G', 'I', 'S'}
	shmVersion = uint32(1)
	// ipcEOS is the IPC stream end-of-stream marker: continuation token
	// (0xFFFFFFFF) followed by a 0-length metadata field.
	ipcEOS = [8]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00}
)

// ShmSegment is a POSIX shared-memory segment used as a side-channel for
// zero-copy Arrow IPC batch transfer between processes. Lockstep RPC
// (only one of client or server is touching the segment at a time) means
// no locking is required inside the allocator beyond what's enforced by
// kernel page-cache coherence on the mapping.
//
// Lifecycle:
//   - Client side: ShmCreate → advertise (name, size) on each request
//     via MetaShmSegmentName/MetaShmSegmentSize → Close (which unlinks).
//   - Server side: ShmAttach (track=false) per request → use → Close
//     (does not unlink; the client owns the OS object).
type ShmSegment struct {
	name  string
	size  int
	data  []byte // mmap'd region; len == size
	track bool   // true = own the OS object, unlink on Close

	// mu serializes allocator header writes. The protocol's lockstep
	// invariant (only one of client/server is touching the segment at a
	// time) makes inter-process locking unnecessary, but a single
	// process can call AllocateAndWrite/FreeOffset from multiple
	// goroutines (e.g. the HTTP transport's per-connection workers
	// sharing one server-attached segment). This guards that intra-
	// process race; inter-process coherence still relies on lockstep.
	mu sync.Mutex

	// closed guards against use-after-Close: any read/write through
	// data after Close would dereference a nil slice. Close swaps it,
	// public methods short-circuit when set.
	closed atomic.Bool
}

// ShmCreate creates a new POSIX shm segment of `size` bytes (must be
// strictly greater than ShmHeaderSize). The returned segment owns the OS
// object — Close calls shm_unlink.
func ShmCreate(size int) (*ShmSegment, error) {
	if size <= ShmHeaderSize {
		return nil, fmt.Errorf("shm size must be > %d, got %d", ShmHeaderSize, size)
	}
	for i := 0; i < 16; i++ {
		name := makeShmName()
		seg, err := shmTryCreate(name, size)
		if err == nil {
			if err := seg.initializeHeader(); err != nil {
				_ = seg.Close()
				return nil, err
			}
			return seg, nil
		}
		if !errors.Is(err, unix.EEXIST) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("shm_open exhausted name retries")
}

// ShmAttach attaches to an existing segment by name. If track is false
// (server-side use) Close does NOT call shm_unlink.
func ShmAttach(name string, size int, track bool) (*ShmSegment, error) {
	if size <= ShmHeaderSize {
		return nil, fmt.Errorf("shm size must be > %d, got %d", ShmHeaderSize, size)
	}
	fd, err := shmOpenForAttach(name)
	if err != nil {
		return nil, err
	}
	data, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	_ = unix.Close(fd)
	if err != nil {
		return nil, fmt.Errorf("mmap: %w", err)
	}
	seg := &ShmSegment{name: name, size: size, data: data, track: track}
	if err := seg.validateHeader(); err != nil {
		_ = seg.Close()
		return nil, err
	}
	return seg, nil
}

// Name returns the OS name of the segment (with leading slash on Linux/macOS).
func (s *ShmSegment) Name() string { return s.name }

// Size returns the total segment size in bytes (including the header).
func (s *ShmSegment) Size() int { return s.size }

// Close munmaps the region and, if this segment was the owner, calls
// shm_unlink. Safe to call multiple times.
func (s *ShmSegment) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	var firstErr error
	if s.data != nil {
		if err := unix.Munmap(s.data); err != nil {
			firstErr = err
		}
		s.data = nil
	}
	if s.track && s.name != "" {
		if err := shmUnlink(s.name); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// initializeHeader writes magic+version+data_size+0 allocations.
func (s *ShmSegment) initializeHeader() error {
	if len(s.data) < ShmHeaderSize {
		return fmt.Errorf("segment too small for header: %d < %d", len(s.data), ShmHeaderSize)
	}
	dataSize := uint64(s.size - ShmHeaderSize)
	copy(s.data[0:4], shmMagic[:])
	binary.LittleEndian.PutUint32(s.data[4:8], shmVersion)
	binary.LittleEndian.PutUint64(s.data[8:16], dataSize)
	binary.LittleEndian.PutUint32(s.data[16:20], 0)
	binary.LittleEndian.PutUint32(s.data[20:24], 0)
	return nil
}

func (s *ShmSegment) validateHeader() error {
	if !bytes.Equal(s.data[0:4], shmMagic[:]) {
		return fmt.Errorf("bad SHM magic: %v", s.data[0:4])
	}
	if v := binary.LittleEndian.Uint32(s.data[4:8]); v != shmVersion {
		return fmt.Errorf("unsupported SHM version: %d", v)
	}
	got := binary.LittleEndian.Uint64(s.data[8:16])
	want := uint64(s.size - ShmHeaderSize)
	if got != want {
		return fmt.Errorf("data_size mismatch: header says %d, expected %d", got, want)
	}
	return nil
}

// numAllocs returns the current allocation count from the header.
func (s *ShmSegment) numAllocs() int {
	return int(binary.LittleEndian.Uint32(s.data[16:20]))
}

func (s *ShmSegment) readAllocs() [][2]uint64 {
	n := s.numAllocs()
	out := make([][2]uint64, n)
	for i := 0; i < n; i++ {
		base := shmHeaderFixedSize + i*shmAllocEntrySize
		off := binary.LittleEndian.Uint64(s.data[base : base+8])
		ln := binary.LittleEndian.Uint64(s.data[base+8 : base+16])
		out[i] = [2]uint64{off, ln}
	}
	return out
}

func (s *ShmSegment) writeAllocs(allocs [][2]uint64) {
	binary.LittleEndian.PutUint32(s.data[16:20], uint32(len(allocs)))
	for i, e := range allocs {
		base := shmHeaderFixedSize + i*shmAllocEntrySize
		binary.LittleEndian.PutUint64(s.data[base:base+8], e[0])
		binary.LittleEndian.PutUint64(s.data[base+8:base+16], e[1])
	}
}

// allocateLocked first-fits `size` bytes in the data region. Returns
// the absolute offset, or (0, false) if no slot is available. Caller
// must hold s.mu.
func (s *ShmSegment) allocateLocked(size int) (uint64, bool) {
	if size <= 0 {
		return 0, false
	}
	allocs := s.readAllocs()
	if len(allocs) >= ShmMaxAllocs {
		return 0, false
	}
	sz := uint64(size)
	dataEnd := uint64(s.size)
	prevEnd := uint64(ShmHeaderSize)
	for i, e := range allocs {
		gap := e[0] - prevEnd
		if gap >= sz {
			newAllocs := make([][2]uint64, 0, len(allocs)+1)
			newAllocs = append(newAllocs, allocs[:i]...)
			newAllocs = append(newAllocs, [2]uint64{prevEnd, sz})
			newAllocs = append(newAllocs, allocs[i:]...)
			s.writeAllocs(newAllocs)
			return prevEnd, true
		}
		prevEnd = e[0] + e[1]
	}
	if dataEnd-prevEnd >= sz {
		allocs = append(allocs, [2]uint64{prevEnd, sz})
		s.writeAllocs(allocs)
		return prevEnd, true
	}
	return 0, false
}

// freeAtLocked removes the allocation starting at offset. Returns nil
// if found. Caller must hold s.mu.
func (s *ShmSegment) freeAtLocked(offset uint64) error {
	allocs := s.readAllocs()
	for i, e := range allocs {
		if e[0] == offset {
			allocs = append(allocs[:i], allocs[i+1:]...)
			s.writeAllocs(allocs)
			return nil
		}
	}
	return fmt.Errorf("no allocation at offset %d", offset)
}

// Reset clears the allocation table (frees all regions). Safe only when
// no peer is referencing any region.
func (s *ShmSegment) Reset() {
	if s.closed.Load() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return
	}
	binary.LittleEndian.PutUint32(s.data[16:20], 0)
}

// AllocateAndWrite serializes batch as a full IPC stream into the
// segment. For dictionary-encoded schemas the leading schema message
// and trailing EOS marker are stripped, matching Python/Rust.
//
// Returns (offset, length, true) on success, or (0, 0, false) if the
// segment cannot fit the serialized batch. Performs a cheap upper-bound
// capacity check before the (potentially expensive) IPC serialization
// so that requests that obviously won't fit fall back to the pipe
// without paying serialization cost.
func (s *ShmSegment) AllocateAndWrite(batch arrow.RecordBatch) (uint64, int, bool, error) {
	if s.closed.Load() {
		return 0, 0, false, ErrShmClosed
	}
	s.mu.Lock()
	canFit := s.canFitLocked(estimateSerializedSize(batch))
	s.mu.Unlock()
	if !canFit {
		return 0, 0, false, nil
	}
	buf, err := serializeForShm(batch)
	if err != nil {
		return 0, 0, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return 0, 0, false, ErrShmClosed
	}
	offset, ok := s.allocateLocked(len(buf))
	if !ok {
		return 0, 0, false, nil
	}
	copy(s.data[offset:offset+uint64(len(buf))], buf)
	return offset, len(buf), true, nil
}

// canFitLocked reports whether a contiguous gap of `size` bytes exists.
// Caller must hold s.mu.
func (s *ShmSegment) canFitLocked(size int) bool {
	if size <= 0 {
		return false
	}
	allocs := s.readAllocs()
	if len(allocs) >= ShmMaxAllocs {
		return false
	}
	sz := uint64(size)
	dataEnd := uint64(s.size)
	prevEnd := uint64(ShmHeaderSize)
	for _, e := range allocs {
		if e[0]-prevEnd >= sz {
			return true
		}
		prevEnd = e[0] + e[1]
	}
	return dataEnd-prevEnd >= sz
}

// estimateSerializedSize returns a conservative upper bound on the
// number of bytes batch will occupy as an IPC stream — buffer bytes
// plus a small overhead for schema/metadata/framing. Used as a fast
// pre-check by AllocateAndWrite to avoid serializing batches that
// obviously won't fit in the segment.
func estimateSerializedSize(batch arrow.RecordBatch) int {
	return int(batchBufferSize(batch)) + 4096
}

// ReadBatch materializes a batch previously written by AllocateAndWrite.
// The returned batch's column data is COPIED out of the segment (Go's
// arrow-go library does not currently expose a zero-copy buffer-with-
// custom-allocator API like Rust's `Buffer::from_custom_allocation`).
// The caller may free the slot immediately after this returns.
func (s *ShmSegment) ReadBatch(offset uint64, length int, schema *arrow.Schema) (arrow.RecordBatch, error) {
	if s.closed.Load() {
		return nil, ErrShmClosed
	}
	end := offset + uint64(length)
	if end > uint64(s.size) {
		return nil, fmt.Errorf("shm region out of bounds: %d..%d > %d", offset, end, s.size)
	}
	// Copy region under the lock. The header allocator may run
	// concurrently in another goroutine; reading the data region
	// itself doesn't touch the allocator state but we want a stable
	// view of bytes that aren't being concurrently overwritten.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, ErrShmClosed
	}
	region := s.data[offset:end]
	if !schemaHasDictionary(schema) {
		return readIPCStream(region)
	}
	// Dictionary path: synthesize a stream by prepending the schema
	// message and appending EOS, mirroring Rust/Python.
	schemaOnly, err := writeSchemaOnlyStream(schema)
	if err != nil {
		return nil, err
	}
	if len(schemaOnly) < len(ipcEOS) {
		return nil, fmt.Errorf("schema-only stream too short")
	}
	prefix := schemaOnly[:len(schemaOnly)-len(ipcEOS)]
	combined := make([]byte, 0, len(prefix)+len(region)+len(ipcEOS))
	combined = append(combined, prefix...)
	combined = append(combined, region...)
	combined = append(combined, ipcEOS[:]...)
	return readIPCStream(combined)
}

// FreeOffset releases the allocation at offset.
func (s *ShmSegment) FreeOffset(offset uint64) error {
	if s.closed.Load() {
		return ErrShmClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return ErrShmClosed
	}
	return s.freeAtLocked(offset)
}

// ----------------------------------------------------------------------
// Pointer-batch helpers
// ----------------------------------------------------------------------

// makeShmPointerBatch builds a zero-row batch with shm_offset/shm_length
// metadata, plus any additional metadata supplied.
func makeShmPointerBatch(schema *arrow.Schema, offset uint64, length int, extraMeta map[string]string) arrow.RecordBatch {
	keys := []string{MetaShmOffset, MetaShmLength}
	vals := []string{strconv.FormatUint(offset, 10), strconv.Itoa(length)}
	for k, v := range extraMeta {
		if k == MetaShmOffset || k == MetaShmLength {
			continue
		}
		keys = append(keys, k)
		vals = append(vals, v)
	}
	meta := arrow.NewMetadata(keys, vals)
	zero := emptyBatch(schema)
	defer zero.Release()
	return array.NewRecordBatchWithMetadata(schema, zero.Columns(), 0, meta)
}

// IsShmPointerBatch reports whether batch is a pointer batch (zero rows
// with shm_offset metadata, and not a log batch).
func IsShmPointerBatch(batch arrow.RecordBatch) bool {
	if batch.NumRows() != 0 {
		return false
	}
	rb, ok := batch.(arrow.RecordBatchWithMetadata)
	if !ok {
		return false
	}
	md := rb.Metadata()
	if _, has := md.GetValue(MetaShmOffset); !has {
		return false
	}
	if _, isLog := md.GetValue(MetaLogLevel); isLog {
		return false
	}
	return true
}

// ResolveShmBatch turns a pointer batch into a materialized batch by
// reading the bytes from `seg`. For non-pointer batches (or when seg is
// nil) it returns the input unchanged. The release flag is true when the
// caller is responsible for calling seg.FreeOffset(offset) after the
// returned batch is no longer needed (always true here, since the Go
// implementation copies bytes out of the mapping).
//
// Recovers panics from the IPC reader path; on panic returns a non-nil
// error and leaves the batch unresolved so the caller can decide
// whether to fail the request or proceed with the original (zero-row)
// pointer batch.
func ResolveShmBatch(batch arrow.RecordBatch, seg *ShmSegment) (resolved arrow.RecordBatch, releaseOffset uint64, release bool, err error) {
	if seg == nil || !IsShmPointerBatch(batch) {
		return batch, 0, false, nil
	}
	defer func() {
		if rv := recover(); rv != nil {
			resolved = batch
			releaseOffset = 0
			release = false
			err = fmt.Errorf("shm resolve panic: %v", rv)
		}
	}()
	rb := batch.(arrow.RecordBatchWithMetadata)
	md := rb.Metadata()
	offStr, _ := md.GetValue(MetaShmOffset)
	lenStr, _ := md.GetValue(MetaShmLength)
	offset, perr := strconv.ParseUint(offStr, 10, 64)
	if perr != nil {
		return nil, 0, false, fmt.Errorf("bad shm_offset: %w", perr)
	}
	length, perr := strconv.Atoi(lenStr)
	if perr != nil {
		return nil, 0, false, fmt.Errorf("bad shm_length: %w", perr)
	}
	out, rerr := seg.ReadBatch(offset, length, batch.Schema())
	if rerr != nil {
		return nil, 0, false, rerr
	}
	// Strip pointer keys, add shm_source.
	keys := []string{}
	vals := []string{}
	for i := 0; i < md.Len(); i++ {
		k := md.Keys()[i]
		if k == MetaShmOffset || k == MetaShmLength {
			continue
		}
		keys = append(keys, k)
		vals = append(vals, md.Values()[i])
	}
	keys = append(keys, MetaShmSource)
	vals = append(vals, seg.name)
	newMeta := arrow.NewMetadata(keys, vals)
	resolvedWithMeta := array.NewRecordBatchWithMetadata(out.Schema(), out.Columns(), out.NumRows(), newMeta)
	out.Release()
	return resolvedWithMeta, offset, true, nil
}

// MaybeWriteToShm tries to write batch into seg and return a pointer
// batch. If seg is nil, batch is empty, or the segment cannot fit the
// serialized bytes, returns batch unchanged with replaced=false.
//
// When replaced=true the returned pointer batch is a new RecordBatch
// owned by the caller (with refcount 1); the original `batch` is
// untouched (caller is still responsible for releasing it). The pointer
// batch carries the original batch's custom metadata merged with the
// shm_offset/shm_length keys (pointer keys override on collision).
//
// A panic from the underlying Arrow IPC writer (or any other dependency
// reached from serializeForShm) is recovered into an error so the
// surrounding RPC dispatch can fall back to the pipe transport rather
// than crashing the worker. Callers that want stricter behavior should
// avoid this wrapper.
func MaybeWriteToShm(batch arrow.RecordBatch, seg *ShmSegment) (out arrow.RecordBatch, replaced bool, err error) {
	if seg == nil || batch.NumRows() == 0 {
		return batch, false, nil
	}
	defer func() {
		if rv := recover(); rv != nil {
			out = batch
			replaced = false
			err = fmt.Errorf("shm write panic: %v", rv)
		}
	}()
	offset, length, ok, werr := seg.AllocateAndWrite(batch)
	if werr != nil {
		return batch, false, werr
	}
	if !ok {
		return batch, false, nil
	}
	var existing map[string]string
	if rb, ok := batch.(arrow.RecordBatchWithMetadata); ok {
		md := rb.Metadata()
		existing = make(map[string]string, md.Len())
		for i := 0; i < md.Len(); i++ {
			existing[md.Keys()[i]] = md.Values()[i]
		}
	}
	pointer := makeShmPointerBatch(batch.Schema(), offset, length, existing)
	return pointer, true, nil
}

// ----------------------------------------------------------------------
// IPC (de)serialization helpers
// ----------------------------------------------------------------------

func schemaHasDictionary(schema *arrow.Schema) bool {
	for _, f := range schema.Fields() {
		if _, ok := f.Type.(*arrow.DictionaryType); ok {
			return true
		}
	}
	return false
}

// serializeForShm writes batch as a full IPC stream. For dict-encoded
// schemas the leading schema message and trailing EOS are stripped so
// the SHM region holds dictionary messages + record-batch message only.
func serializeForShm(batch arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	if !schemaHasDictionary(batch.Schema()) {
		return buf.Bytes(), nil
	}
	full := buf.Bytes()
	afterSchema, err := skipOneIPCMessage(full)
	if err != nil {
		return nil, err
	}
	if !bytes.HasSuffix(full, ipcEOS[:]) {
		return nil, fmt.Errorf("stream missing trailing EOS marker")
	}
	return full[afterSchema : len(full)-len(ipcEOS)], nil
}

func writeSchemaOnlyStream(schema *arrow.Schema) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func readIPCStream(buf []byte) (arrow.RecordBatch, error) {
	rdr, err := ipc.NewReader(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	defer rdr.Release()
	if !rdr.Next() {
		if e := rdr.Err(); e != nil {
			return nil, e
		}
		return nil, fmt.Errorf("empty SHM region")
	}
	out := rdr.RecordBatch()
	out.Retain()
	return out, nil
}

// skipOneIPCMessage advances past a single IPC message (continuation
// marker + metadata length + metadata + body) and returns the byte
// offset just after it.
func skipOneIPCMessage(buf []byte) (int, error) {
	if len(buf) < 8 {
		return 0, fmt.Errorf("IPC stream too short")
	}
	pos := 0
	if buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF {
		pos += 4
	}
	if pos+4 > len(buf) {
		return 0, fmt.Errorf("IPC stream truncated")
	}
	metaLen := int(binary.LittleEndian.Uint32(buf[pos : pos+4]))
	pos += 4
	if metaLen == 0 {
		return 0, fmt.Errorf("unexpected EOS while skipping schema")
	}
	if pos+metaLen > len(buf) {
		return 0, fmt.Errorf("IPC metadata truncated")
	}
	bodyLen, err := readMessageBodyLength(buf[pos : pos+metaLen])
	if err != nil {
		return 0, err
	}
	return pos + metaLen + bodyLen, nil
}

// readMessageBodyLength parses a flatbuffer-encoded org.apache.arrow.flatbuf.Message
// just enough to extract its bodyLength field. Avoiding a full flatbuffers
// dependency by hand-walking the same offsets used by arrow-go.
//
// The Message table layout is documented in
// arrow-format/Message.fbs and the field offsets are stable.
func readMessageBodyLength(meta []byte) (int, error) {
	if len(meta) < 8 {
		return 0, fmt.Errorf("flatbuffer too short")
	}
	// Root offset is at byte 0 (uint32 LE).
	rootOff := binary.LittleEndian.Uint32(meta[0:4])
	if int(rootOff) >= len(meta) {
		return 0, fmt.Errorf("flatbuffer root out of range")
	}
	// At rootOff is a soffset_t (int32) pointing back to the vtable.
	tablePos := int(rootOff)
	if tablePos+4 > len(meta) {
		return 0, fmt.Errorf("flatbuffer table truncated")
	}
	vtableSOff := int32(binary.LittleEndian.Uint32(meta[tablePos : tablePos+4]))
	vtablePos := tablePos - int(vtableSOff)
	if vtablePos < 0 || vtablePos+6 > len(meta) {
		return 0, fmt.Errorf("flatbuffer vtable out of range")
	}
	vtableSize := int(binary.LittleEndian.Uint16(meta[vtablePos : vtablePos+2]))
	// Field order in Message: version (0), header_type (1), header (2),
	// bodyLength (3), custom_metadata (4). bodyLength field index = 3,
	// vtable slot offset = 4 + 2*3 = 10.
	const bodyLengthSlot = 4 + 2*3
	if bodyLengthSlot+2 > vtableSize {
		// Field absent → default 0.
		return 0, nil
	}
	if vtablePos+bodyLengthSlot+2 > len(meta) {
		return 0, fmt.Errorf("flatbuffer vtable truncated")
	}
	fieldOff := int(binary.LittleEndian.Uint16(meta[vtablePos+bodyLengthSlot : vtablePos+bodyLengthSlot+2]))
	if fieldOff == 0 {
		return 0, nil
	}
	abs := tablePos + fieldOff
	if abs+8 > len(meta) {
		return 0, fmt.Errorf("flatbuffer body field out of range")
	}
	return int(binary.LittleEndian.Uint64(meta[abs : abs+8])), nil
}

// ----------------------------------------------------------------------
// POSIX shm_open helpers
// ----------------------------------------------------------------------

// shmOpen wraps shm_open() via cgo. flags is a bitmask of O_* values
// from <fcntl.h>; mode is the POSIX permission bits (used only when
// O_CREAT is set).
func shmOpen(name string, flags int, mode uint32) (int, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	fd, err := C.go_shm_open(cname, C.int(flags), C.mode_t(mode))
	if fd < 0 {
		if err == nil {
			err = syscall.EINVAL
		}
		return -1, err
	}
	return int(fd), nil
}

func shmUnlink(name string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	rc, err := C.go_shm_unlink(cname)
	if rc < 0 {
		if err == nil {
			err = syscall.EINVAL
		}
		return err
	}
	return nil
}

// shmTryCreate calls shm_open(O_CREAT|O_EXCL|O_RDWR) + ftruncate + mmap.
// Returns *ShmSegment with track=true on success, an error wrapping
// EEXIST if another segment already owns the name, or a generic error
// otherwise.
func shmTryCreate(name string, size int) (*ShmSegment, error) {
	fd, err := shmOpen(name, unix.O_CREAT|unix.O_EXCL|unix.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Ftruncate(fd, int64(size)); err != nil {
		_ = unix.Close(fd)
		_ = shmUnlink(name)
		return nil, fmt.Errorf("ftruncate: %w", err)
	}
	data, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	_ = unix.Close(fd)
	if err != nil {
		_ = shmUnlink(name)
		return nil, fmt.Errorf("mmap: %w", err)
	}
	return &ShmSegment{name: name, size: size, data: data, track: true}, nil
}

// shmOpenForAttach calls shm_open(O_RDWR) on `name`, falling back to
// `/`+name on POSIX systems where the leading slash was stripped at the
// Python boundary.
func shmOpenForAttach(name string) (int, error) {
	fd, err := shmOpen(name, unix.O_RDWR, 0o600)
	if err == nil {
		return fd, nil
	}
	if len(name) == 0 || name[0] == '/' {
		return -1, fmt.Errorf("shm_open(attach) %q: %w", name, err)
	}
	fd2, err2 := shmOpen("/"+name, unix.O_RDWR, 0o600)
	if err2 == nil {
		return fd2, nil
	}
	return -1, fmt.Errorf("shm_open(attach) %q: %v; with leading slash: %v", name, err, err2)
}

// makeShmName returns a process-and-time-derived segment name, matching
// the leading-slash convention used by Rust and Python.
func makeShmName() string {
	pid := os.Getpid()
	nanos := unix.TimespecToNsec(unixNow())
	// Add an atomic counter so rapid successive calls in the same nanosecond
	// don't collide.
	c := shmNameCounter.Add(1)
	return fmt.Sprintf("/vgi_rpc_%x_%x_%x", pid, nanos, c)
}

var shmNameCounter atomic.Uint64

func unixNow() unix.Timespec {
	var ts unix.Timespec
	_ = unix.ClockGettime(unix.CLOCK_REALTIME, &ts)
	return ts
}

// ----------------------------------------------------------------------
// Server-side: per-request attach
// ----------------------------------------------------------------------

// shmAttachFromMetadata extracts MetaShmSegmentName/MetaShmSegmentSize
// from request metadata and attaches a non-tracking segment. Returns nil
// if the metadata is absent (no shm in this request) or attach fails.
// Server hot path: never returns an error; failures are silently
// degraded into "no shm" so the request still serves over the pipe.
//
// Only honoured for local pipe / Unix-socket transports where the
// client and server share a kernel and the client legitimately owns
// the named segment.  HTTP transport is explicitly rejected: a remote
// client could otherwise supply an arbitrary POSIX shm name and have
// the server attach to it, leading to information disclosure or
// crashes on multi-tenant hosts.  HTTP dispatch does not currently
// call this helper; the rejection is defence-in-depth against future
// refactors that might wire it differently.
func shmAttachFromMetadata(reqMeta map[string]string, isHTTP bool) *ShmSegment {
	if isHTTP {
		if name, ok := reqMeta[MetaShmSegmentName]; ok {
			slog.Warn("vgirpc: refusing dynamic SHM attach over HTTP transport",
				"client_supplied_segment", name)
		}
		return nil
	}
	name, hasName := reqMeta[MetaShmSegmentName]
	sizeStr, hasSize := reqMeta[MetaShmSegmentSize]
	if !hasName || !hasSize {
		return nil
	}
	size, err := strconv.Atoi(sizeStr)
	if err != nil || size <= ShmHeaderSize {
		return nil
	}
	seg, err := ShmAttach(name, size, false /* track */)
	if err != nil {
		return nil
	}
	return seg
}
