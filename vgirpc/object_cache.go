// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"container/list"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The per-process object cache (STATE_REF_V1.md §11).
//
// Stored StateRef values and argument references both name an object in
// storage by its object_id. Resolving one fetches the object, verifies it and
// opens it; this cache keeps the result so the next turn -- or the next scan
// goroutine of the same query -- does not fetch again. The rules, all
// required:
//
//  1. Keyed by object_id, never by a digest. Grants mint objects for claimed
//     hashes without seeing the bytes, so a digest-keyed cache would hand one
//     principal's value to another who merely knows its hash. An object_id
//     only reaches a server inside a tag or reference sealed for the caller.
//  2. Single-flight: concurrent resolves of one object_id share one fetch.
//  3. Byte-bounded LRU: VGI_RPC_OBJECT_CACHE_BYTES, default
//     min(1 GiB, 40% of the cgroup memory limit, else of physical memory). A
//     value larger than the bound is returned but not retained, with a
//     warning.
//  4. Memos hold the object_id, never the value, and look it up again on use,
//     so resident bytes stay bounded by this cache alone.
//  5. fetched_at: a hit on an entry fetched more than one day ago is a miss,
//     so the cache never outlives the bucket lifecycle that deletes objects.
//
// Nothing here is visible on the wire.

// ObjectCacheBytesEnv overrides the object cache's byte bound.
const ObjectCacheBytesEnv = "VGI_RPC_OBJECT_CACHE_BYTES"

// ObjectCacheMaxAge is the age past which a cached entry is treated as a
// miss: one day, the bucket lifecycle bound.
const ObjectCacheMaxAge = 24 * time.Hour

const oneGiB = int64(1) << 30

// ObjectCacheStats is a snapshot of an [ObjectCache]'s counters.
type ObjectCacheStats struct {
	ResidentBytes int64 // bytes of values currently retained
	Entries       int64 // number of retained values
	MaxBytes      int64 // the byte bound
	Hits          int64 // lookups answered from the cache
	Misses        int64 // lookups that fetched (aged-out entries included)
	Evictions     int64 // entries dropped to stay under the bound, or aged out
	Fetches       int64 // fetches actually performed (single-flight joins excluded)
}

type objectCacheEntry struct {
	id        string
	value     []byte
	fetchedAt time.Time
}

type objectCacheFlight struct {
	done  chan struct{}
	value []byte
	err   error
}

// ObjectCache is a byte-bounded, single-flight LRU of verified object values
// keyed by object_id. Safe for concurrent use. Values are the verified,
// opened, decompressed bytes the caller's fetch returns; the cache never
// inspects them, and callers must treat a returned slice as read-only.
type ObjectCache struct {
	mu                               sync.Mutex
	maxBytes                         int64
	maxAge                           time.Duration
	now                              func() time.Time
	entries                          map[string]*list.Element
	order                            *list.List // front = most recently used
	resident                         int64
	flights                          map[string]*objectCacheFlight
	hits, misses, evictions, fetches int64
}

// NewObjectCache creates a cache bounded at maxBytes (0 retains nothing:
// every lookup fetches, still single-flight).
func NewObjectCache(maxBytes int64) *ObjectCache {
	if maxBytes < 0 {
		maxBytes = 0
	}
	return &ObjectCache{
		maxBytes: maxBytes,
		maxAge:   ObjectCacheMaxAge,
		now:      time.Now,
		entries:  make(map[string]*list.Element),
		order:    list.New(),
		flights:  make(map[string]*objectCacheFlight),
	}
}

// MaxBytes returns the byte bound.
func (c *ObjectCache) MaxBytes() int64 { return c.maxBytes }

// Stats returns a snapshot of the counters.
func (c *ObjectCache) Stats() ObjectCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return ObjectCacheStats{
		ResidentBytes: c.resident,
		Entries:       int64(len(c.entries)),
		MaxBytes:      c.maxBytes,
		Hits:          c.hits,
		Misses:        c.misses,
		Evictions:     c.evictions,
		Fetches:       c.fetches,
	}
}

// Clear drops every entry (tests, and an explicit operator reset).
func (c *ObjectCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*list.Element)
	c.order.Init()
	c.resident = 0
}

// AgeEntries moves every entry's fetched_at back by d and returns how many
// entries it shifted. A test hook: it proves the one-day rule without
// waiting a day (exposed to the shared suite as object_cache_age).
func (c *ObjectCache) AgeEntries(d time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.order.Front(); el != nil; el = el.Next() {
		entry := el.Value.(*objectCacheEntry)
		entry.fetchedAt = entry.fetchedAt.Add(-d)
	}
	return len(c.entries)
}

// Contains reports whether a fresh entry for objectID is retained, without
// touching any counter.
func (c *ObjectCache) Contains(objectID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[objectID]
	return ok && c.now().Sub(el.Value.(*objectCacheEntry).fetchedAt) <= c.maxAge
}

// GetOrFetch returns the value for objectID, fetching it at most once across
// goroutines. On a miss exactly one caller runs fetch; concurrent callers wait
// for and share its result (or its error). hit is true when the value came
// from the cache rather than from this call's fetch or a joined one.
func (c *ObjectCache) GetOrFetch(objectID string, fetch func() ([]byte, error)) (value []byte, hit bool, err error) {
	c.mu.Lock()
	if el, ok := c.entries[objectID]; ok {
		entry := el.Value.(*objectCacheEntry)
		if c.now().Sub(entry.fetchedAt) <= c.maxAge {
			c.order.MoveToFront(el)
			c.hits++
			c.mu.Unlock()
			slog.Debug("object_cache", "event", "hit")
			return entry.value, true, nil
		}
		// Aged out: the object may already be gone; never serve it.
		c.removeLocked(el)
		c.evictions++
	}
	c.misses++
	flight, joined := c.flights[objectID]
	if !joined {
		flight = &objectCacheFlight{done: make(chan struct{})}
		c.flights[objectID] = flight
		c.fetches++
	}
	c.mu.Unlock()

	if joined {
		<-flight.done
		return flight.value, false, flight.err
	}

	slog.Debug("object_cache", "event", "miss")
	func() {
		defer func() {
			if rv := recover(); rv != nil {
				flight.err = fmt.Errorf("object cache fetch panicked: %v", rv)
			}
		}()
		flight.value, flight.err = fetch()
	}()
	c.mu.Lock()
	delete(c.flights, objectID)
	if flight.err == nil {
		c.insertLocked(objectID, flight.value)
	}
	c.mu.Unlock()
	close(flight.done)
	return flight.value, false, flight.err
}

func (c *ObjectCache) removeLocked(el *list.Element) {
	entry := el.Value.(*objectCacheEntry)
	c.order.Remove(el)
	delete(c.entries, entry.id)
	c.resident -= int64(len(entry.value))
}

func (c *ObjectCache) insertLocked(objectID string, value []byte) {
	size := int64(len(value))
	if size > c.maxBytes {
		slog.Warn("object_cache: value exceeds the cache bound; resolved but not retained",
			"event", "not_retained", "bytes", size, "max_bytes", c.maxBytes, "env", ObjectCacheBytesEnv)
		return
	}
	if el, ok := c.entries[objectID]; ok {
		c.removeLocked(el)
	}
	c.entries[objectID] = c.order.PushFront(&objectCacheEntry{id: objectID, value: value, fetchedAt: c.now()})
	c.resident += size
	for c.resident > c.maxBytes && c.order.Len() > 0 {
		c.removeLocked(c.order.Back())
		c.evictions++
		slog.Debug("object_cache", "event", "evict", "resident_bytes", c.resident)
	}
}

// DefaultObjectCacheBytes returns the cache bound: VGI_RPC_OBJECT_CACHE_BYTES
// when set, else min(1 GiB, 40% of the cgroup memory limit, else of physical
// memory). getenv defaults to os.Getenv. A value that is not a non-negative
// integer is an error.
func DefaultObjectCacheBytes(getenv func(string) string) (int64, error) {
	getenv = envLookup(getenv)
	if raw := strings.TrimSpace(getenv(ObjectCacheBytesEnv)); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			return 0, fmt.Errorf("%s must be a non-negative integer, got %q", ObjectCacheBytesEnv, raw)
		}
		return v, nil
	}
	memory := cgroupMemoryLimit()
	if memory <= 0 {
		memory = physicalMemory()
	}
	if memory <= 0 {
		return oneGiB, nil
	}
	return min(oneGiB, memory*40/100), nil
}

// cgroupMemoryLimit reads the cgroup v2 memory.max, then v1
// memory.limit_in_bytes; 0 when unlimited or unknown.
func cgroupMemoryLimit() int64 {
	for _, path := range []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		text := strings.TrimSpace(string(raw))
		if text == "" || text == "max" {
			return 0
		}
		v, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			continue
		}
		// cgroup v1 reports "unlimited" as a huge page-aligned number.
		if v <= 0 || v >= 1<<60 {
			return 0
		}
		return v
	}
	return 0
}

var (
	defaultObjectCacheOnce sync.Once
	defaultObjectCache     *ObjectCache
)

// DefaultObjectCache returns the process-wide object cache, creating it on
// first use with the bound [DefaultObjectCacheBytes] reports. An invalid
// VGI_RPC_OBJECT_CACHE_BYTES is logged and the memory default used instead;
// call [DefaultObjectCacheBytes] at startup to refuse it outright.
func DefaultObjectCache() *ObjectCache {
	defaultObjectCacheOnce.Do(func() {
		bound, err := DefaultObjectCacheBytes(nil)
		if err != nil {
			slog.Warn("object_cache: ignoring invalid bound", "err", err)
			bound, _ = DefaultObjectCacheBytes(func(name string) string {
				if name == ObjectCacheBytesEnv {
					return ""
				}
				return os.Getenv(name)
			})
		}
		defaultObjectCache = NewObjectCache(bound)
	})
	return defaultObjectCache
}
