// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"sync/atomic"
)

// accessLogSampler drops a deterministic fraction of access-log records.
//
// Three properties make the difference between a sampler that helps and one
// that quietly costs someone an incident:
//
// **Errors are never sampled.** A rate below 1 exists because successful
// calls are repetitive, which is exactly what failures are not. Dropping one
// error in ten leaves a consumer unable to read a falling error count as a
// fix landing rather than as the dice going the other way.
//
// **The decision is per call, not per record.** It is a function of a stable
// identifier — stream_id when present, request_id otherwise — so every record
// of one stream shares its init's fate. Random per-record sampling shreds a
// multi-record call into fragments indistinguishable from data loss, and the
// calls likeliest to be split are the long streams most worth studying.
//
// **The rate rides on every kept record** as sample_rate. A consumer counting
// calls has to divide by it, and a rate discoverable only from a deployment's
// flags is a rate that gets guessed wrong.
type accessLogSampler struct {
	rate      float64
	threshold uint32
	// fallback keys records that carry neither identifier, degrading to
	// per-record sampling rather than dropping them on the floor.
	fallback atomic.Uint64
}

// newAccessLogSampler returns a sampler keeping rate of the non-error
// records, or an error when rate is outside 0.0–1.0.
//
// The caller is expected to surface that error at startup rather than at the
// first request: a rate of 100 meaning "100%" would otherwise silently log
// everything, and a negative one silently nothing.
func newAccessLogSampler(rate float64) (*accessLogSampler, error) {
	if math.IsNaN(rate) || rate < 0.0 || rate > 1.0 {
		return nil, fmt.Errorf("vgirpc: access-log sample rate must be between 0.0 and 1.0, got %v", rate)
	}
	return &accessLogSampler{
		rate: rate,
		// Compare against a 32-bit hash: exact enough for sampling, and it
		// keeps the decision to one hash plus one integer compare.
		threshold: uint32(rate * float64(math.MaxUint32)),
	}, nil
}

// keep reports whether record should be emitted, stamping sample_rate on it
// when sampling is active and the record survived.
func (s *accessLogSampler) keep(record map[string]any) bool {
	if s.rate >= 1.0 {
		return true
	}
	if record["status"] == "error" {
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(s.key(record)))
	if h.Sum32() > s.threshold {
		return false
	}
	record["sample_rate"] = s.rate
	return true
}

// key returns the identifier the decision is keyed on: stream_id first so
// every record of one stream shares a fate, then request_id.
func (s *accessLogSampler) key(record map[string]any) string {
	for _, field := range [...]string{"stream_id", "request_id"} {
		if v, ok := record[field].(string); ok && v != "" {
			return v
		}
	}
	return strconv.FormatUint(s.fallback.Add(1), 36)
}
