// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/base64"
	"fmt"
)

// The control channel carries binary as standard, padded base64 (RFC 4648 §4):
// not the URL alphabet, and not line-wrapped. Both alternatives decode cleanly
// in some languages and fail in others, which is the kind of difference that
// shows up as a corrupt Arrow buffer three layers away from its cause.

func encodeBase64(raw []byte) string {
	return base64.StdEncoding.EncodeToString(raw)
}

func decodeBase64(encoded string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("base64 decode: %w", err)
	}
	return raw, nil
}
