// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Signed session cookie (stores code_verifier + state + original URL + return_to)
// ---------------------------------------------------------------------------

// packOAuthCookie packs PKCE session data into a signed, base64-encoded cookie value.
//
// Wire format v4:
//
//	[1B version=4] [8B created_at uint64 LE]
//	[2B cv_len uint16 LE] [cv_len bytes code_verifier]
//	[2B state_len uint16 LE] [state_len bytes state_nonce]
//	[2B url_len uint16 LE] [url_len bytes original_url]
//	[2B rt_len uint16 LE] [rt_len bytes return_to]
//	[32B HMAC-SHA256(session_key, all above)]
func packOAuthCookie(verifier, state, originalURL, returnTo string, sessionKey []byte, createdAt int64) string {
	cvBytes := []byte(verifier)
	stateBytes := []byte(state)
	urlBytes := []byte(originalURL)
	rtBytes := []byte(returnTo)

	// Calculate total payload size: 1 + 8 + 4*(2) + len(fields)
	payloadLen := 1 + 8 + 2 + len(cvBytes) + 2 + len(stateBytes) + 2 + len(urlBytes) + 2 + len(rtBytes)
	payload := make([]byte, 0, payloadLen)

	// Version
	payload = append(payload, sessionCookieVersion)

	// Created at (uint64 LE)
	var ts [8]byte
	binary.LittleEndian.PutUint64(ts[:], uint64(createdAt))
	payload = append(payload, ts[:]...)

	// Code verifier
	var lenBuf [2]byte
	binary.LittleEndian.PutUint16(lenBuf[:], uint16(len(cvBytes)))
	payload = append(payload, lenBuf[:]...)
	payload = append(payload, cvBytes...)

	// State nonce
	binary.LittleEndian.PutUint16(lenBuf[:], uint16(len(stateBytes)))
	payload = append(payload, lenBuf[:]...)
	payload = append(payload, stateBytes...)

	// Original URL
	binary.LittleEndian.PutUint16(lenBuf[:], uint16(len(urlBytes)))
	payload = append(payload, lenBuf[:]...)
	payload = append(payload, urlBytes...)

	// Return to
	binary.LittleEndian.PutUint16(lenBuf[:], uint16(len(rtBytes)))
	payload = append(payload, lenBuf[:]...)
	payload = append(payload, rtBytes...)

	// HMAC
	mac := hmac.New(sha256.New, sessionKey)
	mac.Write(payload)
	sig := mac.Sum(nil)

	// base64 URL encoding WITH padding (to match Python's urlsafe_b64encode)
	return base64.URLEncoding.EncodeToString(append(payload, sig...))
}

// unpackOAuthCookie unpacks and verifies a signed OAuth session cookie.
// Returns (verifier, state, originalURL, returnTo, err).
func unpackOAuthCookie(cookieValue string, sessionKey []byte, maxAge int) (verifier, state, originalURL, returnTo string, err error) {
	raw, err := base64.URLEncoding.DecodeString(cookieValue)
	if err != nil {
		// Also try RawURL (no padding) for robustness
		raw, err = base64.RawURLEncoding.DecodeString(cookieValue)
		if err != nil {
			return "", "", "", "", fmt.Errorf("malformed session cookie")
		}
	}

	// Minimum: version(1) + timestamp(8) + 4*length(2) + HMAC(32) = 49
	if len(raw) < 49 {
		return "", "", "", "", fmt.Errorf("session cookie too short")
	}

	// Verify HMAC before inspecting payload
	payload := raw[:len(raw)-pkceHMACLen]
	receivedMAC := raw[len(raw)-pkceHMACLen:]
	mac := hmac.New(sha256.New, sessionKey)
	mac.Write(payload)
	expectedMAC := mac.Sum(nil)
	if subtle.ConstantTimeCompare(receivedMAC, expectedMAC) != 1 {
		return "", "", "", "", fmt.Errorf("session cookie signature mismatch")
	}

	// Parse payload
	version := payload[0]
	if version != sessionCookieVersion {
		return "", "", "", "", fmt.Errorf("unexpected session cookie version: %d", version)
	}

	createdAt := binary.LittleEndian.Uint64(payload[1:9])
	if maxAge > 0 {
		age := time.Now().Unix() - int64(createdAt)
		if age < 0 || age > int64(maxAge) {
			return "", "", "", "", fmt.Errorf("session cookie expired (age=%ds, max=%ds)", age, maxAge)
		}
	}

	pos := 9

	// Code verifier
	if pos+2 > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	cvLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	if pos+cvLen > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	verifier = string(payload[pos : pos+cvLen])
	pos += cvLen

	// State nonce
	if pos+2 > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	stateLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	if pos+stateLen > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	state = string(payload[pos : pos+stateLen])
	pos += stateLen

	// Original URL
	if pos+2 > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	urlLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	if pos+urlLen > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	originalURL = string(payload[pos : pos+urlLen])
	pos += urlLen

	// Return to
	if pos+2 > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	rtLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	if pos+rtLen > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	returnTo = string(payload[pos : pos+rtLen])

	return verifier, state, originalURL, returnTo, nil
}
