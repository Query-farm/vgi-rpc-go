// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// CallContext provides request-scoped information and logging to method handlers.
// A CallContext is not safe for concurrent use. If a handler spawns goroutines,
// they must not call [CallContext.ClientLog] without external synchronization.
type CallContext struct {
	// Ctx is the request-scoped context, carrying cancellation and deadlines.
	Ctx context.Context
	// RequestID is the client-supplied identifier for this request, echoed in
	// all response metadata.
	RequestID string
	// ServerID is the server identifier set via [Server.SetServerID].
	ServerID string
	// Method is the name of the RPC method being invoked.
	Method string
	// LogLevel is the client-requested minimum log severity. Log messages
	// below this level are silently discarded by [CallContext.ClientLog].
	LogLevel LogLevel
	// Auth is the authentication context for this request. It is never nil;
	// unauthenticated requests receive [Anonymous].
	Auth *AuthContext
	// TransportMetadata holds transport-level key/value pairs such as
	// remote_addr, user_agent, and IPC custom metadata.
	TransportMetadata map[string]string
	// Cookies holds the incoming HTTP request cookies.  Empty for non-HTTP
	// transports (pipe, subprocess, Unix socket).
	Cookies           map[string]string
	logs              []LogMessage
	responseCookies   []CookieSpec
	cookieSinkEnabled bool
}

// ClientLog records a log message that will be sent to the client.
// The message is only recorded if its level is at or above the client-requested log level.
func (ctx *CallContext) ClientLog(level LogLevel, msg string, extras ...KV) {
	if logLevelPriority(level) > logLevelPriority(ctx.LogLevel) {
		return
	}
	logMsg := LogMessage{
		Level:   level,
		Message: msg,
	}
	if len(extras) > 0 {
		logMsg.Extras = make(map[string]string, len(extras))
		for _, kv := range extras {
			logMsg.Extras[kv.Key] = kv.Value
		}
	}
	ctx.logs = append(ctx.logs, logMsg)
}

// drainLogs returns and clears all accumulated log messages.
func (ctx *CallContext) drainLogs() []LogMessage {
	logs := ctx.logs
	ctx.logs = nil
	return logs
}

// CookieAttrs holds the optional attributes for a Set-Cookie header.
// The zero value is valid and produces a cookie with only name=value.
type CookieAttrs struct {
	Expires     time.Time
	MaxAge      int
	Domain      string
	Path        string
	Secure      bool
	HttpOnly    bool
	SameSite    http.SameSite
	Partitioned bool
}

// CookieSpec is a queued cookie mutation to apply to the HTTP response.
// Created by [CallContext.SetCookie] / [CallContext.DeleteCookie] and
// consumed by the HTTP unary handler after dispatch completes.
type CookieSpec struct {
	Name   string
	Value  string
	Delete bool
	CookieAttrs
}

// errCookieNotUnaryHTTP is returned by SetCookie / DeleteCookie when the
// call is not a unary RPC served over HTTP (e.g. streaming method or
// pipe/subprocess transport).
var errCookieNotUnaryHTTP = errors.New("SetCookie/DeleteCookie is only supported inside unary RPC methods served over HTTP")

// SetCookie queues a Set-Cookie header for the HTTP response.  Only valid
// inside a unary RPC method served over HTTP; returns an error otherwise.
func (ctx *CallContext) SetCookie(name, value string, attrs CookieAttrs) error {
	if !ctx.cookieSinkEnabled {
		return errCookieNotUnaryHTTP
	}
	ctx.responseCookies = append(ctx.responseCookies, CookieSpec{
		Name:        name,
		Value:       value,
		CookieAttrs: attrs,
	})
	return nil
}

// DeleteCookie queues a cookie deletion on the HTTP response.  Only valid
// inside a unary RPC method served over HTTP.
func (ctx *CallContext) DeleteCookie(name, path, domain string) error {
	if !ctx.cookieSinkEnabled {
		return errCookieNotUnaryHTTP
	}
	ctx.responseCookies = append(ctx.responseCookies, CookieSpec{
		Name:   name,
		Delete: true,
		CookieAttrs: CookieAttrs{
			Path:   path,
			Domain: domain,
		},
	})
	return nil
}

// enableCookieSink marks this CallContext as able to accept Set-Cookie
// directives.  Called by the unary HTTP handler before dispatch.
func (ctx *CallContext) enableCookieSink() {
	ctx.cookieSinkEnabled = true
}

// drainCookies returns and clears all queued cookie mutations.
func (ctx *CallContext) drainCookies() []CookieSpec {
	cookies := ctx.responseCookies
	ctx.responseCookies = nil
	return cookies
}
