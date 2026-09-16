// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"net/http"
	"strings"
	"sync"
)

// Sticky sessions, client half.
//
// The server half has been here since the transport was written; this is the
// other end of it, and without it a Go client cannot hold a session at all --
// it sends no VGI-Session-Accept, so the server never mints one, and every call
// lands wherever the load balancer feels like sending it. That is invisible
// against a single-process server and fatal against a fleet.
//
// The mechanism is deliberately small. A client in a session stamps
// VGI-Session-Accept on every request, plus VGI-Session once the server has
// minted a token, plus whatever VGI-Echo-* values the server told it to carry
// back. The server answers with VGI-Session (a token, possibly rotated),
// VGI-Echo-<name> (routing the client must replay -- there is no Set-Cookie
// analogue for headers), and VGI-Session-Close when the session is over.

// clientSession is the token and echo headers in flight for one scope.
//
// Guarded by a mutex because the client is documented as safe for concurrent
// unary calls, and every one of them reads and writes this.
type clientSession struct {
	mu    sync.Mutex
	token string
	echo  map[string]string
	// detached suppresses the exit-time DELETE. A caller that stashed the
	// token means the session to outlive this client, and releasing it on
	// scope exit would evict the very thing they saved.
	detached bool
}

// BeginSession opens a sticky-session scope on this client.
//
// token resumes a session a previous scope detached; "" lets the server mint
// one on the next call.
//
// Scopes nest. A session is a scope rather than a connection-wide mode because
// that is what it is used for: a caller holding one session open legitimately
// opens a second -- to observe that a drained server refuses new sessions
// while the one already open keeps serving, say -- and a flat model makes the
// inner [HttpClient.EndSession] silently destroy the outer session, so the
// next call on it fails with no session bound. Calls in flight use the
// innermost scope; ending it restores the one beneath.
func (c *HttpClient) BeginSession(token string) {
	if c == nil {
		return
	}
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()
	c.sessions = append(c.sessions, &clientSession{token: token, echo: make(map[string]string)})
}

// currentSession returns the innermost open scope, or nil.
func (c *HttpClient) currentSession() *clientSession {
	if c == nil {
		return nil
	}
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()
	if len(c.sessions) == 0 {
		return nil
	}
	return c.sessions[len(c.sessions)-1]
}

// CurrentSessionToken returns the session token in flight, or "" when no
// session is open or the server has not minted one yet.
//
// Worth stashing across a process lifetime together with
// [HttpClient.CurrentEchoHeaders]: on a platform whose routing is
// client-supplied, a token resumed without its echo headers reaches a
// different backend and resolves to nothing.
func (c *HttpClient) CurrentSessionToken() string {
	session := c.currentSession()
	if session == nil {
		return ""
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.token
}

// CurrentEchoHeaders returns a copy of the VGI-Echo-* values the server asked
// this client to replay, keyed by the name with the prefix removed.
//
// Empty before the session-opening call returns, and empty again once the
// server closes the session.
func (c *HttpClient) CurrentEchoHeaders() map[string]string {
	out := make(map[string]string)
	session := c.currentSession()
	if session == nil {
		return out
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	for name, value := range session.echo {
		out[name] = value
	}
	return out
}

// DetachSession hands the session token to the caller and suppresses the
// release [HttpClient.EndSession] would otherwise perform.
//
// Returns "" when no session was live. After detaching, the server-side
// session stays alive until its TTL elapses or someone closes it explicitly.
func (c *HttpClient) DetachSession() string {
	session := c.currentSession()
	if session == nil {
		return ""
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	token := session.token
	session.detached = true
	return token
}

// EndSession closes the scope opened by [HttpClient.BeginSession].
//
// Unless the session was detached, this issues a best-effort
// DELETE {prefix}/__session__ so the server releases the registry entry now
// rather than at TTL. Best-effort is the whole contract: a session the server
// has already closed, or a server that has gone away, must not turn scope exit
// into a failure the caller has no action for.
func (c *HttpClient) EndSession(ctx context.Context) {
	if c == nil {
		return
	}
	c.sessionMu.Lock()
	if len(c.sessions) == 0 {
		c.sessionMu.Unlock()
		return
	}
	session := c.sessions[len(c.sessions)-1]
	c.sessions = c.sessions[:len(c.sessions)-1]
	c.sessionMu.Unlock()
	session.mu.Lock()
	token := session.token
	detached := session.detached
	session.mu.Unlock()
	if token == "" || detached {
		return
	}
	c.deleteSessionBestEffort(ctx, token)
}

// stickySessionEndpoint is the framework route that releases a session. Like
// the other framework endpoints it sits below the prefix and carries no
// routing key: a session belongs to a connection, not to a protocol.
const stickySessionEndpoint = "__session__"

func (c *HttpClient) deleteSessionBestEffort(ctx context.Context, token string) {
	if ctx == nil {
		ctx = context.Background()
	}
	u := *c.baseURL
	u.Path = strings.TrimRight(c.baseURL.Path, "/") + c.prefix + "/" + stickySessionEndpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), nil)
	if err != nil {
		return
	}
	req.Header = c.headers.Clone()
	req.Header.Set(stickySessionHeader, token)
	resp, err := c.inner.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}

// applySessionHeaders stamps the session opt-in, the token, and the echo
// headers onto one outgoing request.
//
// Caller-set headers win: an operator who set a header explicitly meant it,
// and an echo value silently overwriting it would be a routing decision made
// behind their back.
func (c *HttpClient) applySessionHeaders(header http.Header) {
	session := c.currentSession()
	if session == nil {
		return
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	header.Set(stickySessionAcceptHeader, "true")
	if session.token != "" {
		header.Set(stickySessionHeader, session.token)
	}
	for name, value := range session.echo {
		if header.Get(name) == "" {
			header.Set(name, value)
		}
	}
}

// observeSessionHeaders records what the server said about the session.
//
// Runs on every response rather than only the opening one, because a token may
// rotate and because VGI-Session-Close can arrive on any turn. It is a map
// write on a handful of headers, so the cost of checking always is lower than
// the cost of the one case where checking sometimes is wrong.
func (c *HttpClient) observeSessionHeaders(header http.Header) {
	session := c.currentSession()
	if session == nil {
		return
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if token := header.Get(stickySessionHeader); token != "" {
		session.token = token
	}
	advertised := advertisedEchoNames(header)
	for name, values := range header {
		if len(values) == 0 {
			continue
		}
		if rest, ok := cutPrefixFold(name, stickyEchoHeaderPrefix); ok {
			session.echo[advertised.spell(rest)] = values[0]
		}
	}
	if strings.EqualFold(strings.TrimSpace(header.Get(stickySessionCloseHeader)), "true") {
		// Dropped, not remembered as "closed": the opt-in header keeps going
		// out, so the next call opens a fresh session rather than failing.
		// That is what the reference does, and a scope whose session the
		// server ended is still a usable scope.
		session.token = ""
		for name := range session.echo {
			delete(session.echo, name)
		}
	}
}

// echoNames restores the spelling the server used for an echo header name.
//
// Go's net/http canonicalises every response header name it parses, so the
// server's VGI-Echo-x-vgi-conformance-echo reaches this client as
// Vgi-Echo-X-Vgi-Conformance-Echo and the name after the prefix comes out
// re-cased. Header names are case-insensitive on the wire, so nothing is wrong
// there -- but the captured map is a caller-visible dictionary keyed by name,
// and a caller that looked up the name the server advertised would miss.
//
// The server's own advertisement is the way back: VGI-Sticky-Echo-Headers
// lists the names, and it is a header *value*, which no one canonicalises. So
// the advertised spelling wins whenever it matches case-insensitively, and the
// canonical form is the fallback for a server that echoes a name it never
// advertised.
type echoNames map[string]string

func advertisedEchoNames(header http.Header) echoNames {
	raw := header.Get(stickyEchoHeadersHeader)
	if raw == "" {
		return nil
	}
	out := make(echoNames)
	for _, part := range strings.Split(raw, ",") {
		if name := strings.TrimSpace(part); name != "" {
			out[strings.ToLower(name)] = name
		}
	}
	return out
}

func (n echoNames) spell(name string) string {
	if advertised, ok := n[strings.ToLower(name)]; ok {
		return advertised
	}
	return name
}

// cutPrefixFold is strings.CutPrefix with ASCII case folding, because Go's
// http.Header canonicalises names to Vgi-Echo-... while the wire spells them
// VGI-Echo-... and both have to match the same constant.
func cutPrefixFold(value, prefix string) (string, bool) {
	if len(value) < len(prefix) || !strings.EqualFold(value[:len(prefix)], prefix) {
		return "", false
	}
	return value[len(prefix):], true
}
