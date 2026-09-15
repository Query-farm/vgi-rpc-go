// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"fmt"
	"net/http"
	"strings"
)

// Namespaced HTTP routing: {prefix}/{protocol}/{method}.
//
// On HTTP the protocol rides twice -- in the vgi_rpc.protocol metadata field
// and as a path segment. The metadata field is canonical: it is the only
// carrier on the stdio, unix and named-pipe transports, so it is the one every
// port already has to read. The path segment is a required *faithful
// projection*, present so an edge device -- a WAF, an API gateway, a routing
// load balancer -- can act on the protocol without an Arrow parser.
//
// Left unchecked the two may disagree, and then edge policy is applied to one
// protocol while the worker dispatches another: the
// Content-Length/Transfer-Encoding shape, where the intermediary and the origin
// read one request as two.

// serverTokenScope is the AAD scope for tokens that belong to the server rather
// than to any one protocol -- sticky-session tokens, which live at the
// un-namespaced prefix. Spelled with a leading NUL so it cannot collide with a
// protocol name, which must start [A-Za-z_]. Mirrors Python's SERVER_SCOPE.
const serverTokenScope = "\x00server"

// rawProtocolSegment returns the protocol path segment exactly as it arrived on
// the wire, before any percent-decoding.
//
// net/http's ServeMux decodes each segment before handing it to PathValue, so
// PathValue alone cannot see a percent sign: "/demo%2EApp%2Ev1/echo" reaches the
// handler as "demo.App.v1" and would route. That is the split view this rule
// exists to close -- the edge matched one string and the worker would dispatch
// another -- so the check has to run on the escaped path.
func (h *HttpServer) rawProtocolSegment(r *http.Request) string {
	rest := strings.TrimPrefix(r.URL.EscapedPath(), h.prefix)
	rest = strings.TrimPrefix(rest, "/")
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[:i]
	}
	return rest
}

// resolveHTTPRoute maps the (protocol, method) pair a request path names to a
// hosted method, or explains why it cannot.
//
// Every failure is a 404, matching unknown-method and gRPC's use of
// UNIMPLEMENTED for both: an unroutable path is unroutable whatever the body
// says, and a client that cannot reach a protocol needs the same answer it gets
// for a method it cannot reach.
func (h *HttpServer) resolveHTTPRoute(r *http.Request, protocol, method string) (*methodInfo, *protocolBinding, error) {
	// Before anything else, and without decoding. The protocol charset never
	// requires percent-encoding, so a percent sign is a bug or an attempt to
	// have the edge and the worker read different strings. Compare raw bytes;
	// never compare decoded-against-raw -- which is why this reads the escaped
	// path rather than the decoded wildcard value.
	if raw := h.rawProtocolSegment(r); strings.Contains(raw, "%") {
		return nil, nil, &ProtocolNotSpecifiedError{
			Hosted: sortedKeys(h.server.bindings()),
			Detail: fmt.Sprintf(
				"protocol path segment %q contains a percent sign; the protocol charset never "+
					"requires encoding, so this is rejected rather than decoded", raw),
		}
	}
	return h.server.resolve(protocol, method)
}

// checkProtocolCarriage requires the two carriers of the routing key to agree.
//
// The metadata field is canonical and the path segment is its projection, so a
// disagreement means edge policy and worker dispatch saw different protocols.
// An absent metadata field is refused too, including against a server hosting
// exactly one protocol: an exemption would let an intermediary that rebuilds a
// request and drops the field land silently on whichever protocol happened to
// be registered first, rather than being told.
//
// Mirrors the vgi_rpc.method check the HTTP dispatchers already make.
func (h *HttpServer) checkProtocolCarriage(pathProtocol string, meta map[string]string) error {
	declared := meta[MetaProtocol]
	if declared == "" {
		return &ProtocolNotSpecifiedError{Hosted: sortedKeys(h.server.bindings())}
	}
	if declared != pathProtocol {
		return &ProtocolNotSupportedError{
			Requested: declared,
			Hosted:    sortedKeys(h.server.bindings()),
			Detail: fmt.Sprintf(
				"protocol mismatch: the request path names %q but the Arrow IPC custom_metadata "+
					"'%s' names %q. These must agree.", pathProtocol, MetaProtocol, declared),
		}
	}
	return nil
}

// handleRpcGet answers a GET on the two-segment RPC route.
//
// RPC endpoints are POST-only, and Go's mux would otherwise answer 405 for any
// two-segment GET that happens to match the wildcard route -- including paths
// belonging to other features. Only a first segment that is actually a hosted
// protocol earns the 405; everything else is a 404, because the path was never
// an RPC endpoint in the first place.
func (h *HttpServer) handleRpcGet(w http.ResponseWriter, r *http.Request) {
	protocol := r.PathValue("protocol")
	_, hosted := h.server.bindings()[protocol]
	if strings.Contains(h.rawProtocolSegment(r), "%") || !hosted {
		if h.enableNotFoundPage && h.notFoundHTML != nil {
			h.handleNotFound(w, r)
			return
		}
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Allow", "POST")
	http.Error(w, "RPC endpoints accept POST only", http.StatusMethodNotAllowed)
}

// handleReserved serves the flat, server-level reserved methods that belong to
// no protocol and so are not namespaced: POST {prefix}/__describe__.
//
// Anything that is not a reserved name 404s here rather than falling through to
// a protocol lookup -- the flat route is not a catch-all. Without that, every
// unrelated one-segment POST would be answered as a malformed RPC call.
func (h *HttpServer) handleReserved(w http.ResponseWriter, r *http.Request) {
	// Authentication first, as on every other RPC route: an anonymous probe of
	// a path that does not exist must not learn that it does not exist before
	// it has been asked for a credential.
	identity := h.authenticateIdentity(w, r)
	if identity == nil {
		return
	}
	var budgetOK bool
	r, budgetOK = h.applyResponseBudget(w, r, nil)
	if !budgetOK {
		return
	}
	method := r.PathValue("reserved")
	if !strings.HasPrefix(method, "__") || !strings.HasSuffix(method, "__") || len(method) < 5 {
		h.writeHttpError(w, http.StatusNotFound,
			&MethodNotImplementedError{Method: method}, nil)
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != arrowContentType {
		h.writeHttpError(w, http.StatusUnsupportedMediaType,
			fmt.Errorf("unsupported content type: %s", ct), nil)
		return
	}
	if method == "__describe__" {
		h.handleDescribe(w, r)
		return
	}
	h.writeHttpError(w, http.StatusNotFound,
		&MethodNotImplementedError{Method: method}, nil)
}
