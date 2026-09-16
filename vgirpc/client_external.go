// © Copyright 2025-2026, Query.Farm LLC - https://query.farm

package vgirpc

import (
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
)

// External-location resolution, client half.
//
// A server whose response outgrows the wire replaces it with a zero-row
// pointer batch naming a URL, and the client fetches the payload itself. The
// machinery to do that has been here since external storage landed
// ([ResolveExternalLocation]); what was missing was any way to tell a client
// to use it, so both clients answered a pointer batch with
// "external-location responses require an external resolver" -- a correct
// refusal that no caller could act on.
//
// Resolution stays opt-in rather than defaulted. Following a URL a peer chose
// is an outbound request to an address the caller did not pick, so it is a
// decision a caller makes explicitly, with byte caps and a URL validator
// attached.
//
// The validator is the part to get right, and [ExternalLocationConfig] does
// not supply one: unlike every other field on it, URLValidator has no default,
// so a zero value follows any URL a peer sends -- plain http://, a link-local
// metadata address, an internal host. [DefaultExternalLocationConfig] installs
// [HTTPSOnlyValidator]; a config built by hand has to say so itself.

// WithClientExternalResolution makes the client fetch external-location
// pointer batches instead of refusing them.
//
// Pass nil to leave resolution off, which is the default.
//
// Set URLValidator on the config. It is the one field with no default, so a
// bare &ExternalLocationConfig{} fetches from wherever the peer points --
// start from [DefaultExternalLocationConfig], or set [HTTPSOnlyValidator]
// explicitly. A nil validator is the deliberate opt-out, for a local test
// server vending http:// URLs and for nothing else.
func WithClientExternalResolution(config *ExternalLocationConfig) HttpClientOption {
	return func(cfg *httpClientConfig) error {
		cfg.external = config
		return nil
	}
}

// WithTcpClientExternalResolution is [WithClientExternalResolution] for the raw
// transports.
//
// A raw peer is on the other end of one socket rather than behind a URL, so a
// pointer batch there is rarer -- but it is the same wire, and a server
// configured to externalize does so on every transport it serves.
func WithTcpClientExternalResolution(config *ExternalLocationConfig) TcpClientOption {
	return func(cfg *tcpClientConfig) error {
		cfg.external = config
		return nil
	}
}

// resolveExternalBatch fetches a pointer batch's payload, or reports the
// refusal a client with no resolver owes its caller.
//
// The returned batch replaces the pointer and is owned by the caller; the
// pointer is released here, because a caller that has been handed a resolved
// batch has no handle left on the one it replaced.
func resolveExternalBatch(
	config *ExternalLocationConfig,
	onLog ClientLogHandler,
	record arrow.RecordBatch,
	metadata map[string]string,
) (arrow.RecordBatch, map[string]string, error) {
	if config == nil {
		record.Release()
		return nil, nil, &RpcError{
			Type:    "ProtocolError",
			Message: "external-location responses require an external resolver",
		}
	}
	resolved, resolvedMeta, err := resolveExternalLocationWithLog(record, metadataToArrow(metadata), config, onLog)
	if err != nil {
		record.Release()
		var rpcErr *RpcError
		if errors.As(err, &rpcErr) {
			// An exception inside the fetched payload is the producer's, not a
			// fetch failure: relay its type rather than flattening it.
			return nil, nil, rpcErr
		}
		return nil, nil, &RpcError{Type: "ProtocolError", Message: err.Error()}
	}
	record.Release()
	return resolved, arrowToMetadata(resolvedMeta), nil
}

// metadataToArrow renders a decoded metadata map back into Arrow's form.
//
// The clients decode custom metadata into a map because that is what they hand
// callers; [ResolveExternalLocation] reads Arrow metadata because that is what
// the server side passes it. One of the two has to convert, and doing it here
// keeps the conversion on the rare path.
func metadataToArrow(metadata map[string]string) arrow.Metadata {
	keys := make([]string, 0, len(metadata))
	values := make([]string, 0, len(metadata))
	for key, value := range metadata {
		keys = append(keys, key)
		values = append(values, value)
	}
	return arrow.NewMetadata(keys, values)
}

// arrowToMetadata is the inverse of [metadataToArrow].
func arrowToMetadata(meta arrow.Metadata) map[string]string {
	keys, values := meta.Keys(), meta.Values()
	out := make(map[string]string, len(keys))
	for i := range keys {
		out[keys[i]] = values[i]
	}
	return out
}
