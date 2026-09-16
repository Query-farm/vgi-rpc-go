// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Command vgi-rpc-conformance-client-driver exposes this port's *client* to
// the shared conformance suite.
//
// The suite is written in Python and drives a server. This driver is how it
// drives a foreign client instead: it speaks the newline-delimited JSON
// control protocol specified in the Python reference at
// tools/cross-port/specs/CLIENT_DRIVER_PROTOCOL.md on stdin/stdout, and
// forwards each op to [vgirpc.HttpClient] or [vgirpc.TcpClient].
//
// The gate this exists for is one sentence of that spec: a conforming client
// passes the conformance suite *against the reference server*. Until now the
// Go client had only ever been run against the Go server, which is the
// configuration that hid a routing-key defect in another port for weeks -- a
// permissive server cannot validate a client, and every accommodation a server
// makes for the client it ships with is invisible to exactly that pair.
//
// Which means this program is a relay and nothing more. It decodes no values,
// resolves no external pointers, retries nothing, normalises no error, and
// defaults neither the routing key nor the method name. Anything it repaired
// on the client's behalf would turn a client defect into a passing run, which
// is the one outcome that costs more than a red suite.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func main() {
	driver := &driver{
		out:  bufio.NewWriter(os.Stdout),
		logs: make([]vgirpc.LogMessage, 0, 8),
	}
	// ReadString rather than bufio.Scanner: a control line carries a whole
	// base64 Arrow batch, and the suite's large-payload cases run to hundreds
	// of megabytes of it. Scanner takes a fixed ceiling, and a line over the
	// ceiling arrives as a JSON parse error that looks nothing like its cause.
	reader := bufio.NewReaderSize(os.Stdin, 1<<20)
	for {
		line, err := reader.ReadString('\n')
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			if stop := driver.dispatch(trimmed); stop {
				break
			}
		}
		if err != nil {
			break
		}
	}
	// EOF on stdin is a shutdown with no response: the harness kills a driver
	// that has not exited five seconds after one.
	driver.teardown()
	driver.out.Flush()
}

// driver holds the one connection a driver process ever has.
//
// Dispatch is flat rather than nested (the spec allows either): stream ops read
// these fields. The harness guarantees it never interleaves a non-stream op
// with an open stream and never sends a stream op after a terminal event, so
// the flat shape needs no sub-loop to stay in sequence -- but stream_open while
// a stream is open is still refused rather than leaking the first one.
type driver struct {
	out *bufio.Writer

	http *vgirpc.HttpClient
	tcp  *vgirpc.TcpClient

	stream driverStream

	// logs accumulates what the server said during the current op. Drained
	// into every response, because a record belongs to the op it arrived
	// during and must be delivered exactly once.
	logsMu sync.Mutex
	logs   []vgirpc.LogMessage
}

// request is the control-protocol request. Unknown fields are ignored, which is
// how the protocol grows.
type request struct {
	Op        string          `json:"op"`
	Transport string          `json:"transport"`
	Target    json.RawMessage `json:"target"`
	Protocol  string          `json:"protocol"`
	External  bool            `json:"external"`
	// CompressionLevel is tri-state: absent picks the client default, null
	// disables request compression, an integer selects that zstd level.
	CompressionLevel json.RawMessage   `json:"compression_level"`
	Headers          map[string]string `json:"headers"`
	ShmSize          int               `json:"shm_size"`

	RequestB64 string `json:"request_b64"`
	InputB64   string `json:"input_b64"`
	IsExchange bool   `json:"is_exchange"`
	HasHeader  bool   `json:"has_header"`

	Count int     `json:"count"`
	Token *string `json:"token"`
}

func (d *driver) dispatch(line string) (stop bool) {
	var req request
	if err := json.Unmarshal([]byte(line), &req); err != nil {
		d.fail("bad control line: " + err.Error())
		return false
	}
	switch req.Op {
	case "connect":
		d.opConnect(&req)
	case "unary":
		d.opUnary(&req)
	case "describe":
		d.opDescribe()
	case "stream_open":
		d.opStreamOpen(&req)
	case "tick":
		d.opTick(&req)
	case "next_with_token":
		d.opNextWithToken()
	case "exchange":
		d.opExchange(&req)
	case "cancel":
		d.opCancel()
	case "close":
		d.opClose()
	case "capabilities", "request_upload_urls", "session_begin", "session_token",
		"session_echo_headers", "session_detach", "session_end":
		d.opHTTPAdmin(&req)
	case "shutdown":
		d.teardown()
		d.respond(map[string]any{"ok": true})
		return true
	default:
		d.fail("unknown op: " + req.Op)
	}
	return false
}

// teardown releases the stream and the connection, in that order.
//
// Part of the contract rather than an optimisation: the harness runs thousands
// of connections, so a driver that leaks a socket per connection exhausts the
// runner instead of failing a test.
func (d *driver) teardown() {
	if d.stream != nil {
		d.stream.Close(context.Background())
		d.stream = nil
	}
	if d.http != nil {
		d.http.Close()
		d.http = nil
	}
	if d.tcp != nil {
		_ = d.tcp.Close()
		d.tcp = nil
	}
}

// ---------------------------------------------------------------------------
// Control channel
// ---------------------------------------------------------------------------

// respond writes one response line and flushes.
//
// stdout is the control channel and nothing else; a stray write here
// desynchronises the entire run, so every diagnostic in this program goes to
// stderr.
func (d *driver) respond(payload map[string]any) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		encoded = []byte(`{"ok": false, "error": "driver could not encode its response"}`)
	}
	d.out.Write(encoded)
	d.out.WriteByte('\n')
	d.out.Flush()
}

// fail answers with the driver-level channel: the op was not carried out.
//
// Distinct from a call error, which is ok:true carrying a structured error
// object. ok describes the driver; error describes the call. Conflating the
// two is the most common driver defect, and it matters because a test that
// asserts on a peer's error_type sees "TransportError" instead.
func (d *driver) fail(message string) {
	d.respond(map[string]any{"ok": false, "error": message})
}

// drainLogs empties the accumulated log buffer for one response.
func (d *driver) drainLogs() []any {
	d.logsMu.Lock()
	defer d.logsMu.Unlock()
	out := make([]any, 0, len(d.logs))
	for _, message := range d.logs {
		extra := make(map[string]string, len(message.Extras))
		for key, value := range message.Extras {
			extra[key] = value
		}
		out = append(out, map[string]any{
			// Uppercase because the harness looks the name up in an enum and
			// raises on any other spelling.
			"level":   strings.ToUpper(string(message.Level)),
			"message": message.Message,
			"extra":   extra,
		})
	}
	d.logs = d.logs[:0]
	return out
}

// errorJSON renders a call error for the ok:true channel.
//
// error_type is asserted verbatim by tests, so a peer's class name is relayed
// as-is and never translated into a Go type name. A failure the client library
// raised itself -- a refused connection, a capped body -- is still a call
// error, not a driver failure, and gets TransportError.
func errorJSON(err error) map[string]any {
	var rpcErr *vgirpc.RpcError
	if errors.As(err, &rpcErr) {
		return map[string]any{
			"error_type":    rpcErr.Type,
			"error_message": rpcErr.Message,
			"traceback":     rpcErr.Traceback,
		}
	}
	return map[string]any{
		"error_type":    "TransportError",
		"error_message": err.Error(),
		"traceback":     "",
	}
}

// ---------------------------------------------------------------------------
// Arrow IPC framing
// ---------------------------------------------------------------------------

// readOneBatch decodes a complete IPC stream holding exactly one batch, and
// keeps that batch's Arrow custom metadata attached to it.
//
// The metadata is the point: it is where the RPC lives. The caller hands the
// batch straight to the client, which reads the method name and the caller's
// application keys off it.
func readOneBatch(encoded string) (arrow.RecordBatch, map[string]string, error) {
	raw, err := decodeBase64(encoded)
	if err != nil {
		return nil, nil, err
	}
	reader, err := ipc.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, nil, fmt.Errorf("read request IPC stream: %w", err)
	}
	defer reader.Release()
	if !reader.Next() {
		if readErr := reader.Err(); readErr != nil {
			return nil, nil, fmt.Errorf("read request batch: %w", readErr)
		}
		return nil, nil, errors.New("request IPC stream carried no batch")
	}
	batch := reader.RecordBatch()
	batch.Retain()
	return batch, batchMetadata(batch), nil
}

// batchMetadata lifts an IPC batch's custom metadata into a map.
func batchMetadata(batch arrow.RecordBatch) map[string]string {
	out := make(map[string]string)
	annotated, ok := batch.(arrow.RecordBatchWithMetadata)
	if !ok {
		return out
	}
	meta := annotated.Metadata()
	keys, values := meta.Keys(), meta.Values()
	for i := range keys {
		out[keys[i]] = values[i]
	}
	return out
}

// writeOneBatch re-serialises a batch and its metadata as a one-batch IPC
// stream, which is what every *_b64 field on the wire carries.
func writeOneBatch(batch arrow.RecordBatch, metadata map[string]string) (string, error) {
	keys := make([]string, 0, len(metadata))
	values := make([]string, 0, len(metadata))
	for key, value := range metadata {
		keys = append(keys, key)
		values = append(values, value)
	}
	annotated := array.NewRecordBatchWithMetadata(
		batch.Schema(), batch.Columns(), batch.NumRows(), arrow.NewMetadata(keys, values))
	defer annotated.Release()
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()))
	if err := writer.Write(annotated); err != nil {
		_ = writer.Close()
		return "", fmt.Errorf("write batch IPC stream: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("close batch IPC stream: %w", err)
	}
	return encodeBase64(buf.Bytes()), nil
}

// methodOf reads the method name a request batch names.
//
// A missing key fails loudly naming it. Defaulting the method was a real
// earlier-driver bug, and its cost was that every lost-metadata failure got
// reported as a retired-method error instead of as the framing bug it was.
func methodOf(metadata map[string]string) (string, error) {
	method := metadata[vgirpc.MetaMethod]
	if method == "" {
		return "", fmt.Errorf("request metadata carries no %s", vgirpc.MetaMethod)
	}
	return method, nil
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

func (d *driver) opConnect(req *request) {
	if d.http != nil || d.tcp != nil {
		d.fail("already connected")
		return
	}
	// The routing key is required and must never be defaulted. Over HTTP the
	// reference server routes only {protocol}/{method}, so substituting a
	// hardcoded name here would build a path that happens to exist and hide
	// exactly the class of defect this driver was written to catch.
	if req.Protocol == "" {
		d.fail("connect carries no protocol routing key")
		return
	}
	if err := d.connect(req); err != nil {
		d.fail(err.Error())
		return
	}
	d.respond(map[string]any{"ok": true})
}

func (d *driver) connect(req *request) error {
	onLog := func(message vgirpc.LogMessage) {
		d.logsMu.Lock()
		d.logs = append(d.logs, message)
		d.logsMu.Unlock()
	}
	switch req.Transport {
	case "http":
		var target string
		if err := json.Unmarshal(req.Target, &target); err != nil {
			return errors.New("http target must be a url string")
		}
		options := []vgirpc.HttpClientOption{
			vgirpc.WithClientProtocol(req.Protocol),
			vgirpc.WithClientLogHandler(onLog),
		}
		for name, value := range req.Headers {
			options = append(options, vgirpc.WithClientHeader(name, value))
		}
		if level, ok, err := compressionLevel(req.CompressionLevel); err != nil {
			return err
		} else if ok {
			options = append(options, vgirpc.WithClientRequestCompression(level))
		}
		if req.External {
			// The URL validator is disabled rather than left at HTTPS-only
			// because the conformance fixtures vend http:// download URLs from
			// a loopback store. Resolution itself stays in the client -- doing
			// it here would make the external tests pass without the client
			// ever performing a fetch.
			options = append(options, vgirpc.WithClientExternalResolution(
				&vgirpc.ExternalLocationConfig{URLValidator: nil}))
		}
		client, err := vgirpc.NewHttpClient(target, options...)
		if err != nil {
			return err
		}
		d.http = client
		return nil
	case "tcp", "unix":
		var target string
		if err := json.Unmarshal(req.Target, &target); err != nil {
			return fmt.Errorf("%s target must be a string", req.Transport)
		}
		options := []vgirpc.TcpClientOption{
			vgirpc.WithTcpClientProtocol(req.Protocol),
			vgirpc.WithTcpClientLogHandler(onLog),
		}
		if req.External {
			options = append(options, vgirpc.WithTcpClientExternalResolution(
				&vgirpc.ExternalLocationConfig{URLValidator: nil}))
		}
		if req.Transport == "unix" {
			client, err := vgirpc.NewUnixClient(context.Background(), target, options...)
			if err != nil {
				return err
			}
			d.tcp = client
			return nil
		}
		host, port, err := splitHostPort(target)
		if err != nil {
			return err
		}
		client, err := vgirpc.NewTcpClient(context.Background(), host, port, options...)
		if err != nil {
			return err
		}
		d.tcp = client
		return nil
	case "stdio", "shm":
		// Reported rather than faked. This port has a stdio *server* and no
		// stdio *client*, so there is nothing here to drive; a driver that
		// silently substituted another transport would report a pass for a
		// client that cannot make the call at all.
		return fmt.Errorf("this port has no %s client, only a %s server", req.Transport, req.Transport)
	default:
		return fmt.Errorf("unknown transport: %s", req.Transport)
	}
}

// compressionLevel reads the tri-state compression_level field.
//
// Absent means "the client's own default", null means request compression is
// off, and an integer selects that zstd level. Absence is handled even though
// the reference shim always sends the key, because reading a missing key as
// null would silently disable a default the client was entitled to pick.
func compressionLevel(raw json.RawMessage) (int, bool, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, false, nil
	}
	var level int
	if err := json.Unmarshal(raw, &level); err != nil {
		return 0, false, fmt.Errorf("compression_level must be an integer or null: %w", err)
	}
	return level, true, nil
}

// splitHostPort parses a "HOST:PORT" target, defaulting an empty host to
// loopback.
func splitHostPort(target string) (string, int, error) {
	host, rawPort := "127.0.0.1", target
	if index := strings.LastIndex(target, ":"); index >= 0 {
		if index > 0 {
			host = target[:index]
		}
		rawPort = target[index+1:]
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		return "", 0, fmt.Errorf("tcp target must be HOST:PORT: %w", err)
	}
	return host, port, nil
}

// ---------------------------------------------------------------------------
// Calls
// ---------------------------------------------------------------------------

func (d *driver) opUnary(req *request) {
	if d.http == nil && d.tcp == nil {
		d.fail("not connected")
		return
	}
	batch, metadata, err := readOneBatch(req.RequestB64)
	if err != nil {
		d.fail(err.Error())
		return
	}
	defer batch.Release()
	method, err := methodOf(metadata)
	if err != nil {
		d.fail(err.Error())
		return
	}
	// expected is nil: the driver relays opaque batches and has no declared
	// result schema to enforce. The Python side owns value marshaling.
	var result *vgirpc.ClientBatch
	if d.http != nil {
		result, err = d.http.CallUnary(context.Background(), method, batch, nil)
	} else {
		result, err = d.tcp.CallUnary(context.Background(), method, batch, nil)
	}
	logs := d.drainLogs()
	if err != nil {
		d.respond(map[string]any{"ok": true, "result_b64": nil, "logs": logs, "error": errorJSON(err)})
		return
	}
	defer result.Release()
	encoded, err := writeOneBatch(result.Batch, result.Metadata)
	if err != nil {
		d.fail(err.Error())
		return
	}
	d.respond(map[string]any{"ok": true, "result_b64": encoded, "logs": logs, "error": nil})
}

func (d *driver) opDescribe() {
	if d.http == nil && d.tcp == nil {
		d.fail("not connected")
		return
	}
	var described *vgirpc.ClientServiceDescription
	var err error
	if d.http != nil {
		described, err = d.http.Describe(context.Background())
	} else {
		described, err = d.tcp.Describe(context.Background())
	}
	logs := d.drainLogs()
	if err != nil {
		d.respond(map[string]any{"ok": true, "describe": nil, "logs": logs, "error": errorJSON(err)})
		return
	}
	methods := make([]any, 0, len(described.Methods))
	for _, method := range described.Methods {
		methods = append(methods, map[string]any{
			"name":        method.Name,
			"method_type": method.MethodType,
			"has_return":  method.HasReturn,
			"has_header":  method.HasHeader,
			// null when the server genuinely cannot say, which is what
			// "unknown" means on the wire and what a unary method always is.
			"is_exchange": exchangeFlag(method.MethodType, method.StreamKind),
			// The server's own schema bytes, relayed rather than re-encoded:
			// only the schema message is read, and passing them through has one
			// fewer place for the two sides to disagree.
			"params_schema_b64": optionalBase64(method.ParamsSchemaIPC),
			"result_schema_b64": optionalBase64(method.ResultSchemaIPC),
			"header_schema_b64": optionalBase64(method.HeaderSchemaIPC),
		})
	}
	d.respond(map[string]any{"ok": true, "logs": logs, "error": nil, "describe": map[string]any{
		"protocol_name":    described.ProtocolName,
		"request_version":  described.RequestVersion,
		"describe_version": described.DescribeVersion,
		"protocol_hash":    described.ProtocolHash,
		"server_id":        described.ServerID,
		"protocol_version": described.ProtocolVersion,
		"methods":          methods,
	}})
}

// exchangeFlag renders a stream kind as the control protocol's tri-state.
func exchangeFlag(methodType, streamKind string) any {
	if methodType != "stream" {
		return nil
	}
	switch streamKind {
	case "exchange":
		return true
	case "producer":
		return false
	default:
		return nil
	}
}

// optionalBase64 encodes schema bytes, or null when the method declares none.
func optionalBase64(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return encodeBase64(raw)
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

// driverStream is the one stream shape the ops work against, over either
// transport's session type.
type driverStream interface {
	Header() *vgirpc.ClientBatch
	Next(ctx context.Context, custom map[string]string) (*vgirpc.ClientBatch, bool, error)
	Token() string
	Exchange(ctx context.Context, input arrow.RecordBatch) (*vgirpc.ClientBatch, error)
	Cancel(ctx context.Context) error
	Close(ctx context.Context)
}

type httpStream struct{ inner *vgirpc.HttpClientStream }

func (s httpStream) Header() *vgirpc.ClientBatch { return s.inner.Header() }
func (s httpStream) Next(ctx context.Context, custom map[string]string) (*vgirpc.ClientBatch, bool, error) {
	return s.inner.NextWithMetadata(ctx, custom)
}
func (s httpStream) Token() string { return s.inner.Token() }
func (s httpStream) Exchange(ctx context.Context, input arrow.RecordBatch) (*vgirpc.ClientBatch, error) {
	return s.inner.Exchange(ctx, input)
}
func (s httpStream) Cancel(ctx context.Context) error { return s.inner.Cancel(ctx) }
func (s httpStream) Close(context.Context)            { s.inner.Close() }

type tcpStream struct{ inner *vgirpc.TcpClientStream }

func (s tcpStream) Header() *vgirpc.ClientBatch { return s.inner.Header() }
func (s tcpStream) Next(ctx context.Context, custom map[string]string) (*vgirpc.ClientBatch, bool, error) {
	return s.inner.NextWithMetadata(ctx, custom)
}

// Token is always empty on a byte-stream transport: the stream is the
// connection, so there is no resumable state to hand back.
func (s tcpStream) Token() string { return "" }
func (s tcpStream) Exchange(ctx context.Context, input arrow.RecordBatch) (*vgirpc.ClientBatch, error) {
	return s.inner.Exchange(ctx, input)
}
func (s tcpStream) Cancel(ctx context.Context) error { return s.inner.Cancel(ctx) }
func (s tcpStream) Close(ctx context.Context)        { _ = s.inner.Close(ctx) }

func (d *driver) opStreamOpen(req *request) {
	if d.http == nil && d.tcp == nil {
		d.fail("not connected")
		return
	}
	if d.stream != nil {
		d.fail("a stream is already open")
		return
	}
	batch, metadata, err := readOneBatch(req.RequestB64)
	if err != nil {
		d.fail(err.Error())
		return
	}
	defer batch.Release()
	method, err := methodOf(metadata)
	if err != nil {
		d.fail(err.Error())
		return
	}
	// is_exchange comes from the protocol declaration and is authoritative.
	// Inferring the stream kind from the method name is a fixture-shaped
	// accident that does not survive the next method the suite adds.
	schemas := vgirpc.ClientStreamSchema{HasHeader: req.HasHeader}
	ctx := context.Background()
	var opened driverStream
	if d.http != nil {
		var session *vgirpc.HttpClientStream
		if req.IsExchange {
			session, err = d.http.OpenExchange(ctx, method, batch, schemas)
		} else {
			session, err = d.http.OpenProducer(ctx, method, batch, schemas)
		}
		if session != nil {
			opened = httpStream{session}
		}
	} else {
		var session *vgirpc.TcpClientStream
		if req.IsExchange {
			session, err = d.tcp.OpenExchange(ctx, method, batch, schemas)
		} else {
			session, err = d.tcp.OpenProducer(ctx, method, batch, schemas)
		}
		if session != nil {
			opened = tcpStream{session}
		}
	}
	logs := d.drainLogs()
	if err != nil {
		// The driver did carry out the op; the server refused. That is the
		// call-error channel, and leaving no stream open is the other half of
		// it.
		if opened != nil {
			opened.Close(ctx)
		}
		d.respond(map[string]any{"ok": true, "header_b64": nil, "logs": logs, "error": errorJSON(err)})
		return
	}
	d.stream = opened
	var header any
	if batchHeader := opened.Header(); batchHeader != nil {
		defer batchHeader.Release()
		encoded, encodeErr := writeOneBatch(batchHeader.Batch, batchHeader.Metadata)
		if encodeErr != nil {
			// The stream opened but this driver cannot relay its header, so
			// the harness will never drive it. Retire it here rather than
			// leaving a stream open that nothing will ever close.
			d.retireStream()
			d.fail(encodeErr.Error())
			return
		}
		header = encoded
	}
	d.respond(map[string]any{"ok": true, "header_b64": header, "logs": logs, "error": nil})
}

func (d *driver) opTick(req *request) {
	if d.stream == nil {
		d.fail("no stream is open")
		return
	}
	// An input on a tick carries an empty batch whose custom metadata is the
	// per-tick metadata; the batch itself is ignored.
	var custom map[string]string
	if req.InputB64 != "" {
		batch, metadata, err := readOneBatch(req.InputB64)
		if err != nil {
			d.fail(err.Error())
			return
		}
		batch.Release()
		custom = metadata
	}
	batch, ok, err := d.stream.Next(context.Background(), custom)
	d.respondStreamItem(batch, ok, err, false, nil)
}

func (d *driver) opNextWithToken() {
	if d.stream == nil {
		d.fail("no stream is open")
		return
	}
	batch, ok, err := d.stream.Next(context.Background(), nil)
	// Always reported, null included: a byte-stream transport carries no
	// resumable stream state, and "the field is absent" and "there is no
	// token" are different claims.
	token := d.stream.Token()
	var reported any
	if token != "" {
		reported = token
	}
	d.respondStreamItem(batch, ok, err, true, reported)
}

func (d *driver) opExchange(req *request) {
	if d.stream == nil {
		d.fail("no stream is open")
		return
	}
	batch, _, err := readOneBatch(req.InputB64)
	if err != nil {
		d.fail(err.Error())
		return
	}
	defer batch.Release()
	reply, err := d.stream.Exchange(context.Background(), batch)
	d.respondStreamItem(reply, reply != nil, err, false, nil)
}

// respondStreamItem renders one stream turn and retires the stream when the
// turn was terminal.
//
// End-of-stream and an error are both terminal; an error is additionally
// reported as done, because there is nothing further to read.
func (d *driver) respondStreamItem(batch *vgirpc.ClientBatch, ok bool, err error, withToken bool, token any) {
	logs := d.drainLogs()
	response := map[string]any{"ok": true, "logs": logs}
	if withToken {
		response["token"] = token
	}
	switch {
	case err != nil:
		// Released here rather than relied on being nil: no client path
		// returns a batch beside an error today, and a driver that leaked one
		// if a client ever did would show up as a memory figure, not a bug.
		if batch != nil {
			batch.Release()
		}
		d.retireStream()
		response["done"] = true
		response["batch_b64"] = nil
		response["error"] = errorJSON(err)
	case !ok || batch == nil:
		d.retireStream()
		response["done"] = true
		response["batch_b64"] = nil
		response["error"] = nil
	default:
		defer batch.Release()
		encoded, encodeErr := writeOneBatch(batch.Batch, batch.Metadata)
		if encodeErr != nil {
			d.fail(encodeErr.Error())
			return
		}
		response["done"] = false
		response["batch_b64"] = encoded
		response["error"] = nil
	}
	d.respond(response)
}

// retireStream releases a stream that has reached a terminal event.
func (d *driver) retireStream() {
	if d.stream == nil {
		return
	}
	d.stream.Close(context.Background())
	d.stream = nil
}

func (d *driver) opCancel() {
	// A cancel with no stream open is a successful no-op.
	if d.stream == nil {
		d.respond(map[string]any{"ok": true, "logs": d.drainLogs()})
		return
	}
	_ = d.stream.Cancel(context.Background())
	d.retireStream()
	d.respond(map[string]any{"ok": true, "logs": d.drainLogs()})
}

func (d *driver) opClose() {
	d.retireStream()
	d.respond(map[string]any{"ok": true})
}

// ---------------------------------------------------------------------------
// HTTP-only ops
// ---------------------------------------------------------------------------

func (d *driver) opHTTPAdmin(req *request) {
	if d.http == nil {
		if d.tcp != nil {
			d.fail("op requires http transport")
			return
		}
		d.fail("not connected")
		return
	}
	ctx := context.Background()
	switch req.Op {
	case "capabilities":
		caps, err := d.http.DiscoverCapabilities(ctx)
		if err != nil {
			d.fail(err.Error())
			return
		}
		d.respond(map[string]any{"ok": true, "caps": map[string]any{
			"sticky_enabled":                  caps.StickyEnabled,
			"sticky_default_ttl":              optionalInt(caps.StickyDefaultTTL),
			"sticky_echo_headers":             stringList(caps.StickyEchoHeaders),
			"upload_url_support":              caps.UploadURLSupport,
			"max_request_bytes":               optionalInt(caps.MaxRequestBytes),
			"max_response_bytes":              optionalInt(caps.MaxResponseBytes),
			"max_externalized_response_bytes": optionalInt(caps.MaxExternalizedResponseBytes),
			"externalization_enabled":         caps.ExternalizationEnabled,
			"max_upload_bytes":                optionalInt(caps.MaxUploadBytes),
			"supported_encodings":             stringList(caps.SupportedEncodings),
		}})
	case "request_upload_urls":
		count := req.Count
		if count < 1 {
			count = 1
		}
		urls, err := d.http.RequestUploadURLs(ctx, count)
		if err != nil {
			d.fail(err.Error())
			return
		}
		rendered := make([]any, 0, len(urls))
		for _, url := range urls {
			rendered = append(rendered, map[string]any{
				"upload_url":   url.UploadURL,
				"download_url": url.DownloadURL,
				"expires_at":   url.ExpiresAt.Unix(),
			})
		}
		d.respond(map[string]any{"ok": true, "urls": rendered})
	case "session_begin":
		token := ""
		if req.Token != nil {
			token = *req.Token
		}
		d.http.BeginSession(token)
		d.respond(map[string]any{"ok": true})
	case "session_token":
		d.respond(map[string]any{"ok": true, "token": optionalString(d.http.CurrentSessionToken())})
	case "session_echo_headers":
		d.respond(map[string]any{"ok": true, "headers": d.http.CurrentEchoHeaders()})
	case "session_detach":
		d.respond(map[string]any{"ok": true, "token": optionalString(d.http.DetachSession())})
	case "session_end":
		d.http.EndSession(ctx)
		d.respond(map[string]any{"ok": true})
	default:
		d.fail("unknown admin op: " + req.Op)
	}
}

// optionalInt renders an unadvertised byte cap or TTL as null rather than 0.
func optionalInt(value int64) any {
	if value <= 0 {
		return nil
	}
	return value
}

// optionalString renders an absent token as null rather than "".
func optionalString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// stringList renders an empty list as [] rather than null.
func stringList(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
