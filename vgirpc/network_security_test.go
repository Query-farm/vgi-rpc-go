// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func networkSecurityRequest(t *testing.T, method string, batch arrow.RecordBatch, extra map[string]string) []byte {
	t.Helper()
	metadata := map[string]string{MetaMethod: method, MetaProtocol: testProtocol, MetaRequestVersion: ProtocolVersion}
	for key, value := range extra {
		metadata[key] = value
	}
	return regressionIPC(t, batch, arrow.MetadataFrom(metadata))
}

func TestNetworkTransportNeverAdvertisesSharedMemory(t *testing.T) {
	for _, network := range []bool{false, true} {
		t.Run(strconv.FormatBool(network), func(t *testing.T) {
			server := NewServer()
			batch := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 0)
			defer batch.Release()
			request := bytes.NewReader(networkSecurityRequest(t, "__transport_options__", batch, nil))
			var response bytes.Buffer
			if network {
				server.ServeNetworkWithContext(context.Background(), request, &response)
			} else {
				server.ServeWithContext(context.Background(), request, &response)
			}
			reader, err := ipc.NewReader(&response)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Release()
			if !reader.Next() {
				t.Fatalf("missing transport options: %v", reader.Err())
			}
			metadata := reader.RecordBatch().(arrow.RecordBatchWithMetadata).Metadata()
			if got, _ := metadata.GetValue(MetaTransportShm); got != strconv.FormatBool(shmSupported && !network) {
				t.Fatalf("shm advertisement = %q", got)
			}
			wantKind := TransportKindPipe
			if network {
				wantKind = TransportKindTcp
			}
			if server.TransportKind() != wantKind {
				t.Fatalf("transport = %q, want %q", server.TransportKind(), wantKind)
			}
		})
	}
}

func TestNetworkRejectsSharedMemoryMetadataBeforeDispatch(t *testing.T) {
	keys := []string{MetaShmSegmentName, MetaShmSegmentSize, MetaShmOffset, MetaShmLength, MetaShmSource, "vgi_rpc.shm_future"}
	for _, key := range keys {
		for _, schemaMetadata := range []bool{false, true} {
			t.Run(key+"/schema="+strconv.FormatBool(schemaMetadata), func(t *testing.T) {
				server := NewServer()
				calls := 0
				Unary(server, "probe", func(_ context.Context, _ *CallContext, p regressionParams) (regressionResult, error) {
					calls++
					return regressionResult(p), nil
				})
				batch := regressionBatch(t, 1)
				defer batch.Release()
				extra := map[string]string{key: "1"}
				if schemaMetadata {
					metadata := arrow.MetadataFrom(extra)
					schema := arrow.NewSchema(batch.Schema().Fields(), &metadata)
					annotated := array.NewRecordBatch(schema, batch.Columns(), batch.NumRows())
					defer annotated.Release()
					batch = annotated
					extra = nil
				}
				data := networkSecurityRequest(t, "probe", batch, extra)
				data = append(data, networkSecurityRequest(t, "probe", batch, nil)...)
				request := bytes.NewReader(data)
				var response bytes.Buffer
				server.ServeNetworkWithContext(context.Background(), request, &response)
				if calls != 0 || request.Len() == 0 {
					t.Fatalf("unsafe request dispatched or following request consumed: calls=%d remaining=%d", calls, request.Len())
				}
				if !strings.Contains(response.String(), "shared memory is disabled") {
					t.Fatal("missing shared-memory rejection")
				}
			})
		}
	}
}

func TestNetworkCannotAttachAdvertisedLocalSegment(t *testing.T) {
	if !shmSupported {
		t.Skip("platform does not support shared memory")
	}
	segment, err := ShmCreate(ShmHeaderSize + 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer segment.Close()
	metadata := map[string]string{MetaShmSegmentName: segment.Name(), MetaShmSegmentSize: strconv.Itoa(segment.Size())}
	connection := &shmConnState{disabled: true}
	if connection.ensure(metadata) != nil || connection.seg != nil {
		t.Fatal("network connection attached a local shared-memory segment")
	}
	server := NewServer()
	calls := 0
	Unary(server, "probe", func(_ context.Context, _ *CallContext, p regressionParams) (regressionResult, error) {
		calls++
		return regressionResult(p), nil
	})
	params := regressionBatch(t, 1)
	defer params.Release()
	column := array.NewSlice(params.Column(0), 0, 0)
	defer column.Release()
	pointer := array.NewRecordBatch(regressionSchema, []arrow.Array{column}, 0)
	defer pointer.Release()
	metadata[MetaShmOffset] = strconv.Itoa(ShmHeaderSize)
	metadata[MetaShmLength] = "1"
	var response bytes.Buffer
	err = server.serveOne(context.Background(), bytes.NewReader(networkSecurityRequest(t, "probe", pointer, metadata)), &response, connection)
	if err == nil || connection.seg != nil || calls != 0 || !strings.Contains(response.String(), "shared memory is disabled") {
		t.Fatalf("pointer request reached local memory or dispatch: attached=%t calls=%d error=%v", connection.seg != nil, calls, err)
	}
	// Local pipe/Unix connections retain their existing attachment semantics.
	local := &shmConnState{}
	defer local.close()
	if local.ensure(metadata) == nil {
		t.Fatal("local shared-memory attachment no longer works")
	}
}

func TestNetworkRejectsSharedMemoryStreamContinuations(t *testing.T) {
	for _, producer := range []bool{false, true} {
		for _, key := range []string{MetaShmSegmentName, MetaShmSegmentSize, MetaShmOffset, MetaShmLength, MetaShmSource} {
			t.Run(strconv.FormatBool(producer)+"/"+key, func(t *testing.T) {
				server := NewServer()
				hook := &regressionHook{}
				server.SetDispatchHook(hook)
				state := &networkSecurityState{}
				handler := func(context.Context, *CallContext, regressionParams) (*StreamResult, error) {
					return &StreamResult{OutputSchema: regressionSchema, InputSchema: regressionSchema, State: state}, nil
				}
				if producer {
					Producer(server, "stream", regressionSchema, handler)
				} else {
					Exchange(server, "stream", regressionSchema, regressionSchema, handler)
				}
				params := regressionBatch(t, 1)
				defer params.Release()
				data := networkSecurityRequest(t, "stream", params, nil)
				// Include cancellation to ensure it cannot bypass the pointer guard.
				metadata := arrow.MetadataFrom(map[string]string{key: "1", MetaCancel: "true"})
				data = append(data, regressionIPC(t, params, metadata)...)
				data = append(data, networkSecurityRequest(t, "stream", params, nil)...)
				request := bytes.NewReader(data)
				var response bytes.Buffer
				server.ServeNetworkWithContext(context.Background(), request, &response)
				if state.calls != 0 || state.cancels != 0 || request.Len() == 0 {
					t.Fatalf("unsafe continuation dispatched or drained: calls=%d cancels=%d remaining=%d", state.calls, state.cancels, request.Len())
				}
				if hook.ends != 1 || hook.lastErr == nil || !strings.Contains(hook.lastErr.Error(), "shared memory is disabled") {
					t.Fatalf("dispatch cleanup did not receive rejection: ends=%d error=%v", hook.ends, hook.lastErr)
				}
				if !strings.Contains(response.String(), "shared memory is disabled") {
					t.Fatal("missing shared-memory rejection")
				}
			})
		}
	}
}

type networkSecurityState struct{ calls, cancels int }

func (s *networkSecurityState) Produce(context.Context, *OutputCollector, *CallContext) error {
	s.calls++
	return nil
}

func (s *networkSecurityState) Exchange(context.Context, arrow.RecordBatch, *OutputCollector, *CallContext) error {
	s.calls++
	return nil
}

func (s *networkSecurityState) OnCancel(context.Context, *CallContext) error {
	s.cancels++
	return nil
}

func TestNetworkPreservesVerifiedConnectionIdentity(t *testing.T) {
	server := NewServer()
	calls := 0
	Unary(server, "probe", func(_ context.Context, call *CallContext, p regressionParams) (regressionResult, error) {
		calls++
		if call.Auth.Principal != "verified-peer" || !call.Auth.Authenticated || call.Kind != TransportKindTcp {
			t.Fatalf("missing verified identity: %#v, transport=%q", call.Auth, call.Kind)
		}
		return regressionResult(p), nil
	})
	ctx, err := WithConnectionIdentity(context.Background(), &AuthContext{Authenticated: true, Principal: "verified-peer"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	batch := regressionBatch(t, 1)
	defer batch.Release()
	request := networkSecurityRequest(t, "probe", batch, nil)
	request = append(request, request...)
	var response bytes.Buffer
	server.ServeNetworkWithContext(ctx, bytes.NewReader(request), &response)
	if calls != 2 {
		t.Fatalf("connection reuse dispatched %d calls, want 2", calls)
	}
}

func TestTCPListenerDisablesSharedMemory(t *testing.T) {
	server := NewServer()
	client, connection := net.Pipe()
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer connection.Close()
		server.serveTcpConn(context.Background(), connection)
	}()
	batch := regressionBatch(t, 1)
	defer batch.Release()
	request := networkSecurityRequest(t, "probe", batch, map[string]string{MetaShmSegmentName: "forbidden"})
	if _, err := client.Write(request); err != nil {
		t.Fatal(err)
	}
	response, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	<-done
	if !bytes.Contains(response, []byte("shared memory is disabled")) {
		t.Fatal("TCP serving did not reject shared memory")
	}
}
