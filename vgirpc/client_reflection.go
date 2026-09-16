// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// The client half of vgi_rpc.Reflection.v1; reflection.go is the server half.
//
// Nothing here is a special transport path: introspection is an ordinary
// co-hosted protocol, so a client asks for it with the same unary call it uses
// for anything else. The single difference is the routing key -- these calls
// address vgi_rpc.Reflection.v1 rather than the protocol the client was
// configured for, which is the whole reason callUnaryOn exists on both
// transports. A client that could only call its own protocol would have to be
// configured for reflection to discover what it should have been configured
// for.
//
// The decoder is the part that carries the format's guarantee. Minor skew must
// be survivable, which the header of reflection.go states as a rule binding
// every port: read by field name, ignore columns you do not know, and default
// columns that are absent. So this decodes by walking the LOCAL struct's fields
// and looking each one up by name -- a column this build has never heard of is
// then not merely tolerated but unreachable, and a field the peer did not send
// takes its declared default, or its zero value for the fields of these three
// types, every one of which is documented empty-rather-than-null when absent.
// deserializeParams is deliberately not reused: it compares whole schemas with
// Schema.Equal, which is right for a declared parameter contract and wrong
// here, where one added column on a newer server would fail the call that
// exists to explain what the newer server is.

// DescribeVersion is the version of the introspection FORMAT, not of the
// introspection protocol.
//
// It does not move. Introspection's own major version lives in its protocol
// name (vgi_rpc.Reflection.v1), where a mismatch is a routing failure a client
// can act on; a second version number that also had to be checked would give
// the same fact two homes that can disagree.
const DescribeVersion = "5"

// ClientServiceDescription is what a client learned about a server.
//
// It joins the two hops: the protocol's own identity and methods come from
// describe, while ServerID and RequestVersion come from list_protocols, because
// they describe a server rather than a protocol -- two processes serving the
// same protocol must describe it identically or the description is not a
// property of the protocol.
type ClientServiceDescription struct {
	ProtocolName    string
	ProtocolVersion string
	ProtocolHash    string
	RequestVersion  string
	ServerID        string
	DescribeVersion string
	Methods         []ClientMethodDescription
}

// ClientMethodDescription is one method's wire surface as the client received
// it.
//
// The schema fields carry the server's own IPC bytes relayed verbatim. They are
// not re-encoded from a decoded schema: a caller's whole purpose in asking is
// to hand those bytes to its own Arrow implementation, and re-encoding would
// substitute this build's serializer for the peer's without being able to
// promise the result still describes what the peer meant. Equality across ports
// is the protocol hash's job, taken over the decoded structure precisely so
// these bytes need not match.
type ClientMethodDescription struct {
	Name       string
	MethodType string
	HasReturn  bool
	HasHeader  bool
	StreamKind string

	ParamsSchemaIPC []byte
	ResultSchemaIPC []byte
	HeaderSchemaIPC []byte

	Idempotency        string
	Deprecated         bool
	DeprecationMessage string
}

// reflectionCaller is the one capability introspection needs from a transport:
// a unary call addressed to a protocol other than the client's own.
//
// Both HTTP and TCP clients satisfy it with an unexported method, so the two
// transports share one implementation of the two-hop conversation rather than
// each growing a copy that can drift.
type reflectionCaller interface {
	callUnaryOn(ctx context.Context, protocol, method string, params arrow.RecordBatch,
		expected *arrow.Schema) (*ClientBatch, error)
}

// ListProtocols returns what the server hosts, without any protocol's methods.
//
// The cheap hop: a client that already knows a protocol hash can compare it
// here and skip describe entirely.
func (c *HttpClient) ListProtocols(ctx context.Context) (*ProtocolListDesc, error) {
	return reflectionListProtocols(ctx, c)
}

// DescribeProtocol returns the full description of one hosted protocol.
func (c *HttpClient) DescribeProtocol(ctx context.Context, protocol string) (*ServiceDescriptionDesc, error) {
	return reflectionDescribeProtocol(ctx, c, protocol)
}

// Describe returns the server's application protocol, discovered rather than
// named: it lists what the server hosts and describes the first protocol that
// is not framework-owned. Use [HttpClient.DescribeProtocol] when the caller
// knows which of several protocols it wants.
func (c *HttpClient) Describe(ctx context.Context) (*ClientServiceDescription, error) {
	return reflectionDescribe(ctx, c)
}

// ListProtocols returns what the server hosts, without any protocol's methods.
//
// The cheap hop: a client that already knows a protocol hash can compare it
// here and skip describe entirely.
func (client *TcpClient) ListProtocols(ctx context.Context) (*ProtocolListDesc, error) {
	return reflectionListProtocols(ctx, client)
}

// DescribeProtocol returns the full description of one hosted protocol.
func (client *TcpClient) DescribeProtocol(ctx context.Context, protocol string) (*ServiceDescriptionDesc, error) {
	return reflectionDescribeProtocol(ctx, client, protocol)
}

// Describe returns the server's application protocol, discovered rather than
// named: it lists what the server hosts and describes the first protocol that
// is not framework-owned. Use [TcpClient.DescribeProtocol] when the caller
// knows which of several protocols it wants.
func (client *TcpClient) Describe(ctx context.Context) (*ClientServiceDescription, error) {
	return reflectionDescribe(ctx, client)
}

// reflectionListProtocols performs the list_protocols hop.
func reflectionListProtocols(ctx context.Context, caller reflectionCaller) (*ProtocolListDesc, error) {
	out := &ProtocolListDesc{}
	if err := reflectionUnary(ctx, caller, "list_protocols", listProtocolsParams{}, out); err != nil {
		return nil, err
	}
	return out, nil
}

// reflectionDescribeProtocol performs the describe hop for one protocol.
func reflectionDescribeProtocol(ctx context.Context, caller reflectionCaller,
	protocol string) (*ServiceDescriptionDesc, error) {
	out := &ServiceDescriptionDesc{}
	if err := reflectionUnary(ctx, caller, "describe", describeParams{Protocol: protocol}, out); err != nil {
		return nil, err
	}
	return out, nil
}

// reflectionDescribe runs both hops and joins them.
//
// Framework protocols are skipped rather than reported, because a client asking
// "what does this server do" means the application surface; vgi_rpc.Reflection.v1
// describes only how to ask the question. A server hosting nothing else is an
// error and not an empty description: an empty method list reads as "this
// protocol has no methods", which is a different and much more alarming fact
// than "there is no application protocol here to describe".
func reflectionDescribe(ctx context.Context, caller reflectionCaller) (*ClientServiceDescription, error) {
	list, err := reflectionListProtocols(ctx, caller)
	if err != nil {
		return nil, err
	}
	chosen := ""
	hosted := make([]string, 0, len(list.Protocols))
	for _, summary := range list.Protocols {
		hosted = append(hosted, summary.Protocol)
		if chosen == "" && !strings.HasPrefix(summary.Protocol, reservedProtocolPrefix) {
			chosen = summary.Protocol
		}
	}
	if chosen == "" {
		return nil, &RpcError{
			Type: "ProtocolError",
			Message: fmt.Sprintf(
				"server hosts no application protocol to describe; it lists only framework protocols [%s]",
				strings.Join(hosted, ", ")),
		}
	}
	desc, err := reflectionDescribeProtocol(ctx, caller, chosen)
	if err != nil {
		return nil, err
	}
	out := &ClientServiceDescription{
		ProtocolName:    desc.Protocol,
		ProtocolVersion: desc.ProtocolVersion,
		ProtocolHash:    desc.ProtocolHash,
		RequestVersion:  list.RequestVersion,
		ServerID:        list.ServerID,
		DescribeVersion: DescribeVersion,
		Methods:         make([]ClientMethodDescription, 0, len(desc.Methods)),
	}
	for _, method := range desc.Methods {
		// MethodType is relayed, not re-mapped: the server's spelling is the
		// answer, and a client that normalized it would hide a peer that spells
		// it some third way rather than report it.
		//
		//lint:ignore S1016 the wire type and the client-facing type are
		// deliberately independent; a struct conversion would make them one
		// type in two names, so adding a field to the wire struct -- which the
		// format explicitly allows in a minor version -- would fail to compile
		// until the public type grew it too, and would publish it whether or
		// not it belongs in the public surface.
		out.Methods = append(out.Methods, ClientMethodDescription{
			Name:               method.Name,
			MethodType:         method.MethodType,
			HasReturn:          method.HasReturn,
			HasHeader:          method.HasHeader,
			StreamKind:         method.StreamKind,
			ParamsSchemaIPC:    method.ParamsSchemaIPC,
			ResultSchemaIPC:    method.ResultSchemaIPC,
			HeaderSchemaIPC:    method.HeaderSchemaIPC,
			Idempotency:        method.Idempotency,
			Deprecated:         method.Deprecated,
			DeprecationMessage: method.DeprecationMessage,
		})
	}
	return out, nil
}

// reflectionUnary calls one reflection method and decodes its struct result
// into out, which must be a pointer to a struct.
//
// The expected result schema is nil -- unenforced -- on purpose. Everywhere
// else a caller with a compiled declaration should pass it, because an
// unenforced schema is a type error found later and further away. Here the
// point of the call is to tolerate a peer built from a different revision of
// the format, and Schema.Equal against this build's declaration would reject
// exactly the peers introspection exists to explain.
func reflectionUnary(ctx context.Context, caller reflectionCaller, method string, params any, out any) error {
	batch, err := reflectionParamsBatch(params)
	if err != nil {
		return err
	}
	defer batch.Release()

	reply, err := caller.callUnaryOn(ctx, ReflectionProtocolName, method, batch, nil)
	if err != nil {
		return err
	}
	defer reply.Release()

	if err := decodeReflectionResult(reply.Batch, out); err != nil {
		return &RpcError{
			Type:    "ProtocolError",
			Message: fmt.Sprintf("decoding the %s reply from %s: %v", method, ReflectionProtocolName, err),
		}
	}
	return nil
}

// reflectionParamsBatch builds the one-row parameter batch for a tagged struct.
//
// The schema comes from the same derivation the server validates against, so a
// parameter batch built here and a parameter schema derived there cannot
// disagree about column order, type, or nullability. An empty parameter struct
// yields a zero-column, zero-row batch rather than a one-row one: there is no
// column to carry a row.
func reflectionParamsBatch(params any) (arrow.RecordBatch, error) {
	rt := reflect.TypeOf(params)
	schema, err := SchemaForStruct(rt)
	if err != nil {
		return nil, &RpcError{
			Type:    "ProtocolError",
			Message: fmt.Sprintf("deriving the parameter schema for %s: %v", rt, err),
		}
	}
	desc := describeStruct(rt)
	if desc.Err != nil {
		return nil, &RpcError{
			Type:    "ProtocolError",
			Message: fmt.Sprintf("describing the parameter struct %s: %v", rt, desc.Err),
		}
	}
	if len(desc.Fields) == 0 {
		return array.NewRecordBatch(schema, nil, 0), nil
	}

	mem := defaultAllocator()
	value := reflect.ValueOf(params)
	columns := make([]arrow.Array, 0, len(desc.Fields))
	// NewRecordBatch retains what it is given, so the builder's own references
	// are released either way -- on the error path and on the success path.
	defer func() {
		for _, column := range columns {
			column.Release()
		}
	}()
	for ord, field := range desc.Fields {
		column, err := buildArray(mem, schema.Field(ord).Type, value.Field(field.Index).Interface())
		if err != nil {
			return nil, &RpcError{
				Type:    "ProtocolError",
				Message: fmt.Sprintf("encoding reflection parameter %q: %v", field.Info.Name, err),
			}
		}
		columns = append(columns, column)
	}
	return array.NewRecordBatch(schema, columns, 1), nil
}

// decodeReflectionResult reads a unary reply whose result type is a struct.
//
// Such a result is one binary column named "result" holding a complete IPC
// stream of a one-row batch of the struct's flat columns; the wrapping is what
// lets a result of any shape occupy the single result column every unary reply
// has.
func decodeReflectionResult(batch arrow.RecordBatch, target any) error {
	if batch == nil {
		return fmt.Errorf("the reply carried no record batch")
	}
	column := reflectionColumn(batch, "result")
	if column == nil {
		return fmt.Errorf("the reply has no %q column; it carries [%s]", "result",
			strings.Join(reflectionColumnNames(batch), ", "))
	}
	var payload []byte
	switch typed := column.(type) {
	case *array.Binary:
		if typed.Len() == 0 || typed.IsNull(0) {
			return fmt.Errorf("the reply's result column is empty")
		}
		payload = typed.Value(0)
	case *array.LargeBinary:
		if typed.Len() == 0 || typed.IsNull(0) {
			return fmt.Errorf("the reply's result column is empty")
		}
		payload = typed.Value(0)
	default:
		return fmt.Errorf("the reply's result column is %s, want binary", column.DataType())
	}

	reader, err := ipc.NewReader(bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("the result column is not an Arrow IPC stream: %w", err)
	}
	defer reader.Release()
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return fmt.Errorf("reading the result stream: %w", err)
		}
		return fmt.Errorf("the result stream carried no record batch")
	}
	inner := reader.RecordBatch()
	if inner.NumRows() < 1 {
		return fmt.Errorf("the result stream's batch has no rows")
	}
	out := reflect.ValueOf(target)
	if out.Kind() != reflect.Ptr || out.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("decode target must be a pointer to a struct, got %T", target)
	}
	return decodeReflectionStruct(out.Elem(), reflectionRecordLookup(inner), 0)
}

// decodeReflectionStruct fills target from whatever columns the peer sent.
//
// The walk is over the LOCAL struct's fields rather than over the peer's
// columns, which is what makes an unknown column unreachable rather than merely
// tolerated. A field whose column is absent or null takes its declared default,
// and its zero value when it declares none: every field of the reflection types
// is documented empty-rather-than-null when absent, so the zero value is the
// documented answer and refusing the whole description over it would turn the
// minor skew this format promises to survive into a hard failure of the one
// call that can explain the skew.
func decodeReflectionStruct(target reflect.Value, column func(string) arrow.Array, row int) error {
	desc := describeStruct(target.Type())
	if desc.Err != nil {
		return fmt.Errorf("describing %s: %w", target.Type(), desc.Err)
	}
	for _, field := range desc.Fields {
		col := column(field.Info.Name)
		if col == nil || row >= col.Len() || col.IsNull(row) {
			if field.Info.Default == nil {
				continue
			}
			if err := setFieldFromString(target.Field(field.Index), field.Type, *field.Info.Default); err != nil {
				return fmt.Errorf("default for field %q: %w", field.Info.Name, err)
			}
			continue
		}
		if err := setReflectionField(target.Field(field.Index), col, row); err != nil {
			return fmt.Errorf("field %q: %w", field.Info.Name, err)
		}
	}
	return nil
}

// setReflectionField writes one Arrow value into one Go field.
//
// Only the column types the reflection payload actually uses are handled. A
// column of some other type is an error rather than a skip: an unknown COLUMN
// is skew this format promises to survive, but a known field arriving as a type
// this build cannot read is a disagreement about the field itself, and
// zero-filling it would hand the caller a description that is wrong rather than
// absent.
func setReflectionField(target reflect.Value, column arrow.Array, row int) error {
	switch typed := column.(type) {
	case *array.String:
		return setReflectionString(target, typed.Value(row))
	case *array.LargeString:
		return setReflectionString(target, typed.Value(row))
	case *array.Boolean:
		if target.Kind() != reflect.Bool {
			return fmt.Errorf("column is boolean but the field is %s", target.Kind())
		}
		target.SetBool(typed.Value(row))
		return nil
	case *array.Binary:
		return setReflectionBytes(target, typed.Value(row))
	case *array.LargeBinary:
		return setReflectionBytes(target, typed.Value(row))
	case *array.List:
		return setReflectionList(target, typed, row)
	default:
		return fmt.Errorf("column type %s is not part of the reflection payload", column.DataType())
	}
}

// setReflectionString copies a string out of the Arrow buffer that holds it.
//
// arrow-go hands out a string that aliases the record batch's value buffer, and
// the batch is released as soon as the description is decoded. Cloning is what
// keeps a returned description from pointing at memory the caller no longer
// owns a reference to.
func setReflectionString(target reflect.Value, value string) error {
	if target.Kind() != reflect.String {
		return fmt.Errorf("column is a string but the field is %s", target.Kind())
	}
	target.SetString(strings.Clone(value))
	return nil
}

// setReflectionBytes copies bytes out of the Arrow buffer that holds them, for
// the same reason [setReflectionString] clones.
func setReflectionBytes(target reflect.Value, value []byte) error {
	if target.Kind() != reflect.Slice || target.Type().Elem().Kind() != reflect.Uint8 {
		return fmt.Errorf("column is binary but the field is %s", target.Type())
	}
	target.SetBytes(append([]byte(nil), value...))
	return nil
}

// setReflectionList fills a slice field from a list column, decoding a list of
// structs element by element under the same by-name leniency as the record that
// carries it.
func setReflectionList(target reflect.Value, column *array.List, row int) error {
	if target.Kind() != reflect.Slice {
		return fmt.Errorf("column is a list but the field is %s", target.Kind())
	}
	start, end := column.ValueOffsets(row)
	values := column.ListValues()
	out := reflect.MakeSlice(target.Type(), int(end-start), int(end-start))
	for idx := start; idx < end; idx++ {
		element := out.Index(int(idx - start))
		if values.IsNull(int(idx)) {
			continue
		}
		if children, ok := values.(*array.Struct); ok {
			if element.Kind() != reflect.Struct {
				return fmt.Errorf("list items are structs but the slice element is %s", element.Kind())
			}
			if err := decodeReflectionStruct(element, reflectionStructLookup(children), int(idx)); err != nil {
				return fmt.Errorf("list item %d: %w", idx-start, err)
			}
			continue
		}
		if err := setReflectionField(element, values, int(idx)); err != nil {
			return fmt.Errorf("list item %d: %w", idx-start, err)
		}
	}
	target.Set(out)
	return nil
}

// reflectionRecordLookup resolves a record batch's columns by name.
func reflectionRecordLookup(batch arrow.RecordBatch) func(string) arrow.Array {
	return func(name string) arrow.Array {
		return reflectionColumn(batch, name)
	}
}

// reflectionStructLookup resolves a struct column's children by name.
func reflectionStructLookup(column *array.Struct) func(string) arrow.Array {
	structType, ok := column.DataType().(*arrow.StructType)
	return func(name string) arrow.Array {
		if !ok {
			return nil
		}
		idx, found := structType.FieldIdx(name)
		if !found {
			return nil
		}
		return column.Field(idx)
	}
}

// reflectionColumn returns the named column, or nil when the peer did not send
// one.
func reflectionColumn(batch arrow.RecordBatch, name string) arrow.Array {
	for idx := range int(batch.NumCols()) {
		if batch.ColumnName(idx) == name {
			return batch.Column(idx)
		}
	}
	return nil
}

// reflectionColumnNames lists what a batch does carry, for an error message
// about what it does not.
func reflectionColumnNames(batch arrow.RecordBatch) []string {
	names := make([]string, 0, batch.NumCols())
	for idx := range int(batch.NumCols()) {
		names = append(names, batch.ColumnName(idx))
	}
	return names
}
