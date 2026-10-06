// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
)

// The error model: a canonical code, an open reason, and typed details.
//
// Every EXCEPTION batch carries three layers, adopted from gRPC's
// google.rpc.Status (WIRE_PROTOCOL.md §8):
//
//	Code     vgi_rpc.error_code     closed: gRPC's sixteen codes minus OK, sent by NAME
//	Reason   vgi_rpc.error_kind     open, unique within the protocol that raised it
//	Details  vgi_rpc.error_details  a JSON array of typed objects from a fixed catalog
//
// The code is what generic handling keys on (retry or not, how to show it);
// the kind is what a client branches on; the details carry machine-readable
// specifics. All three are plain top-level metadata and are mirrored into
// log_extra, and the details array is capped at 4 KiB -- dropped WHOLE when
// over, never trimmed, because a client cannot tell a partial list from a
// complete one.
//
// A server raises a [*StatusError], or any error implementing ErrorCode() /
// ErrorKind() / ErrorDetails(); a client reads [RpcError.Code],
// [RpcError.Kind] and [RpcError.Details] plus the typed accessors
// ([RpcError.RetryInfo] and friends) and [RpcError.IsRetryable].

// Code is a canonical error code's wire name, e.g. "UNAVAILABLE".
//
// The wire value is the name, never gRPC's number, so a log line, a proxy rule
// and a client switch all read the same string. The set is closed: a client
// that receives any other value treats it as [CodeUnknown] ([ParseCode]).
type Code string

// The sixteen canonical codes: gRPC's, minus OK.
const (
	CodeCancelled          Code = "CANCELLED"
	CodeUnknown            Code = "UNKNOWN"
	CodeInvalidArgument    Code = "INVALID_ARGUMENT"
	CodeDeadlineExceeded   Code = "DEADLINE_EXCEEDED"
	CodeNotFound           Code = "NOT_FOUND"
	CodeAlreadyExists      Code = "ALREADY_EXISTS"
	CodePermissionDenied   Code = "PERMISSION_DENIED"
	CodeResourceExhausted  Code = "RESOURCE_EXHAUSTED"
	CodeFailedPrecondition Code = "FAILED_PRECONDITION"
	CodeAborted            Code = "ABORTED"
	CodeOutOfRange         Code = "OUT_OF_RANGE"
	CodeUnimplemented      Code = "UNIMPLEMENTED"
	CodeInternal           Code = "INTERNAL"
	CodeUnavailable        Code = "UNAVAILABLE"
	CodeDataLoss           Code = "DATA_LOSS"
	CodeUnauthenticated    Code = "UNAUTHENTICATED"
)

// Codes lists the closed set, in gRPC's numeric order.
var Codes = []Code{
	CodeCancelled, CodeUnknown, CodeInvalidArgument, CodeDeadlineExceeded,
	CodeNotFound, CodeAlreadyExists, CodePermissionDenied, CodeResourceExhausted,
	CodeFailedPrecondition, CodeAborted, CodeOutOfRange, CodeUnimplemented,
	CodeInternal, CodeUnavailable, CodeDataLoss, CodeUnauthenticated,
}

// Valid reports whether c is one of the sixteen canonical codes.
func (c Code) Valid() bool {
	for _, known := range Codes {
		if c == known {
			return true
		}
	}
	return false
}

// ParseCode reads a wire value, mapping anything unrecognised -- including the
// empty string -- to [CodeUnknown].
func ParseCode(value string) Code {
	if c := Code(value); c.Valid() {
		return c
	}
	return CodeUnknown
}

// MaxErrorDetailsBytes caps the serialized vgi_rpc.error_details value, in
// UTF-8 bytes as emitted. A server whose array would exceed it omits the array
// entirely -- top-level key and log_extra mirror both.
const MaxErrorDetailsBytes = 4096

// ---------------------------------------------------------------------------
// The detail catalog
// ---------------------------------------------------------------------------

// ErrorDetail is one element of an error's details array.
//
// The catalog types below implement it, and so does [RawDetail], which carries
// a protocol-defined type (named under the protocol's own name) or relays an
// object received from a peer. MarshalJSON must produce a JSON object whose
// "@type" member equals DetailType().
type ErrorDetail interface {
	DetailType() string
	json.Marshaler
}

// Catalog type names.
const (
	ErrorInfoType           = "vgi_rpc.ErrorInfo"
	RetryInfoType           = "vgi_rpc.RetryInfo"
	BadRequestType          = "vgi_rpc.BadRequest"
	PreconditionFailureType = "vgi_rpc.PreconditionFailure"
	QuotaFailureType        = "vgi_rpc.QuotaFailure"
	ResourceInfoType        = "vgi_rpc.ResourceInfo"
	HelpType                = "vgi_rpc.Help"
	LocalizedMessageType    = "vgi_rpc.LocalizedMessage"
)

var errorDetailCatalog = map[string]bool{
	ErrorInfoType: true, RetryInfoType: true, BadRequestType: true,
	PreconditionFailureType: true, QuotaFailureType: true, ResourceInfoType: true,
	HelpType: true, LocalizedMessageType: true,
}

// marshalDetail renders fields as a JSON object with "@type" first.
func marshalDetail(typeName string, fields any) ([]byte, error) {
	body, err := marshalCompact(fields)
	if err != nil {
		return nil, err
	}
	typeJSON, _ := marshalCompact(typeName)
	var buf bytes.Buffer
	buf.WriteString(`{"@type":`)
	buf.Write(typeJSON)
	if inner := bytes.TrimSpace(body[1 : len(body)-1]); len(inner) > 0 {
		buf.WriteByte(',')
		buf.Write(inner)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// marshalCompact is json.Marshal without HTML escaping, so the bytes measured
// against the cap are the bytes a non-Go encoder would also produce for the
// common case.
func marshalCompact(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// ErrorInfo is extra context for the reason. The reason and its domain are
// already error_kind and the protocol, so neither is repeated here.
type ErrorInfo struct {
	Metadata map[string]string
}

// DetailType returns "vgi_rpc.ErrorInfo".
func (ErrorInfo) DetailType() string { return ErrorInfoType }

// MarshalJSON renders the wire object.
func (d ErrorInfo) MarshalJSON() ([]byte, error) {
	md := d.Metadata
	if md == nil {
		md = map[string]string{}
	}
	return marshalDetail(ErrorInfoType, struct {
		Metadata map[string]string `json:"metadata"`
	}{md})
}

// RetryInfo says how long to wait before retrying.
type RetryInfo struct {
	RetryDelaySeconds float64
}

// DetailType returns "vgi_rpc.RetryInfo".
func (RetryInfo) DetailType() string { return RetryInfoType }

// MarshalJSON renders the wire object. A whole number travels as an integer.
func (d RetryInfo) MarshalJSON() ([]byte, error) {
	if math.IsNaN(d.RetryDelaySeconds) || math.IsInf(d.RetryDelaySeconds, 0) || d.RetryDelaySeconds < 0 {
		return nil, errors.New("vgirpc: retry_delay_seconds must be finite and non-negative")
	}
	return marshalDetail(RetryInfoType, struct {
		RetryDelaySeconds float64 `json:"retry_delay_seconds"`
	}{d.RetryDelaySeconds})
}

// FieldViolation is one wrong input.
type FieldViolation struct {
	Field       string `json:"field"`
	Description string `json:"description"`
}

// BadRequest says which inputs were wrong.
type BadRequest struct {
	FieldViolations []FieldViolation
}

// DetailType returns "vgi_rpc.BadRequest".
func (BadRequest) DetailType() string { return BadRequestType }

// MarshalJSON renders the wire object.
func (d BadRequest) MarshalJSON() ([]byte, error) {
	v := d.FieldViolations
	if v == nil {
		v = []FieldViolation{}
	}
	return marshalDetail(BadRequestType, struct {
		FieldViolations []FieldViolation `json:"field_violations"`
	}{v})
}

// PreconditionViolation is one unmet precondition.
type PreconditionViolation struct {
	Type        string `json:"type"`
	Subject     string `json:"subject"`
	Description string `json:"description"`
}

// PreconditionFailure says what state must change before the call can succeed.
type PreconditionFailure struct {
	Violations []PreconditionViolation
}

// DetailType returns "vgi_rpc.PreconditionFailure".
func (PreconditionFailure) DetailType() string { return PreconditionFailureType }

// MarshalJSON renders the wire object.
func (d PreconditionFailure) MarshalJSON() ([]byte, error) {
	v := d.Violations
	if v == nil {
		v = []PreconditionViolation{}
	}
	return marshalDetail(PreconditionFailureType, struct {
		Violations []PreconditionViolation `json:"violations"`
	}{v})
}

// QuotaViolation is one exhausted limit.
type QuotaViolation struct {
	Subject     string `json:"subject"`
	Description string `json:"description"`
}

// QuotaFailure says which limit was hit.
type QuotaFailure struct {
	Violations []QuotaViolation
}

// DetailType returns "vgi_rpc.QuotaFailure".
func (QuotaFailure) DetailType() string { return QuotaFailureType }

// MarshalJSON renders the wire object.
func (d QuotaFailure) MarshalJSON() ([]byte, error) {
	v := d.Violations
	if v == nil {
		v = []QuotaViolation{}
	}
	return marshalDetail(QuotaFailureType, struct {
		Violations []QuotaViolation `json:"violations"`
	}{v})
}

// ResourceInfo names the object the error concerns.
type ResourceInfo struct {
	ResourceType string `json:"resource_type"`
	ResourceName string `json:"resource_name"`
	Owner        string `json:"owner"`
	Description  string `json:"description"`
}

// DetailType returns "vgi_rpc.ResourceInfo".
func (ResourceInfo) DetailType() string { return ResourceInfoType }

// MarshalJSON renders the wire object.
func (d ResourceInfo) MarshalJSON() ([]byte, error) {
	type plain ResourceInfo
	return marshalDetail(ResourceInfoType, plain(d))
}

// HelpLink is one pointer to documentation.
type HelpLink struct {
	Description string `json:"description"`
	URL         string `json:"url"`
}

// Help says where to read more.
type Help struct {
	Links []HelpLink
}

// DetailType returns "vgi_rpc.Help".
func (Help) DetailType() string { return HelpType }

// MarshalJSON renders the wire object.
func (d Help) MarshalJSON() ([]byte, error) {
	v := d.Links
	if v == nil {
		v = []HelpLink{}
	}
	return marshalDetail(HelpType, struct {
		Links []HelpLink `json:"links"`
	}{v})
}

// LocalizedMessage is text that is safe to show an end user. The error message
// itself stays developer-facing English, as in gRPC.
type LocalizedMessage struct {
	Locale  string `json:"locale"`
	Message string `json:"message"`
}

// DetailType returns "vgi_rpc.LocalizedMessage".
func (LocalizedMessage) DetailType() string { return LocalizedMessageType }

// MarshalJSON renders the wire object.
func (d LocalizedMessage) MarshalJSON() ([]byte, error) {
	type plain LocalizedMessage
	return marshalDetail(LocalizedMessageType, plain(d))
}

// RawDetail is a detail as a JSON object, "@type" included.
//
// Use it for a protocol-defined detail type, which must live under the
// protocol's own name (e.g. "vgi.reports.v1.SomeDetail"), or to relay an
// object received from a peer unchanged.
type RawDetail map[string]any

// DetailType returns the object's "@type", or "" when it has none.
func (d RawDetail) DetailType() string {
	s, _ := d["@type"].(string)
	return s
}

// MarshalJSON renders the object as-is.
func (d RawDetail) MarshalJSON() ([]byte, error) {
	return marshalCompact(map[string]any(d))
}

// validateErrorDetails applies the catalog rules: every element names a
// qualified type, no type repeats, and nothing invents a vgi_rpc.* type.
func validateErrorDetails(details []ErrorDetail) error {
	seen := make(map[string]bool, len(details))
	for _, d := range details {
		if d == nil {
			return errors.New("vgirpc: nil error detail")
		}
		name := d.DetailType()
		if name == "" {
			return errors.New("vgirpc: every error detail must name its type in '@type'")
		}
		if seen[name] {
			return errors.New("vgirpc: error detail type " + name + " appears more than once")
		}
		seen[name] = true
		if strings.HasPrefix(name, reservedProtocolPrefix) && !errorDetailCatalog[name] {
			return errors.New("vgirpc: " + name + " claims the reserved 'vgi_rpc.' prefix but is not in the catalog")
		}
		if !strings.Contains(name, ".") {
			return errors.New("vgirpc: " + name + " is not qualified; protocol-defined types live under the protocol's name")
		}
	}
	return nil
}

// ValidateErrorDetails reports whether details obey the catalog rules (each
// type qualified, none repeated, no invented vgi_rpc.* type). A server drops a
// violating array at emission; calling this where the error is built lets the
// author see the mistake instead.
func ValidateErrorDetails(details []ErrorDetail) error {
	return validateErrorDetails(details)
}

// encodeErrorDetails serializes details for vgi_rpc.error_details, or returns
// ok=false -- meaning omit the key and the mirror -- for an empty array, one
// that breaks a catalog rule, or one over [MaxErrorDetailsBytes].
func encodeErrorDetails(details []ErrorDetail) (json.RawMessage, bool) {
	if len(details) == 0 || validateErrorDetails(details) != nil {
		return nil, false
	}
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, d := range details {
		obj, err := d.MarshalJSON()
		if err != nil {
			return nil, false
		}
		// The marshalled object must agree with the type it declared.
		var probe struct {
			Type *string `json:"@type"`
		}
		if json.Unmarshal(obj, &probe) != nil || probe.Type == nil || *probe.Type != d.DetailType() {
			return nil, false
		}
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(obj)
	}
	buf.WriteByte(']')
	if buf.Len() > MaxErrorDetailsBytes {
		// Dropped whole. Never a prefix: a partial list reads as a complete one.
		return nil, false
	}
	return json.RawMessage(buf.Bytes()), true
}

// decodeErrorDetails decodes a vgi_rpc.error_details value. Tolerant by
// design: anything that is not a JSON array decodes as empty, and non-object
// elements are skipped. Unknown types are KEPT -- filtering to the catalog is
// what the typed accessors do.
func decodeErrorDetails(raw string) []map[string]any {
	var elems []json.RawMessage
	if json.Unmarshal([]byte(raw), &elems) != nil {
		return nil
	}
	return objectElements(elems)
}

func objectElements(elems []json.RawMessage) []map[string]any {
	var out []map[string]any
	for _, elem := range elems {
		var obj map[string]any
		dec := json.NewDecoder(bytes.NewReader(elem))
		dec.UseNumber()
		if dec.Decode(&obj) != nil || obj == nil {
			continue
		}
		out = append(out, normaliseNumbers(obj).(map[string]any))
	}
	return out
}

// normaliseNumbers turns json.Number into float64 so relayed values compare
// by value (7 == 7.0) wherever they end up.
func normaliseNumbers(v any) any {
	switch t := v.(type) {
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return t.String()
		}
		return f
	case map[string]any:
		for k, inner := range t {
			t[k] = normaliseNumbers(inner)
		}
		return t
	case []any:
		for i, inner := range t {
			t[i] = normaliseNumbers(inner)
		}
		return t
	}
	return v
}

// ParseErrorDetail decodes one detail object into its catalog type, or returns
// nil when the type is unknown or a field is malformed. A malformed detail is
// treated as absent rather than failing the error it rides on.
func ParseErrorDetail(obj map[string]any) ErrorDetail {
	typeName, _ := obj["@type"].(string)
	switch typeName {
	case ErrorInfoType:
		md := map[string]string{}
		if raw, present := obj["metadata"]; present {
			m, ok := raw.(map[string]any)
			if !ok {
				return nil
			}
			for k, v := range m {
				s, ok := v.(string)
				if !ok {
					return nil
				}
				md[k] = s
			}
		}
		return ErrorInfo{Metadata: md}
	case RetryInfoType:
		f, ok := obj["retry_delay_seconds"].(float64)
		if !ok || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 {
			return nil
		}
		return RetryInfo{RetryDelaySeconds: f}
	case BadRequestType:
		items, ok := detailObjects(obj, "field_violations")
		if !ok {
			return nil
		}
		out := BadRequest{FieldViolations: []FieldViolation{}}
		for _, it := range items {
			f, ok1 := detailString(it, "field")
			d, ok2 := detailString(it, "description")
			if !ok1 || !ok2 {
				return nil
			}
			out.FieldViolations = append(out.FieldViolations, FieldViolation{Field: f, Description: d})
		}
		return out
	case PreconditionFailureType:
		items, ok := detailObjects(obj, "violations")
		if !ok {
			return nil
		}
		out := PreconditionFailure{Violations: []PreconditionViolation{}}
		for _, it := range items {
			t, ok1 := detailString(it, "type")
			s, ok2 := detailString(it, "subject")
			d, ok3 := detailString(it, "description")
			if !ok1 || !ok2 || !ok3 {
				return nil
			}
			out.Violations = append(out.Violations, PreconditionViolation{Type: t, Subject: s, Description: d})
		}
		return out
	case QuotaFailureType:
		items, ok := detailObjects(obj, "violations")
		if !ok {
			return nil
		}
		out := QuotaFailure{Violations: []QuotaViolation{}}
		for _, it := range items {
			s, ok1 := detailString(it, "subject")
			d, ok2 := detailString(it, "description")
			if !ok1 || !ok2 {
				return nil
			}
			out.Violations = append(out.Violations, QuotaViolation{Subject: s, Description: d})
		}
		return out
	case ResourceInfoType:
		rt, ok1 := detailString(obj, "resource_type")
		rn, ok2 := detailString(obj, "resource_name")
		ow, ok3 := detailString(obj, "owner")
		de, ok4 := detailString(obj, "description")
		if !ok1 || !ok2 || !ok3 || !ok4 {
			return nil
		}
		return ResourceInfo{ResourceType: rt, ResourceName: rn, Owner: ow, Description: de}
	case HelpType:
		items, ok := detailObjects(obj, "links")
		if !ok {
			return nil
		}
		out := Help{Links: []HelpLink{}}
		for _, it := range items {
			d, ok1 := detailString(it, "description")
			u, ok2 := detailString(it, "url")
			if !ok1 || !ok2 {
				return nil
			}
			out.Links = append(out.Links, HelpLink{Description: d, URL: u})
		}
		return out
	case LocalizedMessageType:
		l, ok1 := detailString(obj, "locale")
		m, ok2 := detailString(obj, "message")
		if !ok1 || !ok2 {
			return nil
		}
		return LocalizedMessage{Locale: l, Message: m}
	}
	return nil
}

// detailString reads an optional string field: absent is "", a non-string is
// malformed.
func detailString(obj map[string]any, key string) (string, bool) {
	raw, present := obj[key]
	if !present {
		return "", true
	}
	s, ok := raw.(string)
	return s, ok
}

// detailObjects reads an optional array-of-objects field: absent is empty.
func detailObjects(obj map[string]any, key string) ([]map[string]any, bool) {
	raw, present := obj[key]
	if !present {
		return nil, true
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil, false
	}
	out := make([]map[string]any, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, false
		}
		out = append(out, m)
	}
	return out, true
}

// IsRetryable classifies an error by the rule in WIRE_PROTOCOL.md §8:
// UNAVAILABLE is retryable; RESOURCE_EXHAUSTED only when it carries RetryInfo;
// everything else -- ABORTED included, which means "retry the whole operation
// at a higher level" -- is final. A classification, not a policy: nothing in
// this package retries an RPC error automatically, because a method may not be
// idempotent.
func IsRetryable(code Code, details []map[string]any) bool {
	switch ParseCode(string(code)) {
	case CodeUnavailable:
		return true
	case CodeResourceExhausted:
		for _, d := range details {
			if _, ok := ParseErrorDetail(d).(RetryInfo); ok {
				return true
			}
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Reading the model off a server-side error
// ---------------------------------------------------------------------------

type errorCodeCarrier interface {
	ErrorCode() Code
}

type errorDetailsCarrier interface {
	ErrorDetails() []ErrorDetail
}

// ErrorCodeOf returns the canonical code err declares anywhere in its chain,
// or [CodeUnknown] for an unclassified error.
func ErrorCodeOf(err error) Code {
	var carrier errorCodeCarrier
	if errors.As(err, &carrier) {
		return ParseCode(string(carrier.ErrorCode()))
	}
	return CodeUnknown
}

// ErrorKindOf returns the error_kind err declares anywhere in its chain, or "".
//
// errors.As rather than a type assertion: a handler that wraps a typed error
// with fmt.Errorf("...: %w", err) must not lose its classification.
func ErrorKindOf(err error) string {
	var carrier errorKindCarrier
	if errors.As(err, &carrier) {
		return carrier.ErrorKind()
	}
	return ""
}

// ErrorDetailsOf returns the details err declares anywhere in its chain.
func ErrorDetailsOf(err error) []ErrorDetail {
	var carrier errorDetailsCarrier
	if errors.As(err, &carrier) {
		return carrier.ErrorDetails()
	}
	return nil
}

// StatusError is an application error carrying the full error model. Return it
// from a handler to choose the code, the reason and the details a client sees:
//
//	return &vgirpc.StatusError{
//		Code:    vgirpc.CodeUnavailable,
//		Kind:    "report_rebuilding",
//		Message: "report is being rebuilt",
//		Details: []vgirpc.ErrorDetail{vgirpc.RetryInfo{RetryDelaySeconds: 30}},
//	}
//
// Any error type may instead implement ErrorCode() Code, ErrorKind() string and
// ErrorDetails() []ErrorDetail; this is the convenience for when a dedicated
// type would add nothing. Details that break a catalog rule or exceed 4 KiB are
// dropped whole at emission ([ValidateErrorDetails] checks them up front).
type StatusError struct {
	Code    Code
	Kind    string
	Message string
	Details []ErrorDetail
}

func (e *StatusError) Error() string { return e.Message }

// ErrorCode returns the code, [CodeUnknown] when it is not canonical.
func (e *StatusError) ErrorCode() Code { return ParseCode(string(e.Code)) }

// ErrorKind returns the reason, or "".
func (e *StatusError) ErrorKind() string { return e.Kind }

// ErrorDetails returns the details.
func (e *StatusError) ErrorDetails() []ErrorDetail { return e.Details }

// ErrorType is the exception class name a client sees.
func (e *StatusError) ErrorType() string { return "StatusError" }
