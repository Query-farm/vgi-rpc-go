// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

var (
	irohEphemeralSecret [32]byte
	irohEphemeralOnce   sync.Once
	irohEphemeralErr    error
)

const (
	IrohArrowMuxALPN = "vgi-rpc/arrow-mux/1"
	IrohHTTPALPN     = "iroh-http/2"
)

type IrohErrorStage string

const (
	IrohStageParse      IrohErrorStage = "parse"
	IrohStageBind       IrohErrorStage = "bind"
	IrohStageResolve    IrohErrorStage = "resolve"
	IrohStageConnect    IrohErrorStage = "connect"
	IrohStageALPN       IrohErrorStage = "alpn"
	IrohStageOpenStream IrohErrorStage = "open_stream"
	IrohStageWrite      IrohErrorStage = "write"
	IrohStageRead       IrohErrorStage = "read"
	IrohStageCancel     IrohErrorStage = "cancel"
	IrohStageClose      IrohErrorStage = "close"
	IrohStageInternal   IrohErrorStage = "internal"
)

type IrohErrorCategory string

const (
	IrohCategoryInvalidInput      IrohErrorCategory = "invalid_input"
	IrohCategoryUnsupported       IrohErrorCategory = "unsupported"
	IrohCategoryUnavailable       IrohErrorCategory = "unavailable"
	IrohCategoryTimeout           IrohErrorCategory = "timeout"
	IrohCategoryProtocol          IrohErrorCategory = "protocol"
	IrohCategoryConnectionReset   IrohErrorCategory = "connection_reset"
	IrohCategoryCancelled         IrohErrorCategory = "cancelled"
	IrohCategoryAuthentication    IrohErrorCategory = "authentication"
	IrohCategoryResourceExhausted IrohErrorCategory = "resource_exhausted"
	IrohCategoryInternal          IrohErrorCategory = "internal"
)

type IrohDispatchCertainty string

const (
	IrohNotSent IrohDispatchCertainty = "not_sent"
	IrohUnknown IrohDispatchCertainty = "unknown"
	IrohSent    IrohDispatchCertainty = "sent"
)

// IrohTransportError exposes portable retry-safety information.
type IrohTransportError struct {
	Stage             IrohErrorStage
	Category          IrohErrorCategory
	DispatchCertainty IrohDispatchCertainty
	Message           string
	Cause             error
}

func (failure *IrohTransportError) Error() string { return failure.Message }
func (failure *IrohTransportError) Unwrap() error { return failure.Cause }

func irohError(message string, stage IrohErrorStage, category IrohErrorCategory,
	certainty IrohDispatchCertainty, cause error) error {
	return &IrohTransportError{Stage: stage, Category: category,
		DispatchCertainty: certainty, Message: message, Cause: cause}
}

func invalidIroh(message string) error {
	return irohError(message, IrohStageParse, IrohCategoryInvalidInput, IrohNotSent, nil)
}

// IrohEndpoint is a canonical iroh:// or httpi:// VGI endpoint.
type IrohEndpoint struct {
	Scheme          string
	EndpointID      string
	EndpointIDBytes [32]byte
	BasePath        string
	ALPN            string
}

// ParseIrohEndpoint parses without net/url authority and path normalization.
func ParseIrohEndpoint(raw string) (IrohEndpoint, error) {
	if raw == "" || strings.ContainsAny(raw, "\\?#") {
		return IrohEndpoint{}, invalidIroh("vgirpc: invalid VGI Iroh endpoint URI")
	}
	for _, value := range raw {
		if value <= 0x20 || value == 0x7f {
			return IrohEndpoint{}, invalidIroh("vgirpc: invalid VGI Iroh endpoint URI")
		}
	}
	var scheme, remainder string
	switch {
	case strings.HasPrefix(raw, "iroh://"):
		scheme, remainder = "iroh", strings.TrimPrefix(raw, "iroh://")
	case strings.HasPrefix(raw, "httpi://"):
		scheme, remainder = "httpi", strings.TrimPrefix(raw, "httpi://")
	default:
		return IrohEndpoint{}, invalidIroh("vgirpc: Iroh endpoint scheme must be iroh:// or httpi://")
	}
	id, path, found := strings.Cut(remainder, "/")
	if found {
		path = "/" + path
	}
	if len(id) != 64 || strings.ToLower(id) != id {
		return IrohEndpoint{}, invalidIroh("vgirpc: Iroh endpoint ID must be exactly 64 lowercase hexadecimal characters")
	}
	decoded, err := hex.DecodeString(id)
	if err != nil {
		return IrohEndpoint{}, invalidIroh("vgirpc: Iroh endpoint ID must be exactly 64 lowercase hexadecimal characters")
	}
	if scheme == "iroh" && path != "" {
		return IrohEndpoint{}, invalidIroh("vgirpc: iroh:// endpoints cannot contain a path")
	}
	if len(path) > 1 && strings.HasSuffix(path, "/") {
		return IrohEndpoint{}, invalidIroh("vgirpc: httpi:// base paths cannot have a trailing empty segment")
	}
	if strings.Contains(path, "//") {
		return IrohEndpoint{}, invalidIroh("vgirpc: httpi:// base paths cannot contain empty segments")
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "." || segment == ".." {
			return IrohEndpoint{}, invalidIroh("vgirpc: httpi:// base paths cannot contain dot segments")
		}
	}
	for index := 0; index < len(path); index++ {
		if path[index] == '%' && (index+2 >= len(path) || !isHex(path[index+1]) || !isHex(path[index+2])) {
			return IrohEndpoint{}, invalidIroh("vgirpc: httpi:// base path contains an invalid percent escape")
		}
		if path[index] == '%' {
			decoded, _ := hex.DecodeString(path[index+1 : index+3])
			value := decoded[0]
			if value == '.' || value == '/' || value == '\\' || value <= 0x20 || value == 0x7f {
				return IrohEndpoint{}, invalidIroh("vgirpc: httpi:// base path contains an encoded dot, separator, or control")
			}
			index += 2
		}
	}
	if path == "/" {
		path = ""
	}
	var idBytes [32]byte
	copy(idBytes[:], decoded)
	alpn := IrohArrowMuxALPN
	if scheme == "httpi" {
		alpn = IrohHTTPALPN
	}
	return IrohEndpoint{Scheme: scheme, EndpointID: id, EndpointIDBytes: idBytes, BasePath: path, ALPN: alpn}, nil
}

func isHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F'
}

// IrohClientOptions are passed to an application-pinned community/native binding.
type IrohClientOptions struct {
	SecretKey       []byte
	RelayURLs       []string
	NoRelay         bool
	RemoteRelayURL  string
	DirectAddresses []string
	ConnectTimeout  time.Duration
	IOTimeout       time.Duration
}

func (options IrohClientOptions) normalized() (IrohClientOptions, error) {
	if len(options.SecretKey) != 0 && len(options.SecretKey) != 32 {
		return options, invalidIroh("vgirpc: Iroh secret key must contain exactly 32 bytes")
	}
	if options.NoRelay && len(options.RelayURLs) != 0 {
		return options, invalidIroh("vgirpc: NoRelay and RelayURLs are mutually exclusive")
	}
	if options.ConnectTimeout == 0 {
		options.ConnectTimeout = 30 * time.Second
	}
	if options.IOTimeout == 0 {
		options.IOTimeout = 5 * time.Minute
	}
	if options.ConnectTimeout < 0 || options.IOTimeout < 0 {
		return options, invalidIroh("vgirpc: Iroh timeouts must be positive")
	}
	if len(options.SecretKey) == 0 {
		irohEphemeralOnce.Do(func() { _, irohEphemeralErr = rand.Read(irohEphemeralSecret[:]) })
		if irohEphemeralErr != nil {
			return options, irohError("vgirpc: create process-stable Iroh identity",
				IrohStageBind, IrohCategoryInternal, IrohNotSent, irohEphemeralErr)
		}
		options.SecretKey = irohEphemeralSecret[:]
	}
	options.SecretKey = append([]byte(nil), options.SecretKey...)
	options.RelayURLs = append([]string(nil), options.RelayURLs...)
	options.DirectAddresses = append([]string(nil), options.DirectAddresses...)
	return options, nil
}

// IrohDialer is the qualification seam for the community Go binding. The
// returned connection carries one vgi-rpc/arrow-mux/1 bidirectional stream and
// must implement deadlines and cancellation with ordinary net.Conn semantics.
type IrohDialer interface {
	DialIroh(context.Context, IrohEndpoint, IrohClientOptions) (net.Conn, error)
}

// NewIrohClient opens the ordinary stateful raw client through a qualified
// native/community binding. Core intentionally has no connector process or
// automatic runtime download. httpi:// is parsed for the shared endpoint
// contract but is explicitly unsupported until a qualified iroh-http/2 codec
// is available.
func NewIrohClient(ctx context.Context, rawEndpoint string, dialer IrohDialer,
	irohOptions IrohClientOptions, options ...TcpClientOption) (*TcpClient, error) {
	if ctx == nil {
		return nil, invalidIroh("vgirpc: Iroh client context must not be nil")
	}
	endpoint, err := ParseIrohEndpoint(rawEndpoint)
	if err != nil {
		return nil, err
	}
	if endpoint.Scheme != "iroh" {
		return nil, irohError("vgirpc: raw client requires iroh://; httpi:// requires an iroh-http/2 client",
			IrohStageBind, IrohCategoryUnsupported, IrohNotSent, nil)
	}
	if dialer == nil {
		return nil, irohError("vgirpc: iroh:// requires an explicitly configured native/community Iroh dialer",
			IrohStageBind, IrohCategoryUnsupported, IrohNotSent, nil)
	}
	irohOptions, err = irohOptions.normalized()
	if err != nil {
		return nil, err
	}
	config := tcpClientConfig{connectTimeout: defaultTCPClientConnectTimeout,
		maxRequest: defaultClientMaxRequestBytes, maxResponse: defaultClientMaxDecodedResponseBytes}
	for _, option := range options {
		if option != nil {
			if err := option(&config); err != nil {
				return nil, err
			}
		}
	}
	if config.proxyConfigured || config.connectConfigured {
		return nil, invalidIroh("vgirpc: TCP proxy/connect options do not apply to an Iroh dialer")
	}
	dialCtx := ctx
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > irohOptions.ConnectTimeout {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, irohOptions.ConnectTimeout)
		defer cancel()
	}
	conn, err := dialer.DialIroh(dialCtx, endpoint, irohOptions)
	if err != nil {
		var structured *IrohTransportError
		if errors.As(err, &structured) {
			return nil, err
		}
		category := IrohCategoryUnavailable
		stage := IrohStageConnect
		if errors.Is(dialCtx.Err(), context.DeadlineExceeded) {
			category = IrohCategoryTimeout
		}
		if errors.Is(dialCtx.Err(), context.Canceled) {
			category, stage = IrohCategoryCancelled, IrohStageCancel
		}
		return nil, irohError(fmt.Sprintf("vgirpc: connect Iroh worker: %v", err), stage, category, IrohNotSent, err)
	}
	if conn == nil {
		return nil, irohError("vgirpc: Iroh dialer returned a nil connection",
			IrohStageOpenStream, IrohCategoryUnavailable, IrohNotSent, nil)
	}
	conn = &irohDeadlineConn{Conn: conn, timeout: irohOptions.IOTimeout}
	_ = conn.SetDeadline(time.Now().Add(irohOptions.IOTimeout))
	return newTcpClientFromConn(conn, config), nil
}

type irohDeadlineConn struct {
	net.Conn
	timeout time.Duration
}

func (conn *irohDeadlineConn) Read(buffer []byte) (int, error) {
	read, err := conn.Conn.Read(buffer)
	if err == nil {
		return read, nil
	}
	category := IrohCategoryConnectionReset
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		category = IrohCategoryTimeout
	}
	return read, irohError("vgirpc: Iroh read: "+err.Error(), IrohStageRead, category, IrohSent, err)
}

func (conn *irohDeadlineConn) Write(buffer []byte) (int, error) {
	written, err := conn.Conn.Write(buffer)
	if err == nil {
		return written, nil
	}
	category := IrohCategoryConnectionReset
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		category = IrohCategoryTimeout
	}
	return written, irohError("vgirpc: Iroh write: "+err.Error(), IrohStageWrite, category, IrohUnknown, err)
}

func (conn *irohDeadlineConn) SetDeadline(deadline time.Time) error {
	if deadline.IsZero() {
		deadline = time.Now().Add(conn.timeout)
	}
	return conn.Conn.SetDeadline(deadline)
}

func contextualIrohError(conn net.Conn, err error, stage IrohErrorStage,
	certainty IrohDispatchCertainty) error {
	if _, ok := conn.(*irohDeadlineConn); !ok {
		return err
	}
	if errors.Is(err, context.Canceled) {
		return irohError("vgirpc: Iroh operation cancelled", IrohStageCancel,
			IrohCategoryCancelled, certainty, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return irohError("vgirpc: Iroh operation timed out", stage,
			IrohCategoryTimeout, certainty, err)
	}
	return err
}
