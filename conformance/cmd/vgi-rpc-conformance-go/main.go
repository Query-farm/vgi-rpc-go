// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Query-farm/vgi-rpc-go/conformance"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	vgiotel "github.com/Query-farm/vgi-rpc-go/vgirpc/otel"

	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// conformanceReasonHeader lets a conformance request name the reason it
// wants refused with, so the suite can prove this server *discriminates*
// between reason codes rather than stamping one constant on every 401.
//
// This is a fixture affordance, never a protocol behaviour — nothing outside
// the conformance worker reads it.
const conformanceReasonHeader = "X-Conformance-Auth-Reason"

// requestableReasons are the reasons a request may ask to be refused with.
// proxy_required is deliberately absent: the spec derives it from server
// configuration, never from the request, so a worker that let a caller
// summon it would be modelling the contract wrong. Anything not in this map
// falls through to the unclassified path.
var requestableReasons = map[string]vgirpc.AuthReason{
	"missing_credential": vgirpc.AuthReasonMissingCredential,
	"invalid_credential": vgirpc.AuthReasonInvalidCredential,
	"expired_credential": vgirpc.AuthReasonExpiredCredential,
	"insufficient_scope": vgirpc.AuthReasonInsufficientScope,
}

// conformancePrincipalHeader names the principal a request should be
// authenticated as. Another fixture affordance: it lets one worker be reachable
// as several identities, which the sticky replay case and the introspector
// allowlist both need.
const conformancePrincipalHeader = "X-Conformance-Principal"

// Fixed values the shared TestTokenIntrospection group posts and asserts, so
// they are part of this worker's contract rather than decoration. They mirror
// the constants in the upstream _pytest_suite.py.
const (
	conformanceIntrospector     = "conformance-introspector"
	conformanceSubjectToken     = "conformance-opaque-subject-token"
	conformanceSubjectPrincipal = "subject@conformance.example"
	conformanceSubjectTokenName = "conformance-subject"
	// JWS-shaped and deliberately *resolvable*: against an unknown JWS a port
	// with no shape guard rejects it as unknown and passes the test for the
	// wrong reason. Resolvable, the guard is the only thing that can produce a
	// rejection.
	conformanceJWSTrapToken = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl"
	// Well above the ~13 introspections the shared group makes in one second,
	// so the limiter can never turn a conformance run into a flake.
	conformanceIntrospectRateLimit = 200
)

// principalFromHeader authenticates as whatever principal the request names.
// Requests without the header stay anonymous rather than being rejected: the
// suite probes /health and the capability endpoint before it authenticates
// anything.
func principalFromHeader(r *http.Request) (*vgirpc.AuthContext, error) {
	principal := r.Header.Get(conformancePrincipalHeader)
	if principal == "" {
		return vgirpc.Anonymous(), nil
	}
	return &vgirpc.AuthContext{
		Domain:        "conformance",
		Authenticated: true,
		Principal:     principal,
	}, nil
}

// conformanceTokenResolver resolves the two fixed credentials the shared
// introspection group posts; everything else is unresolvable.
func conformanceTokenResolver(credential string) (vgirpc.TokenIdentity, bool, error) {
	if credential == conformanceSubjectToken || credential == conformanceJWSTrapToken {
		return vgirpc.TokenIdentity{
			Principal:  conformanceSubjectPrincipal,
			TokenName:  conformanceSubjectTokenName,
			TTLSeconds: 300,
		}, true, nil
	}
	return vgirpc.TokenIdentity{}, false, nil
}

// rejectAll refuses every request, with the reason the caller named when it
// named one the spec allows a request to ask for.
func rejectAll(r *http.Request) (*vgirpc.AuthContext, error) {
	if reason, ok := requestableReasons[r.Header.Get(conformanceReasonHeader)]; ok {
		// The detail is the reason code itself so the suite can assert the
		// header and the JSON body agree without pinning prose.
		return nil, vgirpc.NewAuthFailure(reason, string(reason))
	}
	// Unclassified: must land on the fallback, not a guess.
	return nil, &vgirpc.RpcError{Type: "ValueError", Message: "auth required"}
}

func main() {
	defer func() {
		if s := vgirpc.LeakCheckSummary(); s != "" {
			fmt.Fprintln(os.Stderr, s)
		}
	}()

	server := vgirpc.NewServer()
	server.SetDebugErrors(true)
	server.SetServiceName("ConformanceService")
	// --server-id overrides the fixed default. TestSticky's wrong-worker case
	// runs two workers that share one AEAD key, and asserts up front that they
	// report distinct server_id — otherwise a token that "belongs to the other
	// worker" would in fact belong to this one and the test would prove nothing.
	serverID := "conformance-go"
	if v := findFlagValue(os.Args, "--server-id"); v != "" {
		serverID = v
	}
	server.SetServerID(serverID)
	// Match Python ConformanceService.protocol_version. Requires
	// vgi-rpc >= 0.18.0 on the client (sends the vgi_rpc.protocol_version
	// request metadata key) — see ci.yml.
	server.SetProtocolVersion("1.0.0")
	conformance.RegisterMethods(server)

	// Core carries no OpenTelemetry dependency, so the accessor that lets an
	// access-log record name the span it ran under is injected here. It reads
	// whatever span is current — application-opened or framework-opened — so
	// it is a no-op until something opens one.
	//
	// This is vgiotel.TraceContext inlined: the root module resolves
	// vgirpc/otel to its published release, which predates that helper.
	// Applications on a released pair should write
	// vgirpc.SetTraceContextProvider(vgiotel.TraceContext) instead.
	vgirpc.SetTraceContextProvider(func(ctx context.Context) (string, string) {
		sc := oteltrace.SpanContextFromContext(ctx)
		if !sc.IsValid() {
			return "", ""
		}
		return sc.TraceID().String(), sc.SpanID().String()
	})

	// Cross-language conformance: --access-log <path> may appear anywhere
	// in os.Args. When present, install an AccessLogHook writing JSONL
	// records to that path per docs/access-log-spec.md in the Python repo.
	if accessLogPath := findFlagValue(os.Args, "--access-log"); accessLogPath != "" {
		f, err := os.OpenFile(accessLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open --access-log file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		hook := vgirpc.NewAccessLogHook(f, "vgi-rpc-go-conformance")
		// Both optional per the spec, and both fail at startup rather than at
		// the first request: a sample rate of 100 meaning "100%" would
		// otherwise silently log everything.
		if raw := findFlagValue(os.Args, "--access-log-sample"); raw != "" {
			rate, perr := strconv.ParseFloat(raw, 64)
			if perr != nil {
				fmt.Fprintf(os.Stderr, "invalid --access-log-sample: %v\n", perr)
				os.Exit(1)
			}
			if serr := hook.SetSampleRate(rate); serr != nil {
				fmt.Fprintf(os.Stderr, "%v\n", serr)
				os.Exit(1)
			}
		}
		// At INFO the record carries no `request_data` at all, so every spec
		// rule governing that field goes unexercised — the validator only
		// checks it is well-formed *when present*. `vgi-rpc-test
		// --require-request-data` turns that silence into a failure, and it
		// needs a worker that can be asked for the payload.
		if hasFlag(os.Args, "--access-log-debug") {
			hook.SetDebug(true)
		}
		if hasFlag(os.Args, "--access-log-async") {
			queueSize := 0
			if raw := findFlagValue(os.Args, "--access-log-queue-size"); raw != "" {
				v, perr := strconv.Atoi(raw)
				if perr != nil {
					fmt.Fprintf(os.Stderr, "invalid --access-log-queue-size: %v\n", perr)
					os.Exit(1)
				}
				queueSize = v
			}
			if aerr := hook.SetAsync(queueSize); aerr != nil {
				fmt.Fprintf(os.Stderr, "%v\n", aerr)
				os.Exit(1)
			}
			// Drain before the file closes, or the queued tail is lost.
			defer hook.Close()
		}
		server.SetDispatchHook(hook)
	}

	authMode := len(os.Args) > 1 && os.Args[1] == "--http-auth"
	storageMode := len(os.Args) > 1 && os.Args[1] == "--http-with-storage"
	zstdStorageMode := len(os.Args) > 1 && os.Args[1] == "--http-with-zstd-storage"
	pkceMode := len(os.Args) > 1 && os.Args[1] == "--http-pkce"
	strictMode := len(os.Args) > 1 && os.Args[1] == "--http-strict"
	proofMode := len(os.Args) > 1 && os.Args[1] == "--http-proof"
	if (len(os.Args) > 1 && os.Args[1] == "--http") || authMode || storageMode || zstdStorageMode || pkceMode || strictMode || proofMode {
		// Parse optional flags that may follow positional args:
		//   --otel-export <path>
		//   --externalize-threshold <bytes>   (overrides default 8 KiB in storage modes)
		//   --max-request-bytes <bytes>       (overrides default 4 KiB inline request cap)
		var otelExportPath string
		externalizeThreshold := int64(-1) // -1 == not specified
		maxRequestBytes := int64(-1)      // -1 == not specified
		// Strict-mode response caps (default 1 MiB matches Python's
		// tests/serve_conformance_http_strict.py — large enough that
		// incidental tests don't trip while still being small enough that
		// the http_response_cap.* tests' 4x targets clearly overshoot).
		maxResponseBytes := int64(1024 * 1024)
		maxExternalizedResponseBytes := int64(1024 * 1024)
		var strictFakeStorageURL string
		for i := 2; i < len(os.Args)-1; i++ {
			switch os.Args[i] {
			case "--otel-export":
				otelExportPath = os.Args[i+1]
			case "--externalize-threshold":
				v, err := strconv.ParseInt(os.Args[i+1], 10, 64)
				if err != nil {
					fmt.Fprintf(os.Stderr, "invalid --externalize-threshold: %v\n", err)
					os.Exit(1)
				}
				externalizeThreshold = v
			case "--max-request-bytes":
				v, err := strconv.ParseInt(os.Args[i+1], 10, 64)
				if err != nil {
					fmt.Fprintf(os.Stderr, "invalid --max-request-bytes: %v\n", err)
					os.Exit(1)
				}
				maxRequestBytes = v
			case "--max-response-bytes":
				v, err := strconv.ParseInt(os.Args[i+1], 10, 64)
				if err != nil {
					fmt.Fprintf(os.Stderr, "invalid --max-response-bytes: %v\n", err)
					os.Exit(1)
				}
				maxResponseBytes = v
			case "--max-externalized-response-bytes":
				v, err := strconv.ParseInt(os.Args[i+1], 10, 64)
				if err != nil {
					fmt.Fprintf(os.Stderr, "invalid --max-externalized-response-bytes: %v\n", err)
					os.Exit(1)
				}
				maxExternalizedResponseBytes = v
			case "--fake-storage":
				strictFakeStorageURL = os.Args[i+1]
			}
		}

		// Configure external location when in storage modes. The fake
		// storage URL is the second argument.
		var fakeStorage *conformance.FakeStorage
		if storageMode || zstdStorageMode {
			if len(os.Args) < 3 {
				fmt.Fprintf(os.Stderr, "missing storage URL argument\n")
				os.Exit(1)
			}
			fakeStorage = conformance.NewFakeStorage(os.Args[2])
			cfg := vgirpc.DefaultExternalLocationConfig(fakeStorage)
			cfg.URLValidator = conformance.AllowAllValidator
			cfg.ExternalizeThresholdBytes = 8 * 1024 // 8 KiB so the test thresholds line up
			if externalizeThreshold > 0 {
				cfg.ExternalizeThresholdBytes = externalizeThreshold
			}
			if zstdStorageMode {
				cfg.Compression = &vgirpc.Compression{Algorithm: "zstd", Level: 3}
			}
			server.SetExternalLocation(cfg)
		}
		if strictMode && strictFakeStorageURL != "" {
			fakeStorage = conformance.NewFakeStorage(strictFakeStorageURL)
			cfg := vgirpc.DefaultExternalLocationConfig(fakeStorage)
			cfg.URLValidator = conformance.AllowAllValidator
			cfg.ExternalizeThresholdBytes = 4096
			if externalizeThreshold > 0 {
				cfg.ExternalizeThresholdBytes = externalizeThreshold
			}
			server.SetExternalLocation(cfg)
		}

		var otelFile *os.File
		var tp *sdktrace.TracerProvider
		var mp *sdkmetric.MeterProvider

		if otelExportPath != "" {
			var err error
			otelFile, err = os.Create(otelExportPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to open otel export file: %v\n", err)
				os.Exit(1)
			}

			traceExp, err := stdouttrace.New(stdouttrace.WithWriter(otelFile))
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to create trace exporter: %v\n", err)
				os.Exit(1)
			}

			metricExp, err := stdoutmetric.New(stdoutmetric.WithWriter(otelFile))
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to create metric exporter: %v\n", err)
				os.Exit(1)
			}

			tp = sdktrace.NewTracerProvider(
				sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(traceExp)),
			)

			mp = sdkmetric.NewMeterProvider(
				sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
			)

			vgiotel.InstrumentServer(server, vgiotel.OtelConfig{
				TracerProvider:   tp,
				MeterProvider:    mp,
				EnableTracing:    true,
				EnableMetrics:    true,
				RecordExceptions: true,
				ServiceName:      "conformance-go",
			})
		}

		// --token-key <hex> pins the AEAD key so two workers can open each
		// other's session tokens. There is no post-construction setter, so the
		// choice has to be made here.
		var httpServer *vgirpc.HttpServer
		if keyHex := findFlagValue(os.Args, "--token-key"); keyHex != "" {
			key, err := hex.DecodeString(keyHex)
			if err != nil {
				fmt.Fprintf(os.Stderr, "invalid --token-key (expected hex): %v\n", err)
				os.Exit(1)
			}
			hs, kerr := vgirpc.NewHttpServerWithKey(server, key)
			if kerr != nil {
				fmt.Fprintf(os.Stderr, "invalid --token-key: %v\n", kerr)
				os.Exit(1)
			}
			httpServer = hs
		} else {
			httpServer = vgirpc.NewHttpServer(server)
		}
		// In storage modes the same fake-storage instance also acts as
		// the upload-URL provider so client-vended request externalization
		// works end-to-end. The 4 KiB request cap is small enough that the
		// large-payload conformance tests trigger the 413 → upload flow.
		if fakeStorage != nil {
			httpServer.SetUploadURLProvider(fakeStorage)
			reqCap := int64(4096)
			if maxRequestBytes > 0 {
				reqCap = maxRequestBytes
			}
			httpServer.SetMaxRequestBytes(reqCap)
			httpServer.SetMaxUploadBytes(64 * 1024 * 1024)
		}
		// Disabling the call-state cache forces every stream continuation down
		// the cache-miss path, so the client's obligation to echo the call
		// token is checked deterministically rather than by luck.
		if hasFlag(os.Args, "--no-call-state-cache") {
			httpServer.SetCallStateCacheEntries(0)
		}
		if authMode {
			httpServer.SetPrefix("/vgi")
			httpServer.SetAuthenticate(rejectAll)
		}
		if proofMode {
			// Mirrors the reference worker's CLI so the shared TestProxyProof
			// group can drive every port with one fixture implementation.
			httpServer.SetPrefix("/vgi")
			secrets, err := vgirpc.ParseProofSecrets(findFlagValue(os.Args, "--proof-secrets"))
			if err != nil {
				fmt.Fprintf(os.Stderr, "invalid --proof-secrets: %v\n", err)
				os.Exit(1)
			}
			skew := 30
			if v := findFlagValue(os.Args, "--proof-skew"); v != "" {
				parsed, perr := strconv.Atoi(v)
				if perr != nil {
					fmt.Fprintf(os.Stderr, "invalid --proof-skew: %v\n", perr)
					os.Exit(1)
				}
				skew = parsed
			}
			mode := vgirpc.ProofMode(findFlagValue(os.Args, "--proof-mode"))
			if mode == "" {
				mode = vgirpc.ProofModeRequire
			}
			originID := findFlagValue(os.Args, "--proof-origin-id")
			if originID == "" {
				originID = "conformance-origin"
			}
			disableReplay := false
			for _, a := range os.Args {
				if a == "--proof-no-replay-cache" {
					disableReplay = true
				}
			}
			gate, gerr := vgirpc.ProofAuthenticate(vgirpc.ProofConfig{
				Mode:               mode,
				OriginID:           originID,
				Secrets:            secrets,
				SkewSeconds:        skew,
				DisableReplayCache: disableReplay,
			}, nil)
			if gerr != nil {
				fmt.Fprintf(os.Stderr, "invalid proof config: %v\n", gerr)
				os.Exit(1)
			}
			httpServer.SetAuthenticate(gate)
			httpServer.SetProxyProofRequired(mode == vgirpc.ProofModeRequire)
		}
		if pkceMode {
			idpURL := findFlagValue(os.Args, "--idp-url")
			if idpURL == "" {
				idpURL = "http://127.0.0.1:9999"
			}
			resource := findFlagValue(os.Args, "--resource")
			if resource == "" {
				resource = "http://127.0.0.1:8000/vgi"
			}
			httpServer.SetPrefix("/vgi")
			httpServer.SetAuthenticate(func(*http.Request) (*vgirpc.AuthContext, error) {
				return nil, &vgirpc.RpcError{Type: "ValueError", Message: "auth required"}
			})
			if err := httpServer.SetOAuthResourceMetadata(&vgirpc.OAuthResourceMetadata{
				Resource:             resource,
				AuthorizationServers: []string{idpURL},
				ClientID:             "my-client-id",
				ClientSecret:         "my-client-secret",
			}); err != nil {
				fmt.Fprintf(os.Stderr, "SetOAuthResourceMetadata: %v\n", err)
				os.Exit(1)
			}
			if err := httpServer.SetOAuthPkce(vgirpc.OAuthPkceConfig{}); err != nil {
				fmt.Fprintf(os.Stderr, "SetOAuthPkce: %v\n", err)
				os.Exit(1)
			}
		}
		if strictMode {
			httpServer.SetMaxResponseBytes(maxResponseBytes)
			httpServer.SetMaxExternalizedResponseBytes(maxExternalizedResponseBytes)
		}
		// Enable sticky sessions on every HTTP conformance variant.
		// Mirrors the Python conformance worker's `enable_sticky=True`
		// default in tests/serve_conformance_http.py, so the canonical
		// TestSticky conformance group runs against the Go worker via
		// `conformance_http_port`. The default TTL (zero) falls back to
		// 300 seconds matching Python's `sticky_default_ttl`.
		// --sticky-ttl <seconds> shortens it so the expiry conformance case has
		// something it can outwait; the header advertises whole seconds, so the
		// flag is an integer.
		stickyTTL := time.Duration(0)
		if v := findFlagValue(os.Args, "--sticky-ttl"); v != "" {
			secs, terr := strconv.Atoi(v)
			if terr != nil || secs <= 0 {
				fmt.Fprintf(os.Stderr, "invalid --sticky-ttl (expected positive integer seconds): %q\n", v)
				os.Exit(1)
			}
			stickyTTL = time.Duration(secs) * time.Second
		}
		httpServer.EnableSticky(stickyTTL)
		// --sticky-auth resolves the principal named in X-Conformance-Principal
		// so one worker is reachable as two identities, which is what the
		// cross-principal replay case needs. Requests without the header stay
		// anonymous rather than being rejected: the suite probes /health and the
		// capability endpoint before it authenticates anything. Unlike
		// --http-auth above, this does NOT move the prefix — the suite connects
		// to this worker exactly as it connects to the plain one.
		if hasFlag(os.Args, "--sticky-auth") {
			httpServer.SetAuthenticate(principalFromHeader)
		}
		// --introspect enables POST /__introspect_token__ with the fixed
		// conformance resolver and a single-principal allowlist, backing the
		// shared TestTokenIntrospection group. It implies principal-header
		// auth so the allowlist has something to check, and leaves the prefix
		// where the plain worker serves. The companion off-mode group runs
		// against the default worker, which is why this needs its own.
		if hasFlag(os.Args, "--introspect") {
			httpServer.SetAuthenticate(principalFromHeader)
			if err := httpServer.EnableTokenIntrospection(vgirpc.TokenIntrospectionConfig{
				Resolver:           conformanceTokenResolver,
				Principals:         []string{conformanceIntrospector},
				RateLimitPerSecond: conformanceIntrospectRateLimit,
			}); err != nil {
				fmt.Fprintf(os.Stderr, "EnableTokenIntrospection: %v\n", err)
				os.Exit(1)
			}
		}
		// The conformance suite's test_echo_header_round_trip probes for
		// a fixed marker echo header; advertise it under the same name
		// as the Python worker (x-vgi-conformance-echo) so cross-language
		// clients exercise the same contract.
		httpServer.SetStickyEchoHeaders(map[string]string{
			"x-vgi-conformance-echo": "conformance-fixed-marker",
		})
		// Mount the test-only admin endpoint that flips the drain flag
		// over the wire. TestSticky::test_drain_rejects_new_opens needs
		// this to test drain semantics without sending SIGTERM. Routes:
		//   POST /__test_drain__  → drain.Drain()
		//   DELETE /__test_drain__ → drain.ClearDrain()
		// Both 204. Not exposed in production make_wsgi_app paths.
		if drain := httpServer.DrainHandle(); drain != nil {
			httpServer.Handle("POST /__test_drain__", testDrainHandler(drain, true))
			httpServer.Handle("DELETE /__test_drain__", testDrainHandler(drain, false))
		}
		// --no-compression backs the shared conformance case
		// test_empty_advertisement_means_never_compressed. The state under
		// test is a *server configuration* — "I can produce no codecs" —
		// which no client request can induce (identity is the client-side
		// opt-out and is covered separately), and it is the only way to put
		// a present-but-empty VGI-Supported-Encodings on the wire. Level 0
		// is Go's opt-out; it must override the level set below, so it is
		// resolved here rather than in a second call.
		compressionLevel := 3
		if hasFlag(os.Args, "--no-compression") {
			compressionLevel = 0
		}
		if err := httpServer.SetCompressionLevel(compressionLevel); err != nil {
			fmt.Fprintf(os.Stderr, "failed to set compression level: %v\n", err)
			os.Exit(1)
		}
		// --cors-origin <origin> allows that origin cross-origin, backing the
		// shared TestCors group. Left unset everywhere else on purpose:
		// TestCorsOffMode asserts an unconfigured worker grants no origin at all.
		if origin := findFlagValue(os.Args, "--cors-origin"); origin != "" {
			httpServer.SetCorsOrigins(origin)
		}
		// Emit one batch per HTTP response so infinite producers (e.g.
		// ``cancellable_producer``) return promptly and the client can follow
		// continuation tokens or cancel mid-stream. Matches the Python
		// reference server's default.
		httpServer.SetProducerBatchLimit(1)

		listenAddr := "127.0.0.1:0"
		if portFlag := findFlagValue(os.Args, "--port"); portFlag != "" {
			listenAddr = "127.0.0.1:" + portFlag
		}
		listener, err := net.Listen("tcp", listenAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to listen: %v\n", err)
			os.Exit(1)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		fmt.Printf("PORT:%d\n", port)
		os.Stdout.Sync()

		srv := &http.Server{Handler: httpServer}

		// Catch SIGTERM/SIGINT so the process exits cleanly and flushes
		// coverage data when built with -cover.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
		go func() {
			<-sigCh
			shutdownCtx := context.Background()
			if tp != nil {
				tp.Shutdown(shutdownCtx)
			}
			if mp != nil {
				mp.Shutdown(shutdownCtx)
			}
			if otelFile != nil {
				otelFile.Close()
			}
			srv.Shutdown(shutdownCtx)
		}()

		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "http serve error: %v\n", err)
			os.Exit(1)
		}
	} else if len(os.Args) > 2 && os.Args[1] == "--unix" {
		path := os.Args[2]
		os.Remove(path)

		listener, err := net.Listen("unix", path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to listen on unix socket: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("UNIX:%s\n", path)
		os.Stdout.Sync()

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
		go func() {
			<-sigCh
			listener.Close()
		}()

		for {
			conn, err := listener.Accept()
			if err != nil {
				break
			}
			server.Serve(conn, conn)
			conn.Close()
		}
		os.Remove(path)
	} else if len(os.Args) > 2 && os.Args[1] == "--tcp" {
		// Raw-TCP transport: same Arrow-IPC framing as --unix, only the
		// listening socket differs. Address is [HOST:]PORT; host defaults to
		// 127.0.0.1 (loopback only) and PORT may be 0 to auto-select.
		//
		// SECURITY: raw TCP carries no authentication or TLS — trusted
		// networks only. Use --http for untrusted networks.
		addr := os.Args[2]
		host, portStr := "127.0.0.1", addr
		if i := strings.LastIndex(addr, ":"); i >= 0 {
			if addr[:i] != "" {
				host = addr[:i]
			}
			portStr = addr[i+1:]
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "--tcp expects [HOST:]PORT, got %q\n", addr)
			os.Exit(2)
		}

		// SIGTERM/SIGINT exits cleanly so coverage data (and the leak-check
		// summary) flush on shutdown, mirroring the --http path.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
		go func() {
			<-sigCh
			os.Exit(0)
		}()

		// RunTcp prints the launcher readiness marker "TCP:<host>:<port>" in
		// onBound (after bind succeeds; the actual port is reported when
		// port==0). After this line the worker MUST NOT write more to stdout.
		if err := server.RunTcp(host, port, 0, func(boundHost string, boundPort int) {
			fmt.Printf("TCP:%s:%d\n", boundHost, boundPort)
			os.Stdout.Sync()
		}); err != nil {
			fmt.Fprintf(os.Stderr, "tcp serve error: %v\n", err)
			os.Exit(1)
		}
	} else {
		server.RunStdio()
	}
}

// findFlagValue scans args for "--name <value>" and returns the value, or
// "" if not found. Used so the --access-log flag can appear before or
// after the transport-mode positional arg.
func findFlagValue(args []string, name string) string {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == name {
			return args[i+1]
		}
	}
	return ""
}

// hasFlag reports whether a valueless flag appears anywhere in args. The
// value-taking flags are parsed by a loop that stops one short of the end
// (it always reads args[i+1]), so a boolean flag has to be scanned for
// separately to work in the final position.
func hasFlag(args []string, name string) bool {
	for _, a := range args {
		if a == name {
			return true
		}
	}
	return false
}

// testDrainHandler returns an http.HandlerFunc that flips the sticky
// drain flag. POST drain, DELETE undrain — both 204. Mirrors the
// Python conformance worker's _TestDrainResource in
// tests/serve_conformance_http.py. Not exposed in production paths.
func testDrainHandler(handle *vgirpc.DrainHandle, drain bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if drain {
			handle.Drain()
		} else {
			handle.ClearDrain()
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
