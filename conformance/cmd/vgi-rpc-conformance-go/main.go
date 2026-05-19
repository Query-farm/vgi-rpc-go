// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/Query-farm/vgi-rpc/conformance"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	vgiotel "github.com/Query-farm/vgi-rpc/vgirpc/otel"

	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	defer func() {
		if s := vgirpc.LeakCheckSummary(); s != "" {
			fmt.Fprintln(os.Stderr, s)
		}
	}()

	server := vgirpc.NewServer()
	server.SetDebugErrors(true)
	server.SetServiceName("ConformanceService")
	server.SetServerID("conformance-go")
	// NOTE: SetProtocolVersion("1.0.0") is intentionally NOT called here
	// yet. The Go server's dispatch-boundary check is wired and ready, but
	// CI installs vgi-rpc from PyPI and the released package (<= 0.17.1)
	// doesn't yet send the vgi_rpc.protocol_version request metadata key.
	// Enabling enforcement here would reject every request with a
	// "Client: <not declared>" ProtocolVersionError. Re-enable once a PyPI
	// release containing the client-side change (Python 416c0d1) is the
	// CI minimum — at which point uncomment the line below to match
	// ConformanceService.protocol_version on the Python side.
	//   server.SetProtocolVersion("1.0.0")
	conformance.RegisterMethods(server)

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
		server.SetDispatchHook(vgirpc.NewAccessLogHook(f, "vgi-rpc-go-conformance"))
	}

	authMode := len(os.Args) > 1 && os.Args[1] == "--http-auth"
	storageMode := len(os.Args) > 1 && os.Args[1] == "--http-with-storage"
	zstdStorageMode := len(os.Args) > 1 && os.Args[1] == "--http-with-zstd-storage"
	pkceMode := len(os.Args) > 1 && os.Args[1] == "--http-pkce"
	strictMode := len(os.Args) > 1 && os.Args[1] == "--http-strict"
	if (len(os.Args) > 1 && os.Args[1] == "--http") || authMode || storageMode || zstdStorageMode || pkceMode || strictMode {
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

		httpServer := vgirpc.NewHttpServer(server)
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
		if authMode {
			httpServer.SetPrefix("/vgi")
			httpServer.SetAuthenticate(func(*http.Request) (*vgirpc.AuthContext, error) {
				return nil, &vgirpc.RpcError{Type: "ValueError", Message: "auth required"}
			})
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
		httpServer.EnableSticky(0)
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
		if err := httpServer.SetCompressionLevel(3); err != nil {
			fmt.Fprintf(os.Stderr, "failed to set compression level: %v\n", err)
			os.Exit(1)
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
