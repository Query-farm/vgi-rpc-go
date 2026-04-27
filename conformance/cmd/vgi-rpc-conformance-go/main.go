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
	if (len(os.Args) > 1 && os.Args[1] == "--http") || authMode || storageMode || zstdStorageMode || pkceMode {
		// Parse optional flags that may follow positional args:
		//   --otel-export <path>
		//   --externalize-threshold <bytes>   (overrides default 8 KiB in storage modes)
		//   --max-request-bytes <bytes>       (overrides default 4 KiB inline request cap)
		var otelExportPath string
		externalizeThreshold := int64(-1) // -1 == not specified
		maxRequestBytes := int64(-1)      // -1 == not specified
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
