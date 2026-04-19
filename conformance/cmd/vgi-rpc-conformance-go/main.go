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
	server := vgirpc.NewServer()
	server.SetDebugErrors(true)
	conformance.RegisterMethods(server)

	if len(os.Args) > 1 && os.Args[1] == "--http" {
		// Parse optional --otel-export flag
		var otelExportPath string
		for i := 2; i < len(os.Args)-1; i++ {
			if os.Args[i] == "--otel-export" {
				otelExportPath = os.Args[i+1]
				break
			}
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
		httpServer.SetCompressionLevel(3)
		// Emit one batch per HTTP response so infinite producers (e.g.
		// ``cancellable_producer``) return promptly and the client can follow
		// continuation tokens or cancel mid-stream. Matches the Python
		// reference server's default.
		httpServer.SetProducerBatchLimit(1)

		listener, err := net.Listen("tcp", "127.0.0.1:0")
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
