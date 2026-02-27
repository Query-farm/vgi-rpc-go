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
)

func main() {
	server := vgirpc.NewServer()
	server.SetDebugErrors(true)
	conformance.RegisterMethods(server)

	if len(os.Args) > 1 && os.Args[1] == "--http" {
		httpServer := vgirpc.NewHttpServer(server)
		httpServer.SetCompressionLevel(3)

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
			srv.Shutdown(context.Background())
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
