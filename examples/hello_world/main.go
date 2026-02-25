// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/Query-farm/vgi-rpc/vgirpc"
)

type GreetParams struct {
	Name string `vgirpc:"name"`
}

type AddParams struct {
	A float64 `vgirpc:"a"`
	B float64 `vgirpc:"b"`
}

func main() {
	server := vgirpc.NewServer()

	vgirpc.Unary(server, "greet", func(_ context.Context, ctx *vgirpc.CallContext, p GreetParams) (string, error) {
		return "Hello, " + p.Name + "!", nil
	})

	vgirpc.Unary(server, "add", func(_ context.Context, ctx *vgirpc.CallContext, p AddParams) (float64, error) {
		return p.A + p.B, nil
	})

	if len(os.Args) > 1 && os.Args[1] == "--http" {
		httpServer := vgirpc.NewHttpServer(server)

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to listen: %v\n", err)
			os.Exit(1)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		fmt.Printf("PORT:%d\n", port)
		os.Stdout.Sync()

		if err := http.Serve(listener, httpServer); err != nil {
			fmt.Fprintf(os.Stderr, "http serve error: %v\n", err)
			os.Exit(1)
		}
	} else {
		server.RunStdio()
	}
}
