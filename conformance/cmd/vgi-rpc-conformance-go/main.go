package main

import (
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/Query-farm/vgi-rpc-go/conformance"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

func main() {
	server := vgirpc.NewServer()
	conformance.RegisterMethods(server)

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
