package main

import (
	"context"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
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

	server.RunStdio()
}
