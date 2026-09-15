package vgirpc

// testProtocol is the routing key the in-package tests address.
//
// The wire protocol requires vgi_rpc.protocol on every request, including
// against a server hosting exactly one protocol, so tests that hand-frame a
// request have to name one. It matches the default primaryProtocolName() for a
// server whose SetServiceName was never called.
const testProtocol = "Service"
