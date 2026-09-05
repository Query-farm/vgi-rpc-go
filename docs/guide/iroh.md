# Iroh clients

The Go SDK defines the complete VGI-facing contract for both Iroh transports,
while keeping the Iroh runtime behind an explicit provider:

- `NewIrohClient` uses `vgi-rpc/arrow-mux/1` through an `IrohDialer` that returns
  a deadline-capable `net.Conn`.
- `NewIrohHTTPClient` uses `iroh-http/2` through an
  `IrohHTTPTransportProvider` that returns an `http.RoundTripper` plus `Close`.

The HTTP form retains the ordinary VGI HTTP state machine: OPTIONS discovery,
request and response budgets, compression, continuations, authentication
headers, and external-location resolution remain owned by `HttpClient`.

```go
irohOptions := vgirpc.IrohClientOptions{
    RelayURLs:      []string{"https://relay.example.com"},
    RemoteRelayURL: "https://relay.example.com",
    ConnectTimeout: 15 * time.Second,
    IOTimeout:      5 * time.Minute,
}

client, err := vgirpc.NewIrohHTTPClient(
    ctx,
    "httpi://<64-lowercase-hex-endpoint-id>/vgi",
    applicationProvider,
    irohOptions,
)
```

Set `NoRelay: true` only for deterministic direct/private-network dialing, and
usually pair it with `DirectAddresses`. `NoRelay` and `RelayURLs` are mutually
exclusive. `RemoteRelayURL` is an address hint for the peer; it does not select
the local endpoint's relay set.

## Why the provider is explicit

Iroh's maintained FFI project currently publishes Python, Swift, Kotlin/JVM,
and JavaScript packages and lists Go as community-maintained. VGI therefore
does not silently select an unrelated Go implementation, download a helper at
runtime, or require cgo for every Go consumer. Applications can pin and qualify
the community/native implementation appropriate to their deployment, while
the small VGI interfaces keep that choice replaceable.

See [Iroh's binding support matrix](https://github.com/n0-computer/iroh-ffi#published-packages).
When a maintained Go binding with the required endpoint, ALPN, relay, stream,
and cancellation behavior is available, it can be packaged as a separate
adapter without changing the VGI client API.

Provider implementations must select `IrohEndpoint.ALPN` exactly, preserve the
configured process/stable secret, consume custom relay and direct-address
hints, honor context cancellation and deadlines, authenticate the remote
EndpointId, and return structured `IrohTransportError` details when possible.
