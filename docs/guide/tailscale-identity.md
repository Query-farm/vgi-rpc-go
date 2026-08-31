# Tailscale peer identity

VGI treats Tailscale as an evidence provider. The worker receives an immutable
peer-evidence snapshot, while the configured peer authentication policy decides
whether that evidence is observed, required, or composed with application
credentials.

## Tailscale Serve

```go
serveIdentity, err := vgirpc.NewTailscaleServeIdentityProvider(
    vgirpc.TailscaleServeOptions{
        Issuer: "tailnet:example.com",
        TrustedProxyAddresses: []string{"127.0.0.1", "::1"},
    },
)
if err != nil {
    log.Fatal(err)
}

server.SetPeerIdentityProviders(serveIdentity)
server.SetPeerAuthenticationPolicy(vgirpc.RequirePeerIdentity("tailscale"))
```

The backend must be reachable only through the exact configured proxy IPs.
Serve user headers produce a verified login subject with `login` stability;
they are deliberately not eligible for `PeerIdentityPrimary`, which requires a
stable subject. Application capabilities are verified opaque JSON. A tagged
node can therefore produce capability-only evidence with no subject. Funnel
requests never produce Tailnet identity.

The adapter accepts plain ASCII and strict RFC 2047 UTF-8 Q encoding. It rejects
duplicate headers, controls, B encoding, duplicate JSON keys, malformed
capability structures, and bounded-size/depth/count violations. Go's HTTP
server preserves repeated header values, allowing the provider to reject them
at the identity boundary.

See Tailscale's official [identity documentation][identity] and [Serve app
capability example][serve-caps].

## LocalAPI WhoIs

Unix socket:

```go
localIdentity, err := vgirpc.NewTailscaleLocalAPIIdentityProvider(
    vgirpc.TailscaleLocalAPIOptions{
        Issuer: "tailnet:example.com",
        UnixSocket: "/var/run/tailscale/tailscaled.sock",
    },
)
```

When no transport field is supplied, the provider selects the official native
transport for the current platform:

- Linux and other Unix systems use `/var/run/tailscale/tailscaled.sock`.
- Windows uses Tailscale's protected `tailscaled` named pipe with identification
  impersonation, matching `safesocket`.
- macOS first looks for the current user's open App Store `IPNExtension`
  same-user-proof using the same bounded `lsof` query as `safesocket`, then
  checks the standalone system extension's `/Library/Tailscale/ipnport` and
  `sameuserproof-$port` files, then falls back to the Unix socket.

`UnixSocket`, `NamedPipe`, and `Endpoint` are mutually exclusive explicit
overrides. macOS discovery never invokes the `tailscale` CLI; the App Store
variant does invoke the system `lsof` utility because that is how Tailscale
proves the credential file is held open by the current user's `IPNExtension`.

Configured local HTTP endpoint, including the macOS same-user-proof password:

```go
localIdentity, err := vgirpc.NewTailscaleLocalAPIIdentityProvider(
    vgirpc.TailscaleLocalAPIOptions{
        Issuer: "tailnet:example.com",
        Endpoint: "http://127.0.0.1:49152",
        Password: localAPIPassword,
    },
)
```

Each resolution performs a fresh `GET /localapi/v0/whois` with the official
`Host: local-tailscaled.sock`. The adapter neither invokes the Tailscale CLI nor
caches results, and its HTTP transport never consults proxy environment
variables. `svc_name` takes precedence over `dst_ip` for destination-scoped
capabilities. One combined caller/provider deadline covers dialing, headers,
and the bounded response body.

Untagged nodes use `user:<numeric UserProfile.ID>` as their stable subject.
Tagged nodes ignore `UserProfile` as caller identity and instead use
`node:<StableNodeID>`. Names and tags remain attributes. Permission denied,
WhoIs no-match, daemon unavailability, timeout, and invalid responses remain
distinct evidence statuses.

The official Tailscale [LocalAPI WhoIs implementation][whois] defines the
request, status, and destination-scoping behavior.

Both adapters are disabled unless explicitly configured. VGI does not manage
tailnet membership, auth keys, routes, grants, or node lifecycle.

For HTTP, provider timeout, capacity exhaustion, and a typed
`AuthUnavailableError` are recorded as that provider's `unavailable` evidence
status. Observation and an already-valid application factor in `any_of` may
therefore continue. Invalid or untrusted provider evidence still rejects the
request, while `require` and peer-primary policies return HTTP 503.

Raw TCP's current `ResolveIdentity` hook is an application-owned aggregate
resolver rather than a list of named provider adapters. It can return named
`unavailable` evidence and valid application auth for policy composition, but
if the aggregate hook itself exceeds the server timeout or global concurrency
limit VGI must fail connection setup: at that boundary it cannot safely know
which provider was unavailable or whether another provider had already found
invalid evidence. Applications needing raw-TCP fallback must orchestrate their
providers inside the hook and return before the outer deadline.

The platform-specific code is covered by injected discovery/dial tests and
Windows/macOS cross-builds. A Linux CI host cannot prove Windows named-pipe ACL
and impersonation behavior or the lifecycle and permissions of either macOS
GUI variant; release qualification still requires real Windows and macOS
runners with installed Tailscale clients.

[identity]: https://tailscale.com/docs/concepts/tailscale-identity
[serve-caps]: https://tailscale.com/docs/reference/examples/serve#forward-app-capabilities-to-a-local-service
[whois]: https://github.com/tailscale/tailscale/blob/main/ipn/localapi/localapi.go
