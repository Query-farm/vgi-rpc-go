# SPIFFE identity behind trusted HTTP proxies

VGI can consume stable SPIFFE workload evidence from a narrowly configured HTTP
proxy boundary. These adapters do not validate a certificate chain themselves:
the adjacent proxy or cloud load balancer validates the chain against the
intended SPIFFE bundle, removes caller-supplied identity headers, and replaces
them with its own values. VGI verifies the exact immediate proxy IP and the leaf
X.509-SVID profile before creating `configured_proxy` evidence.

```go
boundary := vgirpc.SpiffeProxyOptions{
    TrustDomains: []string{"example.org"},
    TrustedProxyAddresses: []string{"127.0.0.1", "10.0.0.8"},
}

envoy, err := vgirpc.NewEnvoyXFCCSpiffeProvider(
    vgirpc.EnvoyXFCCSpiffeOptions{SpiffeProxyOptions: boundary},
)
server.SetPeerIdentityProviders(envoy)
server.SetPeerAuthenticationPolicy(vgirpc.PeerIdentityPrimary("spiffe"))
```

## Direct raw TCP mTLS

Raw TCP workers can terminate mTLS themselves and snapshot the verified client
X.509-SVID for the entire stateful connection:

```go
err := server.RunTcpWithOptions("0.0.0.0", 9400, vgirpc.TcpServerOptions{
    TLSConfig: &tls.Config{
        Certificates: []tls.Certificate{serverCertificate},
        ClientCAs:    workloadBundle,
        MinVersion:   tls.VersionTLS13,
    },
    SpiffeTrustDomains:      []string{"example.org"},
    TLSHandshakeTimeout:     5 * time.Second,
    PeerAuthenticationPolicy: vgirpc.PeerIdentityPrimary("spiffe"),
})
```

The server clones the TLS configuration, independently clones its client/root
trust pools and certificate byte backing, forces `tls.RequireAndVerifyClientCert`,
verifies the chain with `ClientCAs`, and then applies the strict X.509-SVID leaf
profile and trust-domain allowlist. Evidence
uses `provider="spiffe"`, `evidence_source="direct_tls"`, and
`assurance="cryptographic_peer"`. Without an explicit
`PeerAuthenticationPolicy`, the evidence remains observational and does not
authenticate the connection.

When `ProxyProtocolV2Required` is also enabled, VGI first authenticates the
immediate proxy address and consumes its bounded PROXY v2 preamble, then starts
TLS on the remaining stream. PROXY parsing, TLS, and optional identity resolution
consume one monotonic accepted-connection setup budget (the largest configured
stage timeout); earlier work reduces the time left for later stages. Socket
deadlines are cleared before VGI framing begins. A custom `GetConfigForClient`
callback is rejected because it could replace the enforced client-certificate
policy.

Certificate and bundle acquisition/rotation remain the application's or a
SPIFFE Workload API integration's responsibility. This API safely consumes a
current `tls.Config`; it does not yet hot-reload Workload API material itself.

Supported adapters are:

- `NewEnvoyXFCCSpiffeProvider`: one text-format `SANITIZE_SET` XFCC element,
  with exactly one URI and SHA-256 hash. Forwarded or appended chains fail.
- `NewNginxSpiffeProvider`: escaped PEM plus `X-SSL-Client-Verify: SUCCESS`.
- `NewAWSALBSpiffeProvider`: the ALB verify-mode leaf header. Because ALB does
  not emit a per-request `verified=true` header, deployment must guarantee
  verify mode, header replacement, the intended trust store, and an otherwise
  unreachable backend.
- `NewGCPLoadBalancerSpiffeProvider`: requires certificate-present,
  chain-verified, error-free, and SPIFFE-ID custom headers together.
- `NewAzureApplicationGatewaySpiffeProvider`: strict-mode rewrite variables for
  the certificate and `SUCCESS` verification result.
- `NewSpiffeX509HeaderProvider`: the generic escaped-PEM form with a mandatory
  positive verification header.

All providers reject untrusted immediate peers, repeated or case-varied header
names, controls, malformed percent encoding, oversized values, non-canonical
SPIFFE IDs, disallowed trust domains, multiple URI SANs, CA certificates, and
invalid X.509 key usage. Go's raw `http.Header` value slices preserve repeated
headers through the server boundary so ambiguity fails closed.

The backend must not be reachable around the trusted proxy. Proxy IP trust is
an exact-address list, not a CIDR. Certificate-header deployments should also
cap request-header sizes at the proxy and VGI server.
