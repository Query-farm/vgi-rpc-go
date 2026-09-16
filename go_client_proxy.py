"""Python shim that drives the Go ``vgirpc`` client for conformance.

Everything port-agnostic lives in ``vgi_rpc.conformance.client_driver`` in the
Python reference, and the control protocol it speaks is written down in that
repo's ``tools/cross-port/specs/CLIENT_DRIVER_PROTOCOL.md``.  What is left here
is the only genuinely Go-shaped part: where this repository's driver binary
lives, and which transports its client can actually reach.

Why this file exists at all is worth stating, because the Go client was already
covered by a green suite.  It was covered against the *Go server* -- the one
configuration that cannot validate a client, because every accommodation a
server makes for the client it ships with is invisible to exactly that pair.
The gate is ``VGI_CONFORMANCE_SERVER=python``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

from vgi_rpc.conformance.client_driver import ClientDriver, ClientDriverProxy
from vgi_rpc.external import ExternalLocationConfig
from vgi_rpc.log import Message

# ``VGI_CLIENT_DRIVER`` wins when set (every CI leg sets it); this is the
# developer-machine fallback, and matches the Makefile's build output.
_DEFAULT_DRIVER = str(Path(__file__).parent / "conformance-client-driver")

DRIVER = ClientDriver.from_env(default=[_DEFAULT_DRIVER])

#: Transports this port's client can open.
#:
#: ``stdio`` and ``shm`` are absent because the Go port has a stdio *server*
#: and no stdio *client*: there is nothing here to drive.  They are listed as a
#: known gap rather than quietly mapped onto another transport, because a
#: driver that substituted one would report a pass for a call the client cannot
#: make.  The wire is not lost -- ``unix`` and ``tcp`` carry the same raw Arrow
#: IPC framing that ``stdio`` does, over a socket instead of a pipe.
CLIENT_TRANSPORTS = ("http", "http_externalize_always", "unix", "tcp")


def GoClientProxy(  # noqa: N802 - kept as a class-like name for the harness
    transport: str,
    target: object,
    on_log: Callable[[Message], None] | None = None,
    *,
    external_config: ExternalLocationConfig | None = None,
    compression_level: int | None = 1,
    headers: Mapping[str, str] | None = None,
) -> ClientDriverProxy:
    """Open one driver-backed connection to the Go client."""
    return DRIVER.connect(
        transport,
        target,
        on_log,
        external_config=external_config,
        compression_level=compression_level,
        headers=headers,
    )


go_http_connect = DRIVER.http_connect
go_http_capabilities = DRIVER.http_capabilities
go_request_upload_urls = DRIVER.request_upload_urls
go_http_introspect = DRIVER.http_introspect
