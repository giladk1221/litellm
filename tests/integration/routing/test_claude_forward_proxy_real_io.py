import asyncio
import base64
import datetime
import ipaddress
import ssl
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import h11
import httpx
import pytest
import pytest_asyncio
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from starlette.types import Receive, Scope, Send

from litellm.proxy.claude_forward_proxy.protocol import GATEWAY_HEADERS, HTTPConnection
from litellm.proxy.claude_forward_proxy.server import ClaudeForwardProxy, duplex, forward_proxy_lifespan
from litellm.proxy.claude_forward_proxy.settings import ClaudeForwardProxySettings

pytestmark = pytest.mark.asyncio(loop_scope="session")
OAUTH: Final = "Bearer sk-ant-oat01-test-only"


@pytest.fixture
def certificates(tmp_path: Path) -> tuple[str, str, ssl.SSLContext]:
    key: Final = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject: Final = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "api.anthropic.com")])
    certificate: Final = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc))
        .not_valid_after(datetime.datetime(2100, 1, 1, tzinfo=datetime.timezone.utc))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("api.anthropic.com"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    certificate_path: Final = tmp_path / "certificate.pem"
    key_path: Final = tmp_path / "key.pem"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    return str(certificate_path), str(key_path), ssl.create_default_context(cafile=str(certificate_path))


class Application:
    def __init__(self) -> None:
        self.requests: asyncio.Queue[tuple[Scope, bytes]] = asyncio.Queue()
        self.active = True
        self.release_stream = asyncio.Event()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        headers: Final = dict(scope["headers"])
        allowed: Final = self.active and headers.get(b"x-litellm-api-key") == b"Bearer sk-test"
        if scope["path"] == "/v1/models" or not allowed:
            await send({"type": "http.response.start", "status": 200 if allowed else 401, "headers": []})
            await send({"type": "http.response.body", "body": b"{}"})
            return
        message: Final = await receive()
        await self.requests.put((scope, message["body"]))
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"text/event-stream"),
                    (b"llm_provider-anthropic-ratelimit-unified-status", b"allowed"),
                    (b"llm_provider-retry-after", b"7"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": b"event: ping\ndata: {}\n\n", "more_body": True})
        await self.release_stream.wait()
        await send({"type": "http.response.body", "body": b"event: message_stop\ndata: {}\n\n"})


@dataclass(frozen=True)
class Running:
    port: int
    tls: ssl.SSLContext
    app: Application
    upstream: asyncio.Queue[tuple[h11.Request, bytes]]
    proxy: ClaudeForwardProxy
    secure: bool

    def client(self, key: str = "sk-test") -> httpx.AsyncClient:
        scheme: Final = "https" if self.secure else "http"
        proxy: Final = httpx.Proxy(
            f"{scheme}://litellm:{key}@127.0.0.1:{self.port}", ssl_context=self.tls if self.secure else None
        )
        return httpx.AsyncClient(proxy=proxy, verify=self.tls, timeout=5)

    async def connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port, ssl=self.tls if self.secure else None)
        credential: Final = base64.b64encode(b"litellm:sk-test")
        writer.write(
            b"CONNECT api.anthropic.com:443 HTTP/1.1\r\nHost: api.anthropic.com:443\r\nProxy-Authorization: Basic "
            + credential
            + b"\r\n\r\n"
        )
        await writer.drain()
        response: Final = await reader.readuntil(b"\r\n\r\n")
        assert b" 200 " in response
        await writer.start_tls(self.tls, server_hostname="api.anthropic.com")
        return reader, writer


@pytest_asyncio.fixture
async def running(
    certificates: tuple[str, str, ssl.SSLContext], request: pytest.FixtureRequest
) -> AsyncIterator[Running]:
    certificate, key, tls = certificates
    app: Final = Application()
    requests: Final[asyncio.Queue[tuple[h11.Request, bytes]]] = asyncio.Queue()

    async def upstream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connection: Final = HTTPConnection(reader, writer, 5)
        try:
            request: Final = await connection.event()
            assert isinstance(request, h11.Request)
            body: Final = await connection.body(100000)
            assert body is not None
            await requests.put((request, body))
            if dict(request.headers).get(b"upgrade") == b"websocket":
                await connection.send(
                    h11.InformationalResponse(
                        status_code=101, headers=[(b"connection", b"upgrade"), (b"upgrade", b"websocket")]
                    )
                )
                data: Final = await reader.readexactly(4)
                writer.write(data)
                await writer.drain()
            else:
                await connection.send(
                    h11.Response(
                        status_code=200,
                        headers=[(b"content-type", b"application/json"), (b"anthropic-feature", b"native")],
                    )
                )
                await connection.send(h11.Data(data=b'{"service":"native"}'))
                await connection.send(h11.EndOfMessage())
        finally:
            writer.close()

    service: Final = await asyncio.start_server(upstream, "127.0.0.1", 0)

    async def connector(hostname: str, encrypted: bool) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_connection("127.0.0.1", service.sockets[0].getsockname()[1])

    settings: Final = ClaudeForwardProxySettings(
        enabled=True,
        certificate=certificate,
        private_key=key,
        max_body_bytes=1024,
        service_paths=(*ClaudeForwardProxySettings().service_paths, "/api/voice"),
    )
    proxy: Final = ClaudeForwardProxy(app, settings, connector, GATEWAY_HEADERS | {b"x-gateway-key"})
    secure: Final = getattr(request, "param", False) is True
    server: Final = await asyncio.start_server(
        proxy.accept, "127.0.0.1", 0, ssl=proxy.inspection_tls if secure else None
    )
    try:
        yield Running(server.sockets[0].getsockname()[1], tls, app, requests, proxy, secure)
    finally:
        app.release_stream.set()
        server.close()
        service.close()
        await server.wait_closed()
        await service.wait_closed()
        await proxy.close()


@pytest.mark.parametrize("running", [False, True], indirect=True, ids=["http-connect", "https-connect"])
async def test_inference_preserves_oauth_payload_headers_and_incremental_streaming(running: Running) -> None:
    payload: Final = b'{"model":"claude-test","messages":[],"future_field":{"enabled":true}}'
    async with running.client() as client:
        async with client.stream(
            "POST",
            "https://api.anthropic.com/v1/messages?beta=true",
            content=payload,
            headers={"Authorization": OAUTH, "Anthropic-Beta": "test-beta"},
        ) as response:
            assert response.status_code == 200
            assert response.headers["anthropic-ratelimit-unified-status"] == "allowed"
            assert response.headers["retry-after"] == "7"
            chunks: Final = response.aiter_raw()
            assert await asyncio.wait_for(anext(chunks), 2) == b"event: ping\ndata: {}\n\n"
            scope, received = await running.app.requests.get()
            assert received == payload
            assert dict(scope["headers"])[b"authorization"] == OAUTH.encode()
            assert dict(scope["headers"])[b"anthropic-beta"] == b"test-beta"
            assert scope["query_string"] == b"beta=true"
            assert b"proxy-authorization" not in dict(scope["headers"])
            assert running.upstream.empty()
            running.app.release_stream.set()
            assert await anext(chunks) == b"event: message_stop\ndata: {}\n\n"


@pytest.mark.parametrize(
    "path",
    ["/api/oauth/usage", "/api/oauth/profile", "/api/claude_code/environments", "/api/event_logging/v2/batch"],
)
async def test_feature_services_keep_native_credentials_and_never_receive_gateway_keys(
    running: Running, path: str
) -> None:
    async with running.client() as client:
        response: Final = await client.post(
            "https://api.anthropic.com" + path,
            content=b"{}",
            headers={
                "Authorization": OAUTH,
                "x-litellm-api-key": "untrusted",
                "API-Key": "gateway-secret",
                "x-goog-api-key": "gateway-secret",
                "Ocp-Apim-Subscription-Key": "gateway-secret",
                "x-gateway-key": "gateway-secret",
            },
        )
    assert response.json() == {"service": "native"}
    assert response.headers["anthropic-feature"] == "native"
    request, body = await running.upstream.get()
    assert request.target == path.encode()
    assert body == b"{}"
    assert dict(request.headers)[b"authorization"] == OAUTH.encode()
    assert b"x-litellm-api-key" not in dict(request.headers)
    assert b"proxy-authorization" not in dict(request.headers)
    assert b"gateway-secret" not in b" ".join(value for _, value in request.headers)
    assert running.app.requests.empty()


async def test_invalid_gateway_key_never_reaches_anthropic(running: Running) -> None:
    async with running.client("sk-invalid") as client:
        with pytest.raises(httpx.ProxyError, match="407"):
            await client.get("https://api.anthropic.com/api/oauth/usage")
    assert running.upstream.empty()


async def test_api_key_inference_is_rejected_without_upstream_fallback(running: Running) -> None:
    async with running.client() as client:
        response: Final = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": "sk-ant-api-test"},
            json={"model": "claude-test"},
        )
    assert response.status_code == 401
    assert running.upstream.empty()
    assert running.app.requests.empty()


async def test_revocation_after_connect_blocks_the_inference_request(running: Running) -> None:
    reader, writer = await running.connect()
    try:
        running.app.active = False
        writer.write(
            b"POST /v1/messages HTTP/1.1\r\nHost: api.anthropic.com\r\nAuthorization: "
            + OAUTH.encode()
            + b"\r\nContent-Length: 2\r\n\r\n{}"
        )
        await writer.drain()
        assert b" 401 " in await reader.readuntil(b"\r\n\r\n")
        assert running.upstream.empty()
        assert running.app.requests.empty()
    finally:
        writer.close()


async def test_large_bodies_are_rejected_before_any_upstream_request(running: Running) -> None:
    async with running.client() as client:
        response: Final = await client.post(
            "https://api.anthropic.com/v1/messages", content=b"x" * 1025, headers={"Authorization": OAUTH}
        )
    assert response.status_code == 413
    assert running.upstream.empty()
    assert running.app.requests.empty()


@pytest.mark.parametrize(
    ("path", "status"),
    [
        ("/v1/%6dessages", 200),
        ("/v1/messages/../messages", 400),
        ("/v1/%256dessages", 400),
        ("//v1/messages", 400),
        ("/v1/messages/batches", 403),
        ("/v1/responses", 403),
    ],
)
async def test_alternate_paths_cannot_bypass_inference_governance(running: Running, path: str, status: int) -> None:
    reader, writer = await running.connect()
    try:
        writer.write(
            f"POST {path} HTTP/1.1\r\nHost: api.anthropic.com\r\nAuthorization: {OAUTH}\r\n"
            "Content-Length: 2\r\n\r\n{}".encode()
        )
        await writer.drain()
        assert f" {status} ".encode() in await reader.readuntil(b"\r\n\r\n")
        assert running.upstream.empty()
        assert running.app.requests.empty() is (status != 200)
    finally:
        running.app.release_stream.set()
        writer.close()


async def test_lifespan_shuts_down_active_connections_and_releases_port(
    certificates: tuple[str, str, ssl.SSLContext], unused_tcp_port: int
) -> None:
    certificate, key, _ = certificates
    app: Final = Application()
    settings: Final = ClaudeForwardProxySettings(
        enabled=True, certificate=certificate, private_key=key, port=unused_tcp_port
    )
    async with forward_proxy_lifespan(app, settings, {}, authenticated=True):
        reader, writer = await asyncio.open_connection("127.0.0.1", unused_tcp_port)
        writer.write(b"CONNECT ")
        await writer.drain()
    try:
        with suppress(ConnectionResetError):
            assert await asyncio.wait_for(reader.read(), 2) == b""
        with pytest.raises(ConnectionRefusedError):
            await asyncio.open_connection("127.0.0.1", unused_tcp_port)
    finally:
        writer.close()


@pytest.mark.parametrize("explicit", [False, True])
async def test_disabled_listener_needs_no_credentials_or_certificates(explicit: bool, unused_tcp_port: int) -> None:
    app: Final = Application()
    configured: Final = (
        {"enabled": False, "certificate": "/does-not-exist", "port": unused_tcp_port}
        if explicit
        else {"port": unused_tcp_port}
    )
    async with forward_proxy_lifespan(app, configured, {}, authenticated=False):
        with pytest.raises(ConnectionRefusedError):
            await asyncio.open_connection("127.0.0.1", unused_tcp_port)
    assert app.requests.empty()


async def test_enabled_listener_refuses_unauthenticated_gateway(
    certificates: tuple[str, str, ssl.SSLContext], unused_tcp_port: int
) -> None:
    certificate, key, _ = certificates
    settings: Final = ClaudeForwardProxySettings(
        enabled=True, certificate=certificate, private_key=key, port=unused_tcp_port
    )
    with pytest.raises(ValueError, match="master-key authentication"):
        async with forward_proxy_lifespan(Application(), settings, {}, authenticated=False):
            pytest.fail("Unauthenticated listener started")
    with pytest.raises(ConnectionRefusedError):
        await asyncio.open_connection("127.0.0.1", unused_tcp_port)


@pytest.mark.parametrize("path", ["/v2/messages", "/v1/new-inference", "/api/unapproved", "/api/oauth-evil/usage"])
async def test_unclassified_service_routes_fail_closed(running: Running, path: str) -> None:
    async with running.client() as client:
        response: Final = await client.post("https://api.anthropic.com" + path, content=b"{}")
    assert response.status_code == 403
    assert running.app.requests.empty()
    assert running.upstream.empty()


async def test_token_count_uses_gateway_authentication_and_model_pipeline(running: Running) -> None:
    running.app.release_stream.set()
    async with running.client() as client:
        response: Final = await client.post(
            "https://api.anthropic.com/v1/messages/count_tokens",
            content=b'{"model":"claude-test","messages":[]}',
            headers={"Authorization": OAUTH, "X-Forwarded-For": "203.0.113.9", "Forwarded": "for=203.0.113.9"},
        )
    assert response.status_code == 200
    scope, body = await running.app.requests.get()
    assert scope["path"] == "/v1/messages/count_tokens"
    assert b"claude-test" in body
    assert b"x-forwarded-for" not in dict(scope["headers"])
    assert b"forwarded" not in dict(scope["headers"])
    assert running.upstream.empty()


async def test_connection_lifetime_bounds_incomplete_client_requests(
    certificates: tuple[str, str, ssl.SSLContext], unused_tcp_port: int
) -> None:
    certificate, key, _ = certificates
    settings: Final = ClaudeForwardProxySettings(
        enabled=True,
        certificate=certificate,
        private_key=key,
        port=unused_tcp_port,
        connection_lifetime_seconds=0.05,
    )
    async with forward_proxy_lifespan(Application(), settings, {}, authenticated=True):
        reader, writer = await asyncio.open_connection("127.0.0.1", unused_tcp_port)
        try:
            writer.write(b"CONNECT ")
            await writer.drain()
            assert await asyncio.wait_for(reader.read(), 2) == b""
        finally:
            writer.close()


async def test_feature_websocket_upgrade_retains_bidirectional_bytes(running: Running) -> None:
    reader, writer = await running.connect()
    try:
        writer.write(
            b"GET /api/voice HTTP/1.1\r\nHost: api.anthropic.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"
        )
        await writer.drain()
        assert b" 101 " in await reader.readuntil(b"\r\n\r\n")
        writer.write(b"test")
        await writer.drain()
        assert await asyncio.wait_for(reader.readexactly(4), 2) == b"test"
    finally:
        writer.close()


async def test_one_way_activity_keeps_a_duplex_tunnel_open() -> None:
    connections: Final[asyncio.Queue[tuple[asyncio.StreamReader, asyncio.StreamWriter]]] = asyncio.Queue()
    server: Final = await asyncio.start_server(
        lambda reader, writer: connections.put_nowait((reader, writer)), "127.0.0.1", 0
    )
    port: Final = server.sockets[0].getsockname()[1]
    client_reader, client_writer = await asyncio.open_connection("127.0.0.1", port)
    upstream_reader, upstream_writer = await asyncio.open_connection("127.0.0.1", port)
    left, left_writer = await connections.get()
    right, right_writer = await connections.get()
    tunnel: Final = asyncio.create_task(duplex(left, left_writer, right, right_writer, timeout=0.5))
    try:
        for _ in range(8):
            upstream_writer.write(b"ping")
            await upstream_writer.drain()
            assert await asyncio.wait_for(client_reader.readexactly(4), 1) == b"ping"
            await asyncio.sleep(0.1)
        client_writer.write(b"pong")
        await client_writer.drain()
        assert await asyncio.wait_for(upstream_reader.readexactly(4), 1) == b"pong"
        done, _ = await asyncio.wait((tunnel,), timeout=2)
        assert tunnel in done
        with pytest.raises(TimeoutError):
            await tunnel
    finally:
        tunnel.cancel()
        await asyncio.gather(tunnel, return_exceptions=True)
        for writer in (client_writer, upstream_writer, left_writer, right_writer):
            writer.close()
        server.close()
        await server.wait_closed()


@pytest.mark.parametrize("destination", ["127.0.0.1:443", "api.anthropic.com:80", "api.anthropic.com.evil.test:443"])
async def test_unapproved_connect_destinations_are_rejected(running: Running, destination: str) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", running.port)
    try:
        writer.write(f"CONNECT {destination} HTTP/1.1\r\nHost: {destination}\r\n\r\n".encode())
        await writer.drain()
        assert b" 403 " in await reader.readuntil(b"\r\n\r\n")
        assert running.upstream.empty()
    finally:
        writer.close()
