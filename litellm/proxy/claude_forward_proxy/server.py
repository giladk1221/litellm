import asyncio
import ipaddress
import os
import socket
import ssl
import sys
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager, suppress
from typing import Final, TypeAlias
from urllib.parse import unquote_to_bytes
from uuid import uuid4

import h11
import httpx
from pydantic import TypeAdapter
from starlette.types import ASGIApp, Message, Receive, Scope, Send

import litellm
from litellm._internal_context import native_anthropic_subscription
from litellm._logging import verbose_proxy_logger
from litellm.proxy.claude_forward_proxy.inference_policy import NativeInferencePolicy
from litellm.proxy.claude_forward_proxy.protocol import (
    GATEWAY_HEADERS,
    HTTPConnection,
    clean_headers,
    gateway_key,
    native_response_headers,
)
from litellm.proxy.claude_forward_proxy.settings import ClaudeForwardProxySettings
from litellm.proxy.rust_forward.mode import validate_claude_listener_runtime

ANTHROPIC_HOST: Final = "api.anthropic.com"
STATUS_CODE: Final = TypeAdapter(int)
RESPONSE_BODY: Final = TypeAdapter(bytes)
PEER_ADDRESS: Final[TypeAdapter[tuple[str, int] | tuple[str, int, int, int]]] = TypeAdapter(
    tuple[str, int] | tuple[str, int, int, int]
)
Connector: TypeAlias = Callable[[str, bool], Awaitable[tuple[asyncio.StreamReader, asyncio.StreamWriter]]]


def client_address(connection: HTTPConnection) -> tuple[str, int]:
    address: Final = PEER_ADDRESS.validate_python(connection.writer.get_extra_info("peername"), strict=True)
    return (address[0], address[1])


async def public_connection(hostname: str, encrypted: bool) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    addresses: Final = await asyncio.wait_for(
        asyncio.get_running_loop().getaddrinfo(hostname, 443, type=socket.SOCK_STREAM), timeout=30
    )
    if not addresses or any(not ipaddress.ip_address(address[4][0]).is_global for address in addresses):
        raise OSError("Forward proxy destinations must resolve to public addresses")
    return await asyncio.wait_for(
        asyncio.open_connection(
            str(addresses[0][4][0]),
            443,
            ssl=ssl.create_default_context() if encrypted else None,
            server_hostname=hostname if encrypted else None,
        ),
        timeout=30,
    )


async def copy_stream(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, timeout: float, activity: asyncio.Event
) -> None:
    while True:
        chunk = await reader.read(65536)
        if not chunk:
            return
        writer.write(chunk)
        await asyncio.wait_for(writer.drain(), timeout=timeout)
        activity.set()


async def idle_connection(activity: asyncio.Event, timeout: float) -> None:
    while True:
        activity.clear()
        await asyncio.wait_for(activity.wait(), timeout=timeout)


async def duplex(
    left: asyncio.StreamReader,
    left_writer: asyncio.StreamWriter,
    right: asyncio.StreamReader,
    right_writer: asyncio.StreamWriter,
    timeout: float,
) -> None:
    activity: Final = asyncio.Event()
    tasks: Final = (
        asyncio.create_task(copy_stream(left, right_writer, timeout, activity)),
        asyncio.create_task(copy_stream(right, left_writer, timeout, activity)),
        asyncio.create_task(idle_connection(activity, timeout)),
    )
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class RequestBody:
    def __init__(self, body: bytes, reader: asyncio.StreamReader) -> None:
        self.body = body
        self.reader = reader
        self.delivered = False

    async def receive(self) -> Message:
        if not self.delivered:
            self.delivered = True
            return {"type": "http.request", "body": self.body, "more_body": False}
        await self.reader.read(1)
        return {"type": "http.disconnect"}


class ClaudeForwardProxy:
    def __init__(
        self,
        app: ASGIApp,
        settings: ClaudeForwardProxySettings,
        connector: Connector = public_connection,
        credential_headers: frozenset[bytes] = GATEWAY_HEADERS,
    ) -> None:
        self.app = app
        self.settings = settings
        self.connector = connector
        self.credential_headers = credential_headers
        self.connections: frozenset[asyncio.Task[None]] = frozenset()
        self.inspection_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.inspection_tls.minimum_version = ssl.TLSVersion.TLSv1_2
        if not settings.enabled or settings.certificate is None or settings.private_key is None:
            raise ValueError("Claude forward proxy is disabled or has no inspection certificate")
        self.inspection_tls.load_cert_chain(settings.certificate, settings.private_key)
        self.inspection_tls.set_alpn_protocols(["http/1.1"])

    async def authorized(self, key: str, connection: HTTPConnection) -> bool:
        transport: Final = httpx.ASGITransport(app=self.app, client=client_address(connection))
        async with httpx.AsyncClient(transport=transport, base_url="http://litellm") as client:
            response: Final = await asyncio.wait_for(
                client.get("/v1/models", headers={"x-litellm-api-key": f"Bearer {key}"}), timeout=30
            )
            return response.status_code == 200

    def accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if len(self.connections) >= self.settings.max_connections:
            writer.close()
            return
        task: Final = asyncio.create_task(self.handle(reader, writer))
        self.connections = self.connections | {task}
        task.add_done_callback(self.completed)

    def completed(self, task: asyncio.Task[None]) -> None:
        self.connections = self.connections - {task}
        if not task.cancelled() and (error := task.exception()) is not None:
            verbose_proxy_logger.warning("Claude forward proxy connection failed (%s)", type(error).__name__)

    async def close(self, grace_period: float = 0) -> None:
        tasks: Final = tuple(self.connections)
        if tasks and grace_period > 0:
            await asyncio.wait(tasks, timeout=grace_period)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connection: Final = HTTPConnection(reader, writer, self.settings.idle_timeout_seconds)
        try:
            await asyncio.wait_for(self.connect(connection), timeout=self.settings.connection_lifetime_seconds)
        except (OSError, TimeoutError, ValueError, h11.ProtocolError, httpx.HTTPError):
            if connection.state.our_state is h11.SEND_RESPONSE:
                with suppress(OSError, h11.ProtocolError):
                    await connection.reject(502, b"Forward proxy connection failed")
        finally:
            writer.close()
            with suppress(OSError, TimeoutError):
                await asyncio.wait_for(writer.wait_closed(), timeout=5)

    async def connect(self, connection: HTTPConnection) -> None:
        request: Final = await connection.event()
        if not isinstance(request, h11.Request) or request.method != b"CONNECT":
            await connection.reject(405, b"Use HTTP CONNECT")
            return
        authority: Final = request.target.decode("ascii")
        hostname, _, port = authority.rpartition(":")
        if port != "443" or hostname != hostname.lower() or not hostname:
            await connection.reject(403, b"Destination not allowed")
            return
        if hostname != ANTHROPIC_HOST and not self.settings.permits_tunnel(hostname):
            await connection.reject(403, b"Destination not allowed")
            return
        key: Final = gateway_key(request.headers)
        if key is None or not await self.authorized(key, connection):
            await connection.reject(407, b"LiteLLM authentication required")
            return
        if await connection.body(0) != b"" or connection.state.trailing_data[0]:
            await connection.reject(400, b"CONNECT must not contain a body")
            return
        if hostname != ANTHROPIC_HOST:
            upstream, upstream_writer = await self.connector(hostname, False)
            try:
                await connection.send(h11.Response(status_code=200, headers=[]))
                await duplex(
                    connection.reader, connection.writer, upstream, upstream_writer, self.settings.idle_timeout_seconds
                )
            finally:
                upstream_writer.close()
            return
        await connection.send(h11.Response(status_code=200, headers=[]))
        if sys.version_info < (3, 11):
            return
        await connection.writer.start_tls(self.inspection_tls, ssl_handshake_timeout=30)
        inspected: Final = HTTPConnection(connection.reader, connection.writer, self.settings.idle_timeout_seconds)
        await self.inspected_request(inspected, key)

    async def inspected_request(self, connection: HTTPConnection, key: str) -> None:
        request: Final = await connection.event()
        if not isinstance(request, h11.Request):
            return
        headers: Final = dict(request.headers)
        if headers.get(b"host") not in (b"api.anthropic.com", b"api.anthropic.com:443"):
            await connection.reject(421, b"Unexpected host")
            return
        if not request.target.startswith(b"/") or request.target.startswith(b"//") or b"#" in request.target:
            await connection.reject(400, b"Expected origin-form request target")
            return
        path: Final = unquote_to_bytes(request.target.partition(b"?")[0])
        if any(token in path for token in (b"%", b"\\", b"//")) or any(
            part in (b".", b"..") for part in path.split(b"/")
        ):
            await connection.reject(400, b"Ambiguous request path")
            return
        is_inference: Final = path.rstrip(b"/") in (b"/v1/messages", b"/v1/messages/count_tokens")
        if is_inference:
            oauth: Final = headers.get(b"authorization", b"")
            if request.method != b"POST" or not oauth.startswith(b"Bearer sk-ant-oat") or b"x-api-key" in headers:
                await connection.reject(401, b"Claude subscription OAuth is required")
                return
        elif path.startswith((b"/v1/messages/", b"/v1/complete", b"/v1/chat/", b"/v1/responses")):
            await connection.reject(403, b"Inference must use POST /v1/messages")
            return
        elif not self.settings.permits_service(path):
            verbose_proxy_logger.debug("Claude native proxy denied service path %r", path)
            await connection.reject(403, b"Native service path is not approved in claude_forward_proxy.service_paths")
            return
        elif not await self.authorized(key, connection):
            await connection.reject(403, b"LiteLLM access revoked")
            return
        if int(headers.get(b"content-length", b"0")) > self.settings.max_body_bytes:
            await connection.reject(413, b"Request body too large")
            return
        if headers.get(b"expect", b"").lower() == b"100-continue":
            await connection.send(h11.InformationalResponse(status_code=100, headers=[]))
        body: Final = await connection.body(self.settings.max_body_bytes)
        if body is None:
            await connection.reject(413, b"Request body too large")
            return
        if is_inference:
            await self.inference(connection, request, body, key, path.rstrip(b"/"))
            return
        await self.service(connection, request, body)

    async def inference(
        self, connection: HTTPConnection, request: h11.Request, body: bytes, key: str, route: bytes
    ) -> None:
        path, _, query = request.target.partition(b"?")
        call_id: Final = str(uuid4())
        headers: Final = tuple(
            (name, value)
            for name, value in clean_headers(request.headers, self.credential_headers)
            if name not in (b"content-length", b"forwarded", b"x-forwarded-for", b"x-real-ip", b"cache-control")
        )
        scope: Final[Scope] = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "https",
            "path": route.decode("ascii"),
            "raw_path": path,
            "query_string": query,
            "root_path": "",
            "client": client_address(connection),
            "server": (ANTHROPIC_HOST, 443),
            "headers": [
                *headers,
                (b"content-length", str(len(body)).encode()),
                (b"x-litellm-api-key", f"Bearer {key}".encode()),
                (b"x-litellm-call-id", call_id.encode("ascii")),
                (b"cache-control", b"no-cache, no-store"),
            ],
        }

        async def send(message: Message) -> None:
            if message["type"] == "http.response.start":
                await connection.send(
                    h11.Response(
                        status_code=STATUS_CODE.validate_python(message["status"], strict=True),
                        headers=[*native_response_headers(message.get("headers", ())), (b"connection", b"close")],
                    )
                )
            elif message["type"] == "http.response.body":
                await connection.send(
                    h11.Data(data=RESPONSE_BODY.validate_python(message.get("body", b""), strict=True))
                )
                if not message.get("more_body", False):
                    await connection.send(h11.EndOfMessage())

        oauth: Final = dict(request.headers)[b"authorization"].removeprefix(b"Bearer ").decode("ascii")
        with native_anthropic_subscription(oauth, call_id):
            await self.app(scope, RequestBody(body, connection.reader).receive, send)

    async def service(self, connection: HTTPConnection, request: h11.Request, body: bytes) -> None:
        upstream, upstream_writer = await self.connector(ANTHROPIC_HOST, True)
        state: Final = h11.Connection(h11.CLIENT, max_incomplete_event_size=65536)
        headers: Final = tuple(
            (name, value)
            for name, value in clean_headers(request.headers, self.credential_headers)
            if name not in (b"content-length", b"expect")
        )
        upgrade: Final = dict(request.headers).get(b"upgrade")
        upgrade_headers: Final = (
            ((b"connection", b"upgrade"), (b"upgrade", upgrade)) if upgrade else ((b"connection", b"close"),)
        )
        try:
            for event in (
                h11.Request(
                    method=request.method,
                    target=request.target,
                    headers=[*headers, *upgrade_headers, (b"content-length", str(len(body)).encode())],
                ),
                h11.Data(data=body),
                h11.EndOfMessage(),
            ):
                upstream_writer.write(state.send(event))
            await asyncio.wait_for(upstream_writer.drain(), timeout=self.settings.idle_timeout_seconds)
            while True:
                response = state.next_event()
                if response is h11.NEED_DATA:
                    chunk = await asyncio.wait_for(upstream.read(65536), self.settings.idle_timeout_seconds)
                    state.receive_data(chunk)
                elif isinstance(response, h11.InformationalResponse) and response.status_code == 101:
                    await connection.send(response)
                    connection.writer.write(state.trailing_data[0])
                    upstream_writer.write(connection.state.trailing_data[0])
                    await duplex(
                        connection.reader,
                        connection.writer,
                        upstream,
                        upstream_writer,
                        self.settings.idle_timeout_seconds,
                    )
                    return
                elif isinstance(response, h11.Response):
                    await connection.send(
                        h11.Response(
                            status_code=response.status_code,
                            headers=[*clean_headers(response.headers), (b"connection", b"close")],
                        )
                    )
                elif isinstance(response, (h11.Data, h11.InformationalResponse)):
                    await connection.send(response)
                elif isinstance(response, h11.EndOfMessage):
                    await connection.send(h11.EndOfMessage())
                    return
                else:
                    raise h11.RemoteProtocolError("Upstream closed before completing its response")
        finally:
            upstream_writer.close()


@asynccontextmanager
async def forward_proxy_lifespan(
    app: ASGIApp,
    configured: object,
    state: Mapping[str, object],
    *,
    authenticated: bool = False,
    credential_header: object = None,
) -> AsyncGenerator[None]:
    if configured is None:
        yield
        return
    settings: Final = ClaudeForwardProxySettings.model_validate(configured)
    if not settings.enabled:
        yield
        return
    if not authenticated:
        raise ValueError("claude_forward_proxy requires LiteLLM master-key authentication")
    if sys.version_info < (3, 11):
        raise RuntimeError("The Claude forward proxy requires Python 3.11 or later")
    validate_claude_listener_runtime(os.environ)

    async def with_state(scope: Scope, receive: Receive, send: Send) -> None:
        await app({**scope, "state": dict(state)}, receive, send)

    custom_headers: Final[frozenset[bytes]] = (
        frozenset((credential_header.lower().encode("ascii"),))
        if isinstance(credential_header, str) and credential_header.lower() != "authorization"
        else frozenset()
    )
    proxy: Final = ClaudeForwardProxy(with_state, settings, credential_headers=GATEWAY_HEADERS | custom_headers)
    policy: Final = NativeInferencePolicy()
    try:
        listener_tls: Final = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER) if settings.listener_certificate else None
        if listener_tls is not None and settings.listener_certificate is not None:
            listener_tls.minimum_version = ssl.TLSVersion.TLSv1_2
            listener_tls.load_cert_chain(settings.listener_certificate, settings.listener_private_key)
            listener_tls.set_alpn_protocols(["http/1.1"])
        server: Final = await asyncio.start_server(
            proxy.accept,
            settings.host,
            settings.port,
            ssl=listener_tls,
            ssl_handshake_timeout=30 if listener_tls else None,
            reuse_port=sys.platform != "win32",
            start_serving=False,
        )
    except BaseException:
        await proxy.close()
        raise
    try:
        litellm.logging_callback_manager.add_litellm_callback(policy)
        await server.start_serving()
        yield
    finally:
        server.close()
        try:
            await proxy.close(settings.shutdown_timeout_seconds)
            await server.wait_closed()
        finally:
            litellm.logging_callback_manager.remove_callback_from_list_by_object(
                litellm.callbacks, policy, require_self=False
            )
