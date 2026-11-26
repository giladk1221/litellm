import asyncio
import base64
import binascii
from collections.abc import Iterable
from io import BytesIO
from typing import Final

import h11
from anyio import fail_after

HEAD_LIMIT: Final = 65536
HOP_HEADERS: Final = frozenset(
    (
        b"connection",
        b"proxy-connection",
        b"proxy-authorization",
        b"keep-alive",
        b"transfer-encoding",
        b"te",
        b"trailer",
        b"upgrade",
    )
)


def gateway_key(headers: Iterable[tuple[bytes, bytes]]) -> str | None:
    values: Final = tuple(value for name, value in headers if name.lower() == b"proxy-authorization")
    if len(values) != 1:
        return None
    scheme, separator, encoded = values[0].partition(b" ")
    if not separator or scheme.lower() != b"basic":
        return None
    try:
        username, colon, password = base64.b64decode(encoded, validate=True).decode("ascii").partition(":")
    except (ValueError, UnicodeDecodeError, binascii.Error):
        return None
    if username != "litellm" or not colon or not password:
        return None
    if any(ord(char) < 33 for char in password):
        return None
    return password


def clean_headers(headers: Iterable[tuple[bytes, bytes]]) -> tuple[tuple[bytes, bytes], ...]:
    source: Final = tuple(headers)
    connection_tokens: Final = frozenset(
        token.strip().lower()
        for token in b",".join(value for name, value in source if name == b"connection").split(b",")
    )
    return tuple(
        (name, value)
        for name, value in source
        if name not in HOP_HEADERS | connection_tokens and not name.startswith(b"x-litellm-")
    )


def native_response_headers(headers: Iterable[tuple[bytes, bytes]]) -> tuple[tuple[bytes, bytes], ...]:
    source: Final = clean_headers(headers)
    names: Final = frozenset(name for name, _ in source)
    restored: Final = tuple(
        (name.removeprefix(b"llm_provider-"), value)
        for name, value in source
        if name.startswith(b"llm_provider-")
        and (
            name.startswith(b"llm_provider-anthropic-ratelimit-")
            or name in (b"llm_provider-retry-after", b"llm_provider-x-should-retry", b"llm_provider-request-id")
        )
        and name.removeprefix(b"llm_provider-") not in names
    )
    return (*source, *restored)


class HTTPConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, timeout: float) -> None:
        self.reader = reader
        self.writer = writer
        self.timeout = timeout
        self.state = h11.Connection(h11.SERVER, max_incomplete_event_size=HEAD_LIMIT)

    async def event(self) -> h11.Event:
        while True:
            event = self.state.next_event()
            if event is h11.NEED_DATA:
                data = await asyncio.wait_for(self.reader.read(65536), timeout=self.timeout)
                self.state.receive_data(data)
                continue
            if isinstance(event, h11.Event):
                return event
            raise h11.RemoteProtocolError("Unexpected HTTP protocol transition")

    async def send(self, event: h11.Event) -> None:
        data: Final = self.state.send(event)
        if data is not None:
            self.writer.write(data)
            with fail_after(self.timeout):
                await self.writer.drain()

    async def body(self, limit: int) -> bytes | None:
        with BytesIO() as buffer:
            while True:
                event = await self.event()
                if isinstance(event, h11.EndOfMessage):
                    return buffer.getvalue()
                if not isinstance(event, h11.Data):
                    raise h11.RemoteProtocolError("Expected request body")
                if buffer.tell() + len(event.data) > limit:
                    return None
                buffer.write(event.data)

    async def reject(self, status: int, message: bytes) -> None:
        headers: Final = (
            (b"content-type", b"text/plain"),
            (b"content-length", str(len(message)).encode()),
            (b"connection", b"close"),
            *((((b"proxy-authenticate", b'Basic realm="LiteLLM"'),)) if status == 407 else ()),
        )
        await self.send(h11.Response(status_code=status, headers=list(headers)))
        await self.send(h11.Data(data=message))
        await self.send(h11.EndOfMessage())
