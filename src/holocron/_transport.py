"""Transport — the connection between SDK and broker.

`Transport` is the Strategy interface the `Producer` consumes;
`TcpTransport` is the v1 implementation that speaks the wire protocol
over a TCP socket.
"""

from __future__ import annotations

import socket
import threading
from collections.abc import Callable
from types import TracebackType
from typing import Protocol, Self, cast, runtime_checkable

from holocron import _wire
from holocron._record import Record
from holocron.errors import HolocronError, VersionMismatchError


@runtime_checkable
class Transport(Protocol):
    """Strategy interface the Producer calls into.

    Implementations must be safe for concurrent use — one Producer
    instance is typically shared across application threads.
    """

    def publish(self, topic: str, partition: int, record: Record) -> int:
        """Append `record` to the partition, return the broker-assigned offset."""
        ...

    def partitions_for(self, topic: str) -> int:
        """Return the current partition count for `topic`."""
        ...

    def close(self) -> None:
        """Release any underlying resources. Idempotent."""
        ...


class TcpTransport:
    """Speaks the binary wire protocol over a TCP socket.

    Construct via `TcpTransport.connect("host:port")` for normal use,
    or `TcpTransport.from_stream(stream)` when injecting a fake stream
    in tests.
    """

    def __init__(
        self,
        stream: _wire.Stream,
        *,
        close_callback: Callable[[], None] | None = None,
    ) -> None:
        self._stream = stream
        self._close_callback = close_callback or stream.close
        self._lock = threading.Lock()
        self._closed = False

    @classmethod
    def from_stream(
        cls,
        stream: _wire.Stream,
        *,
        credential_kind: _wire.CredentialKind = _wire.CredentialKind.NONE,
        credential: bytes = b"",
        close_callback: Callable[[], None] | None = None,
    ) -> Self:
        """Wrap an already-open stream and perform the handshake."""
        transport = cls(stream, close_callback=close_callback)
        transport._handshake(credential_kind, credential)
        return transport

    @classmethod
    def connect(
        cls,
        address: str,
        *,
        timeout: float = 5.0,
        credential_kind: _wire.CredentialKind = _wire.CredentialKind.NONE,
        credential: bytes = b"",
    ) -> Self:
        """Open a TCP connection to `address` (`host:port`) and handshake."""
        host, port_str = _split_address(address)
        sock = socket.create_connection((host, int(port_str)), timeout=timeout)
        stream = cast(_wire.Stream, sock.makefile("rwb"))

        def _close() -> None:
            try:
                stream.close()
            finally:
                sock.close()

        return cls.from_stream(
            stream,
            credential_kind=credential_kind,
            credential=credential,
            close_callback=_close,
        )

    def publish(self, topic: str, partition: int, record: Record) -> int:
        body = self._call(
            _wire.OpCode.PRODUCE,
            _wire.encode_produce_request(topic, partition, record),
        )
        return _wire.decode_produce_response(body)

    def partitions_for(self, topic: str) -> int:
        body = self._call(
            _wire.OpCode.METADATA,
            _wire.encode_metadata_request(topic),
        )
        return _wire.decode_metadata_response(body)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._close_callback()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _handshake(self, kind: _wire.CredentialKind, credential: bytes) -> None:
        payload = _wire.encode_handshake_request(_wire.WIRE_VERSION, kind, credential)
        body = self._call(_wire.OpCode.HANDSHAKE, payload, skip_closed_check=True)
        version = _wire.decode_handshake_response(body)
        if version != _wire.WIRE_VERSION:
            raise VersionMismatchError(
                _wire.Status.VERSION_MISMATCH,
                f"broker speaks wire version {version}, SDK speaks {_wire.WIRE_VERSION}",
            )

    def _call(
        self,
        op: _wire.OpCode,
        payload: bytes,
        *,
        skip_closed_check: bool = False,
    ) -> bytes:
        with self._lock:
            if self._closed and not skip_closed_check:
                raise HolocronError("transport is closed")
            _wire.write_frame(self._stream, op, payload)
            self._stream.flush()
            return _wire.read_response(self._stream, op)


def _split_address(address: str) -> tuple[str, str]:
    if ":" not in address:
        raise ValueError(f"expected host:port, got {address!r}")
    host, _, port = address.rpartition(":")
    if not host or not port:
        raise ValueError(f"expected host:port, got {address!r}")
    return host, port
