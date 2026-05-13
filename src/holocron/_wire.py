"""Wire-protocol encoder and decoder.

Byte-for-byte compatible with `github.com/jedi-knights/holocron/proto`
at WireVersion 10. Frame layout:

    [u32 BE payload_length][u8 opcode][payload]

Responses prefix the body with a 1-byte status. On non-OK status the
remainder is a length-prefixed UTF-8 error message.
"""

from __future__ import annotations

from enum import IntEnum
from typing import Protocol, runtime_checkable

from holocron._record import Header, Record
from holocron.errors import (
    ForbiddenError,
    HolocronError,
    InternalError,
    InvalidPartitionError,
    InvalidRequestError,
    NotLeaderError,
    ProtocolError,
    RateLimitedError,
    RebalanceNeededError,
    TopicExistsError,
    UnauthorizedError,
    UnknownMemberError,
    UnknownTopicError,
    VersionMismatchError,
)

WIRE_VERSION = 10
MAX_FRAME_BYTES = 64 * 1024 * 1024  # 64 MiB; matches Go maxFrameBytes


class OpCode(IntEnum):
    """Wire opcodes. Names mirror the Go `proto.OpCode` constants."""

    PRODUCE = 0x01
    FETCH = 0x02
    METADATA = 0x03
    CREATE_TOPIC = 0x04
    COMMIT = 0x05
    HANDSHAKE = 0x06
    JOIN_GROUP = 0x07
    HEARTBEAT = 0x08
    LEAVE_GROUP = 0x09
    SYNC = 0x0A
    PRODUCE_BATCH = 0x0B


class Status(IntEnum):
    """Response status byte. Names mirror the Go `proto.Status` constants."""

    OK = 0x00
    UNKNOWN_TOPIC = 0x10
    INVALID_PARTITION = 0x11
    INVALID_REQUEST = 0x12
    TOPIC_EXISTS = 0x13
    VERSION_MISMATCH = 0x20
    UNKNOWN_MEMBER = 0x30
    REBALANCE_NEEDED = 0x31
    NOT_LEADER = 0x40
    UNAUTHORIZED = 0x50
    FORBIDDEN = 0x51
    RATE_LIMITED = 0x60
    INTERNAL = 0xFF


class CredentialKind(IntEnum):
    """Handshake credential tags. Mirrors Go `proto.CredentialKind`."""

    NONE = 0
    API_KEY = 1
    JWT = 2


class WireFormatError(HolocronError):
    """The bytes received from the broker do not parse as a valid frame."""


@runtime_checkable
class Stream(Protocol):
    """The narrow byte-stream interface wire I/O needs.

    Implemented by `io.BytesIO`, by sockets wrapped via `makefile("rwb")`,
    and by the `Pipe` test helper. Defining it explicitly lets us avoid
    the nominal `typing.BinaryIO` ABC (which carries many methods we
    don't use and would force structural-typing escape hatches in tests).
    """

    def read(self, n: int, /) -> bytes: ...

    def write(self, data: bytes, /) -> int: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


_STATUS_ERRORS: dict[int, type[ProtocolError]] = {
    Status.UNKNOWN_TOPIC: UnknownTopicError,
    Status.INVALID_PARTITION: InvalidPartitionError,
    Status.INVALID_REQUEST: InvalidRequestError,
    Status.TOPIC_EXISTS: TopicExistsError,
    Status.VERSION_MISMATCH: VersionMismatchError,
    Status.UNKNOWN_MEMBER: UnknownMemberError,
    Status.REBALANCE_NEEDED: RebalanceNeededError,
    Status.NOT_LEADER: NotLeaderError,
    Status.UNAUTHORIZED: UnauthorizedError,
    Status.FORBIDDEN: ForbiddenError,
    Status.RATE_LIMITED: RateLimitedError,
    Status.INTERNAL: InternalError,
}


# ----- Primitive encoders -----


def encode_string(s: str) -> bytes:
    """Encode `s` as u32 BE length + UTF-8 bytes."""
    data = s.encode("utf-8")
    return len(data).to_bytes(4, "big") + data


def decode_string(buf: bytes) -> tuple[str, int]:
    """Decode a length-prefixed UTF-8 string. Returns (value, bytes consumed)."""
    if len(buf) < 4:
        raise WireFormatError("short buffer for string length")
    n = int.from_bytes(buf[:4], "big")
    if len(buf) - 4 < n:
        raise WireFormatError(f"string body short: need {n} bytes, have {len(buf) - 4}")
    return buf[4 : 4 + n].decode("utf-8"), 4 + n


def encode_bytes(data: bytes) -> bytes:
    """Encode `data` as u32 BE length + raw bytes."""
    return len(data).to_bytes(4, "big") + data


def decode_bytes(buf: bytes) -> tuple[bytes, int]:
    """Decode a length-prefixed byte string. Returns (value, bytes consumed)."""
    if len(buf) < 4:
        raise WireFormatError("short buffer for bytes length")
    n = int.from_bytes(buf[:4], "big")
    if len(buf) - 4 < n:
        raise WireFormatError(f"bytes body short: need {n} bytes, have {len(buf) - 4}")
    return bytes(buf[4 : 4 + n]), 4 + n


def encode_record(record: Record) -> bytes:
    """Encode a Record using the wire layout shared with the disk log."""
    parts = [
        record.offset.to_bytes(8, "big", signed=True),
        record.timestamp.to_bytes(8, "big", signed=True),
        encode_bytes(record.key),
        encode_bytes(record.value),
        len(record.headers).to_bytes(4, "big"),
    ]
    for header in record.headers:
        parts.append(encode_string(header.key))
        parts.append(encode_bytes(header.value))
    return b"".join(parts)


def decode_record(buf: bytes) -> tuple[Record, int]:
    """Decode a Record. Returns (record, bytes consumed)."""
    if len(buf) < 16:
        raise WireFormatError("short buffer for record header")
    pos = 0
    offset = int.from_bytes(buf[pos : pos + 8], "big", signed=True)
    pos += 8
    timestamp = int.from_bytes(buf[pos : pos + 8], "big", signed=True)
    pos += 8
    key, consumed = decode_bytes(buf[pos:])
    pos += consumed
    value, consumed = decode_bytes(buf[pos:])
    pos += consumed
    if len(buf) - pos < 4:
        raise WireFormatError("short buffer for header count")
    header_count = int.from_bytes(buf[pos : pos + 4], "big")
    pos += 4
    headers: list[Header] = []
    for _ in range(header_count):
        hk, consumed = decode_string(buf[pos:])
        pos += consumed
        hv, consumed = decode_bytes(buf[pos:])
        pos += consumed
        headers.append(Header(key=hk, value=hv))
    return (
        Record(
            offset=offset,
            timestamp=timestamp,
            key=key,
            value=value,
            headers=headers,
        ),
        pos,
    )


# ----- Frame I/O -----


def write_frame(stream: Stream, op: OpCode, payload: bytes) -> None:
    """Write `[u32 BE len][u8 op][payload]` to `stream`."""
    header = (1 + len(payload)).to_bytes(4, "big") + bytes([op])
    stream.write(header)
    if payload:
        stream.write(payload)


def read_frame(stream: Stream) -> tuple[OpCode, bytes]:
    """Read a single wire frame. Returns (opcode, payload)."""
    length_bytes = _read_exact(stream, 4)
    n = int.from_bytes(length_bytes, "big")
    if n == 0:
        raise WireFormatError("empty frame")
    if n > MAX_FRAME_BYTES:
        raise WireFormatError(f"frame too large ({n} bytes)")
    body = _read_exact(stream, n)
    return OpCode(body[0]), bytes(body[1:])


def read_response(stream: Stream, expected_op: OpCode) -> bytes:
    """Read a response frame, validate the opcode, and return the body.

    Raises a `ProtocolError` subclass when the status byte is not OK.
    """
    op, payload = read_frame(stream)
    if op != expected_op:
        raise WireFormatError(f"expected opcode 0x{expected_op:02x}, got 0x{op:02x}")
    if not payload:
        raise WireFormatError("response missing status byte")
    status = payload[0]
    body = payload[1:]
    if status == Status.OK:
        return body
    message = ""
    if body:
        try:
            message, _ = decode_string(body)
        except WireFormatError:
            message = ""
    error_cls = _STATUS_ERRORS.get(status, ProtocolError)
    raise error_cls(status, message)


def _read_exact(stream: Stream, n: int) -> bytes:
    """Read exactly `n` bytes or raise; partial reads from socket-like streams retry."""
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise WireFormatError(f"unexpected EOF: need {n} bytes, got {len(buf)}")
        buf.extend(chunk)
    return bytes(buf)


# ----- Request/response encoders -----


def encode_handshake_request(
    version: int, credential_kind: CredentialKind, credential: bytes
) -> bytes:
    """Encode a HandshakeRequest: `[u8 version][u8 kind][bytes credential]`."""
    return bytes([version, credential_kind]) + encode_bytes(credential)


def decode_handshake_response(body: bytes) -> int:
    """Decode a HandshakeResponse body. Returns the broker's wire version byte."""
    if not body:
        raise WireFormatError("empty handshake response")
    return body[0]


def encode_metadata_request(topic: str) -> bytes:
    """Encode a MetadataRequest body — just the topic name as a length-prefixed string."""
    return encode_string(topic)


def decode_metadata_response(body: bytes) -> int:
    """Decode a MetadataResponse body. Returns the partition count."""
    if len(body) < 4:
        raise WireFormatError("short MetadataResponse")
    return int.from_bytes(body[:4], "big")


def encode_produce_request(topic: str, partition: int, record: Record) -> bytes:
    """Encode a ProduceRequest body: `[topic][u32 partition][record]`."""
    return encode_string(topic) + partition.to_bytes(4, "big", signed=True) + encode_record(record)


def decode_produce_response(body: bytes) -> int:
    """Decode a ProduceResponse body. Returns the broker-assigned offset."""
    if len(body) < 8:
        raise WireFormatError("short ProduceResponse")
    return int.from_bytes(body[:8], "big", signed=True)
