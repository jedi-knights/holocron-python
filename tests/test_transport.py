"""Tests for TcpTransport — the wire-protocol-aware adapter."""

from __future__ import annotations

import io

import pytest

from holocron import _wire
from holocron._record import Record
from holocron._transport import TcpTransport
from holocron.errors import HolocronError, UnknownTopicError, VersionMismatchError
from tests.conftest import Pipe


def _ok_response(op: _wire.OpCode, body: bytes) -> bytes:
    """Build a complete `[len][op][status=OK][body]` frame as the broker would."""
    buf = io.BytesIO()
    _wire.write_frame(buf, op, bytes([_wire.Status.OK]) + body)
    return buf.getvalue()


def _error_response(op: _wire.OpCode, status: int, message: str) -> bytes:
    """Build a complete error response frame."""
    buf = io.BytesIO()
    _wire.write_frame(buf, op, bytes([status]) + _wire.encode_string(message))
    return buf.getvalue()


def _connect(canned: bytes) -> tuple[TcpTransport, Pipe]:
    """Build a TcpTransport wired to a Pipe that has already swallowed the handshake."""
    handshake_response = _ok_response(_wire.OpCode.HANDSHAKE, bytes([_wire.WIRE_VERSION]))
    pipe = Pipe(inbound=handshake_response + canned)
    transport = TcpTransport.from_stream(pipe)
    return transport, pipe


class TestHandshake:
    def test_handshake_sends_canonical_anonymous_request(self) -> None:
        _, pipe = _connect(b"")
        pipe.outbound.seek(0)
        op, payload = _wire.read_frame(pipe.outbound)
        assert op == _wire.OpCode.HANDSHAKE
        assert payload == _wire.encode_handshake_request(
            _wire.WIRE_VERSION, _wire.CredentialKind.NONE, b""
        )

    def test_handshake_raises_on_version_mismatch(self) -> None:
        bad_version = bytes([_wire.WIRE_VERSION + 1])
        response = _ok_response(_wire.OpCode.HANDSHAKE, bad_version)
        pipe = Pipe(inbound=response)
        with pytest.raises(VersionMismatchError):
            TcpTransport.from_stream(pipe)

    def test_handshake_propagates_broker_error_status(self) -> None:
        response = _error_response(_wire.OpCode.HANDSHAKE, _wire.Status.UNAUTHORIZED, "no creds")
        pipe = Pipe(inbound=response)
        with pytest.raises(HolocronError):
            TcpTransport.from_stream(pipe)


class TestPublish:
    def test_publish_writes_produce_request_and_returns_offset(self) -> None:
        offset_body = (123).to_bytes(8, "big")
        transport, pipe = _connect(_ok_response(_wire.OpCode.PRODUCE, offset_body))
        record = Record(key=b"k", value=b"v")
        try:
            assert transport.publish("events", 2, record) == 123
        finally:
            transport.close()

        # Skip the handshake frame the transport already sent.
        pipe.outbound.seek(0)
        _wire.read_frame(pipe.outbound)
        op, payload = _wire.read_frame(pipe.outbound)
        assert op == _wire.OpCode.PRODUCE
        assert payload == _wire.encode_produce_request("events", 2, record)

    def test_publish_raises_typed_error_on_unknown_topic(self) -> None:
        transport, _ = _connect(
            _error_response(_wire.OpCode.PRODUCE, _wire.Status.UNKNOWN_TOPIC, "no topic")
        )
        with pytest.raises(UnknownTopicError):
            transport.publish("missing", 0, Record(value=b"v"))


class TestPartitionsFor:
    def test_partitions_for_returns_count_from_metadata_response(self) -> None:
        count_body = (8).to_bytes(4, "big")
        transport, pipe = _connect(_ok_response(_wire.OpCode.METADATA, count_body))
        assert transport.partitions_for("events") == 8

        pipe.outbound.seek(0)
        _wire.read_frame(pipe.outbound)
        op, payload = _wire.read_frame(pipe.outbound)
        assert op == _wire.OpCode.METADATA
        assert payload == _wire.encode_metadata_request("events")

    def test_partitions_for_unknown_topic_raises(self) -> None:
        transport, _ = _connect(
            _error_response(_wire.OpCode.METADATA, _wire.Status.UNKNOWN_TOPIC, "no topic")
        )
        with pytest.raises(UnknownTopicError):
            transport.partitions_for("missing")


class TestLifecycle:
    def test_close_is_idempotent(self) -> None:
        transport, pipe = _connect(b"")
        transport.close()
        transport.close()
        assert pipe.closed

    def test_context_manager_closes_on_exit(self) -> None:
        handshake = _ok_response(_wire.OpCode.HANDSHAKE, bytes([_wire.WIRE_VERSION]))
        pipe = Pipe(inbound=handshake)
        with TcpTransport.from_stream(pipe):
            pass
        assert pipe.closed

    def test_publish_after_close_raises(self) -> None:
        transport, _ = _connect(b"")
        transport.close()
        with pytest.raises(HolocronError):
            transport.publish("events", 0, Record(value=b"v"))
