"""Byte-level tests for the wire encoder/decoder.

Canonical bytes here mirror the Go encoder in
github.com/jedi-knights/holocron/proto at WireVersion 10. If a test
fails after the broker bumps WireVersion, regenerate fixtures rather
than relax the assertions.
"""

import io

import pytest

from holocron import _wire
from holocron._record import Header, Record
from holocron.errors import ProtocolError, UnknownTopicError


class TestPrimitives:
    def test_encode_string_writes_u32_length_prefix_then_utf8(self) -> None:
        assert _wire.encode_string("events") == bytes.fromhex("00000006") + b"events"

    def test_encode_empty_string_is_zero_length(self) -> None:
        assert _wire.encode_string("") == bytes.fromhex("00000000")

    def test_decode_string_returns_value_and_bytes_consumed(self) -> None:
        buf = bytes.fromhex("00000006") + b"events" + b"trailing"
        value, consumed = _wire.decode_string(buf)
        assert value == "events"
        assert consumed == 10

    def test_decode_string_raises_on_short_buffer(self) -> None:
        with pytest.raises(_wire.WireFormatError):
            _wire.decode_string(b"\x00\x00")

    def test_encode_bytes_writes_u32_length_then_payload(self) -> None:
        assert _wire.encode_bytes(b"hello") == bytes.fromhex("00000005") + b"hello"

    def test_encode_empty_bytes_is_zero_length(self) -> None:
        assert _wire.encode_bytes(b"") == bytes.fromhex("00000000")

    def test_decode_bytes_zero_length_returns_empty(self) -> None:
        value, consumed = _wire.decode_bytes(bytes.fromhex("00000000") + b"trailing")
        assert value == b""
        assert consumed == 4


class TestRecord:
    def test_encode_record_no_headers_matches_go_layout(self) -> None:
        # offset=0, ts=0, key=b"k", value=b"v", no headers
        expected = (
            bytes.fromhex("0000000000000000")  # offset
            + bytes.fromhex("0000000000000000")  # timestamp
            + bytes.fromhex("00000001")
            + b"k"  # key
            + bytes.fromhex("00000001")
            + b"v"  # value
            + bytes.fromhex("00000000")  # header count
        )
        record = Record(key=b"k", value=b"v")
        assert _wire.encode_record(record) == expected
        assert len(expected) == 30

    def test_encode_record_with_one_header(self) -> None:
        record = Record(key=b"k", value=b"v", headers=[Header(key="ce_id", value=b"42")])
        encoded = _wire.encode_record(record)
        # Tail after offset(8)+ts(8)+key(5)+value(5) is headers:
        tail = encoded[8 + 8 + 5 + 5 :]
        assert tail == (
            bytes.fromhex("00000001")  # count
            + bytes.fromhex("00000005")
            + b"ce_id"  # header key
            + bytes.fromhex("00000002")
            + b"42"  # header value
        )

    def test_decode_record_round_trips_with_headers(self) -> None:
        record = Record(
            offset=7,
            timestamp=123_456,
            key=b"user-42",
            value=b'{"a":1}',
            headers=[Header(key="trace", value=b"abc")],
        )
        decoded, consumed = _wire.decode_record(_wire.encode_record(record))
        assert decoded == record
        assert consumed == len(_wire.encode_record(record))


class TestFraming:
    def test_write_frame_emits_len_op_payload(self) -> None:
        out = io.BytesIO()
        _wire.write_frame(out, _wire.OpCode.METADATA, b"\xaa\xbb")
        # length = 1 (op) + 2 (payload) = 3
        assert out.getvalue() == bytes.fromhex("00000003") + bytes.fromhex("03") + b"\xaa\xbb"

    def test_read_frame_round_trips(self) -> None:
        out = io.BytesIO()
        _wire.write_frame(out, _wire.OpCode.PRODUCE, b"payload")
        out.seek(0)
        op, payload = _wire.read_frame(out)
        assert op == _wire.OpCode.PRODUCE
        assert payload == b"payload"

    def test_read_frame_rejects_oversized_length(self) -> None:
        too_big = (_wire.MAX_FRAME_BYTES + 1).to_bytes(4, "big")
        with pytest.raises(_wire.WireFormatError):
            _wire.read_frame(io.BytesIO(too_big))

    def test_read_frame_rejects_zero_length(self) -> None:
        with pytest.raises(_wire.WireFormatError):
            _wire.read_frame(io.BytesIO(bytes.fromhex("00000000")))


class TestHandshake:
    def test_encode_anonymous_handshake_request_matches_canonical(self) -> None:
        encoded = _wire.encode_handshake_request(
            version=_wire.WIRE_VERSION,
            credential_kind=_wire.CredentialKind.NONE,
            credential=b"",
        )
        # version(1) + kind(1) + bytes-len(4) + bytes(0)
        assert encoded == bytes.fromhex("0a") + bytes.fromhex("00") + bytes.fromhex("00000000")

    def test_decode_handshake_response_returns_version_byte(self) -> None:
        assert _wire.decode_handshake_response(bytes([_wire.WIRE_VERSION])) == _wire.WIRE_VERSION

    def test_decode_handshake_response_raises_on_empty(self) -> None:
        with pytest.raises(_wire.WireFormatError):
            _wire.decode_handshake_response(b"")


class TestMetadata:
    def test_encode_metadata_request_is_topic_string(self) -> None:
        assert _wire.encode_metadata_request("events") == bytes.fromhex("00000006") + b"events"

    def test_decode_metadata_response_returns_partition_count(self) -> None:
        assert _wire.decode_metadata_response(bytes.fromhex("00000004")) == 4

    def test_decode_metadata_response_raises_on_short_buffer(self) -> None:
        with pytest.raises(_wire.WireFormatError):
            _wire.decode_metadata_response(b"\x00\x00")


class TestProduce:
    def test_encode_produce_request_concatenates_topic_partition_record(self) -> None:
        record = Record(key=b"k", value=b"v")
        encoded = _wire.encode_produce_request(topic="t", partition=2, record=record)
        assert encoded == (
            bytes.fromhex("00000001")
            + b"t"  # topic
            + bytes.fromhex("00000002")  # partition
            + _wire.encode_record(record)
        )

    def test_decode_produce_response_returns_offset(self) -> None:
        body = (42).to_bytes(8, "big")
        assert _wire.decode_produce_response(body) == 42

    def test_decode_produce_response_raises_on_short_buffer(self) -> None:
        with pytest.raises(_wire.WireFormatError):
            _wire.decode_produce_response(b"\x00")


class TestResponseReading:
    def test_read_response_returns_body_for_ok_status(self) -> None:
        # Build an OK Produce response frame: [len][op][status][body]
        body = (99).to_bytes(8, "big")
        payload = bytes([_wire.Status.OK]) + body
        out = io.BytesIO()
        _wire.write_frame(out, _wire.OpCode.PRODUCE, payload)
        out.seek(0)
        got = _wire.read_response(out, _wire.OpCode.PRODUCE)
        assert got == body

    def test_read_response_raises_typed_error_for_known_status(self) -> None:
        msg_bytes = _wire.encode_string("unknown topic")
        payload = bytes([_wire.Status.UNKNOWN_TOPIC]) + msg_bytes
        out = io.BytesIO()
        _wire.write_frame(out, _wire.OpCode.METADATA, payload)
        out.seek(0)
        with pytest.raises(UnknownTopicError) as exc_info:
            _wire.read_response(out, _wire.OpCode.METADATA)
        assert exc_info.value.status == _wire.Status.UNKNOWN_TOPIC
        assert exc_info.value.message == "unknown topic"

    def test_read_response_raises_generic_protocol_error_for_unknown_status(self) -> None:
        # Use a status byte that has no specific subclass mapping.
        unknown_status = 0x7F
        payload = bytes([unknown_status]) + _wire.encode_string("boom")
        out = io.BytesIO()
        _wire.write_frame(out, _wire.OpCode.PRODUCE, payload)
        out.seek(0)
        with pytest.raises(ProtocolError) as exc_info:
            _wire.read_response(out, _wire.OpCode.PRODUCE)
        assert exc_info.value.status == unknown_status
        assert exc_info.value.message == "boom"

    def test_read_response_raises_on_opcode_mismatch(self) -> None:
        out = io.BytesIO()
        _wire.write_frame(out, _wire.OpCode.METADATA, bytes([_wire.Status.OK]))
        out.seek(0)
        with pytest.raises(_wire.WireFormatError):
            _wire.read_response(out, _wire.OpCode.PRODUCE)
