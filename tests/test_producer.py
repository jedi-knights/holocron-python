"""Producer tests — verify routing through Partitioner and Transport.

A fake Transport records calls so we can assert what the Producer
does without involving the wire layer (that's exercised by
test_transport.py).
"""

from __future__ import annotations

import pytest

from holocron._record import Record
from holocron._transport import Transport
from holocron.errors import HolocronError
from holocron.producer import DefaultPartitioner, Producer


class FakeTransport:
    """Records the Producer's calls, returns canned offsets and partition counts."""

    def __init__(self, *, partition_count: int = 4, offset: int = 7) -> None:
        self.partition_count = partition_count
        self.offset = offset
        self.publish_calls: list[tuple[str, int, Record]] = []
        self.partitions_for_calls: list[str] = []
        self.closed = False

    def publish(self, topic: str, partition: int, record: Record) -> int:
        self.publish_calls.append((topic, partition, record))
        return self.offset

    def partitions_for(self, topic: str) -> int:
        self.partitions_for_calls.append(topic)
        return self.partition_count

    def close(self) -> None:
        self.closed = True


class StickyPartitioner:
    """Always returns the same partition index. Lets us assert routing was consulted."""

    def __init__(self, index: int) -> None:
        self.index = index
        self.calls = 0

    def partition(self, record: Record, num_partitions: int) -> int:
        del record, num_partitions
        self.calls += 1
        return self.index


def test_fake_transport_implements_protocol() -> None:
    assert isinstance(FakeTransport(), Transport)


class TestSend:
    def test_returns_offset_from_transport(self) -> None:
        transport = FakeTransport(offset=42)
        producer = Producer(transport)
        assert producer.send("events", Record(key=b"k", value=b"v")) == 42

    def test_calls_partitions_for_to_resolve_partition_count(self) -> None:
        transport = FakeTransport(partition_count=8)
        Producer(transport).send("events", Record(key=b"k"))
        assert transport.partitions_for_calls == ["events"]

    def test_routes_via_partitioner(self) -> None:
        transport = FakeTransport(partition_count=8)
        partitioner = StickyPartitioner(index=5)
        Producer(transport, partitioner=partitioner).send("events", Record(key=b"k"))
        assert partitioner.calls == 1
        assert transport.publish_calls[0][1] == 5

    def test_default_partitioner_is_fnv1a_keyed(self) -> None:
        transport = FakeTransport(partition_count=10)
        Producer(transport).send("events", Record(key=b"foobar"))
        # fnv1a_32(b"foobar") = 0xbf9cf968; 0xbf9cf968 % 10 == 0
        assert transport.publish_calls[0][1] == 0xBF9CF968 % 10

    def test_default_partitioner_round_robins_keyless(self) -> None:
        transport = FakeTransport(partition_count=3)
        producer = Producer(transport)
        for _ in range(6):
            producer.send("events", Record(value=b"v"))
        partitions = [call[1] for call in transport.publish_calls]
        assert partitions == [0, 1, 2, 0, 1, 2]

    def test_passes_record_through_unchanged(self) -> None:
        transport = FakeTransport()
        record = Record(key=b"k", value=b"v")
        Producer(transport).send("events", record)
        assert transport.publish_calls[0][2] is record


class TestLifecycle:
    def test_close_closes_transport(self) -> None:
        transport = FakeTransport()
        producer = Producer(transport)
        producer.close()
        assert transport.closed

    def test_close_is_idempotent(self) -> None:
        transport = FakeTransport()
        producer = Producer(transport)
        producer.close()
        producer.close()  # second close must not raise

    def test_context_manager_closes_on_exit(self) -> None:
        transport = FakeTransport()
        with Producer(transport):
            pass
        assert transport.closed

    def test_send_after_close_raises(self) -> None:
        transport = FakeTransport()
        producer = Producer(transport)
        producer.close()
        with pytest.raises(HolocronError):
            producer.send("events", Record(value=b"v"))


class TestDefaultPartitionerExport:
    def test_default_partitioner_is_re_exported_from_producer_module(self) -> None:
        # Sanity: users should be able to import DefaultPartitioner from the
        # same module as Producer without reaching for private internals.
        assert DefaultPartitioner is not None
