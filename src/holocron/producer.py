"""Producer — high-level record-publishing API.

Re-exports `Partitioner` and `DefaultPartitioner` for convenience so
callers don't need to know that they live in a private module.
"""

from __future__ import annotations

import threading
from types import TracebackType
from typing import Self

from holocron._partitioner import DefaultPartitioner, Partitioner
from holocron._record import Record
from holocron._transport import Transport
from holocron.errors import HolocronError

__all__ = ["DefaultPartitioner", "Partitioner", "Producer"]


class Producer:
    """Publishes records to topics over a `Transport`.

    Holds a `Partitioner` (default: `DefaultPartitioner`) that picks
    the partition for each record. Each `send` call looks up the
    topic's partition count via `transport.partitions_for`, picks a
    partition via the partitioner, and publishes through the
    transport. Returns the broker-assigned offset.

    Producers are safe for concurrent use across threads.
    """

    def __init__(
        self,
        transport: Transport,
        *,
        partitioner: Partitioner | None = None,
    ) -> None:
        self._transport = transport
        self._partitioner: Partitioner = partitioner or DefaultPartitioner()
        self._lock = threading.Lock()
        self._closed = False

    def send(self, topic: str, record: Record) -> int:
        """Publish `record` to `topic` and return the broker-assigned offset."""
        with self._lock:
            if self._closed:
                raise HolocronError("producer is closed")
        partition_count = self._transport.partitions_for(topic)
        partition = self._partitioner.partition(record, partition_count)
        return self._transport.publish(topic, partition, record)

    def close(self) -> None:
        """Close the underlying transport. Idempotent."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._transport.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
