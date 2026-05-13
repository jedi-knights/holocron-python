"""Partition routing — picks a partition index for a record.

`DefaultPartitioner` mirrors the Go SDK: FNV-1a 32-bit of the key for
keyed records, atomic round-robin counter for keyless records. The
hash choice must match Go (`hash/fnv.New32a`) — partition routing is
the broker's per-partition ordering guarantee, and a mismatch between
clients would scatter same-key records across partitions.
"""

from __future__ import annotations

import threading
from typing import Protocol, runtime_checkable

from holocron._record import Record

_FNV1A_32_OFFSET_BASIS = 0x811C9DC5
_FNV1A_32_PRIME = 0x01000193
_U32_MASK = 0xFFFFFFFF


def fnv1a_32(data: bytes) -> int:
    """FNV-1a 32-bit hash. Output matches Go's `hash/fnv.New32a`."""
    h = _FNV1A_32_OFFSET_BASIS
    for byte in data:
        h = ((h ^ byte) * _FNV1A_32_PRIME) & _U32_MASK
    return h


@runtime_checkable
class Partitioner(Protocol):
    """Strategy for routing a record to a partition.

    Implementations must be safe for concurrent use — a single
    Producer instance is shared across threads in typical apps.
    """

    def partition(self, record: Record, num_partitions: int) -> int:
        """Return the partition index in `[0, num_partitions)`."""
        ...


class DefaultPartitioner:
    """Hash the key with FNV-1a 32-bit, round-robin keyless records.

    Routing rules:
      * Non-empty key → `fnv1a_32(key) % num_partitions`
      * Empty key → next slot from an atomic round-robin counter

    Round-robin state is per-instance, so two producers in the same
    process do not coordinate their keyless distribution — matching
    the Go SDK's behavior.
    """

    def __init__(self) -> None:
        self._counter = 0
        self._lock = threading.Lock()

    def partition(self, record: Record, num_partitions: int) -> int:
        if num_partitions <= 0:
            return 0
        if record.key:
            return fnv1a_32(record.key) % num_partitions
        with self._lock:
            slot = self._counter % num_partitions
            self._counter += 1
        return slot
