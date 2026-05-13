"""Tests for FNV-1a hash + DefaultPartitioner.

FNV-1a 32-bit output must match `hash/fnv.New32a` in Go; the broker's
ordering guarantee depends on every SDK landing the same key on the
same partition.
"""

import pytest

from holocron._partitioner import DefaultPartitioner, fnv1a_32
from holocron._record import Record


class TestFnv1a32:
    @pytest.mark.parametrize(
        ("data", "expected"),
        [
            (b"", 0x811C9DC5),
            (b"a", 0xE40C292C),
            (b"foobar", 0xBF9CF968),
        ],
    )
    def test_known_vectors(self, data: bytes, expected: int) -> None:
        assert fnv1a_32(data) == expected


class TestDefaultPartitioner:
    def test_keyed_record_uses_fnv_mod_partitions(self) -> None:
        partitioner = DefaultPartitioner()
        record = Record(key=b"foobar", value=b"")
        assert partitioner.partition(record, num_partitions=10) == 0xBF9CF968 % 10

    def test_same_key_always_lands_on_same_partition(self) -> None:
        partitioner = DefaultPartitioner()
        record = Record(key=b"user-42")
        first = partitioner.partition(record, num_partitions=8)
        for _ in range(50):
            assert partitioner.partition(record, num_partitions=8) == first

    def test_keyless_records_round_robin(self) -> None:
        partitioner = DefaultPartitioner()
        keyless = Record()
        seen = [partitioner.partition(keyless, num_partitions=4) for _ in range(8)]
        assert seen == [0, 1, 2, 3, 0, 1, 2, 3]

    def test_zero_partitions_returns_zero(self) -> None:
        assert DefaultPartitioner().partition(Record(), num_partitions=0) == 0

    def test_negative_partitions_returns_zero(self) -> None:
        assert DefaultPartitioner().partition(Record(), num_partitions=-1) == 0
