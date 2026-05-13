"""Record and Header — the atomic units exchanged with the broker."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Header:
    """Application-level metadata attached to a Record.

    Keys are UTF-8 strings, values are arbitrary bytes. The broker does
    not interpret either; headers travel verbatim alongside the record.
    """

    key: str
    value: bytes


@dataclass(slots=True)
class Record:
    """Atomic unit appended to a topic partition.

    Offset and timestamp are broker-assigned at append time — producers
    leave them zero. Key and value are opaque bytes; the broker does
    not interpret them. Headers carry per-record application metadata.
    """

    key: bytes = b""
    value: bytes = b""
    offset: int = 0
    timestamp: int = 0
    headers: list[Header] = field(default_factory=list)
