"""Shared pytest helpers.

`Pipe` simulates the broker side of a TCP connection: bytes preloaded
into `inbound` are returned by reads, bytes the client writes land in
`outbound`. Lets transport tests assert on the exact bytes the SDK put
on the wire and on what it does with canned responses.
"""

from __future__ import annotations

import io


class Pipe:
    """A duplex byte buffer: tests preload broker responses, inspect client writes."""

    def __init__(self, inbound: bytes = b"") -> None:
        self._inbound = io.BytesIO(inbound)
        self.outbound = io.BytesIO()
        self.closed = False

    def read(self, n: int) -> bytes:
        return self._inbound.read(n)

    def write(self, data: bytes) -> int:
        return self.outbound.write(data)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True
