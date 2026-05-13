"""Exception hierarchy for the Holocron SDK.

`HolocronError` is the root. `ProtocolError` is raised when the broker
returns a non-OK status; specific subclasses correspond to specific
status codes from the wire protocol.
"""

from __future__ import annotations


class HolocronError(Exception):
    """Base class for every error raised by the SDK."""


class ProtocolError(HolocronError):
    """The broker returned a non-OK status byte.

    Subclasses correspond to specific status codes; unknown statuses
    raise this base class directly. Always carries the numeric status
    and the broker's human-readable message.
    """

    def __init__(self, status: int, message: str) -> None:
        text = (
            f"broker error 0x{status:02x}: {message}"
            if message
            else f"broker error: status 0x{status:02x}"
        )
        super().__init__(text)
        self.status = status
        self.message = message


class UnknownTopicError(ProtocolError):
    """Topic does not exist on the broker."""


class InvalidPartitionError(ProtocolError):
    """Partition index is out of range for the topic."""


class InvalidRequestError(ProtocolError):
    """Request payload was malformed."""


class TopicExistsError(ProtocolError):
    """Create-topic conflicted with an existing topic."""


class VersionMismatchError(ProtocolError):
    """Client and broker disagree on wire-protocol version."""


class UnknownMemberError(ProtocolError):
    """Consumer-group member is unknown to the broker."""


class RebalanceNeededError(ProtocolError):
    """Consumer group needs to rejoin and re-receive assignments."""


class NotLeaderError(ProtocolError):
    """Request hit a follower; the operation must be retried at the leader."""


class UnauthorizedError(ProtocolError):
    """Credential missing or invalid."""


class ForbiddenError(ProtocolError):
    """Credential valid but the caller is not authorised for this operation."""


class RateLimitedError(ProtocolError):
    """Broker is rate-limiting this caller; retry with backoff."""


class InternalError(ProtocolError):
    """Broker hit an internal error; the message may have detail."""
