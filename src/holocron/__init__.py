"""Python SDK for the Holocron message broker.

Public API:

    from holocron import Producer, Record, TcpTransport

    with TcpTransport.connect("localhost:9092") as transport, Producer(transport) as producer:
        offset = producer.send("events", Record(key=b"user-42", value=b"hello"))

See https://github.com/jedi-knights/holocron for the broker itself.
"""

from holocron._partitioner import DefaultPartitioner, Partitioner, fnv1a_32
from holocron._record import Header, Record
from holocron._transport import TcpTransport, Transport
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
from holocron.producer import Producer

__version__ = "0.0.1"

__all__ = [
    "DefaultPartitioner",
    "ForbiddenError",
    "Header",
    "HolocronError",
    "InternalError",
    "InvalidPartitionError",
    "InvalidRequestError",
    "NotLeaderError",
    "Partitioner",
    "Producer",
    "ProtocolError",
    "RateLimitedError",
    "RebalanceNeededError",
    "Record",
    "TcpTransport",
    "TopicExistsError",
    "Transport",
    "UnauthorizedError",
    "UnknownMemberError",
    "UnknownTopicError",
    "VersionMismatchError",
    "__version__",
    "fnv1a_32",
]
