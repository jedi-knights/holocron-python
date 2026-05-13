# holocron-python

Python SDK for the [Holocron](https://github.com/jedi-knights/holocron) message broker.

![CI](https://github.com/jedi-knights/holocron-python/actions/workflows/ci.yml/badge.svg)
![Maturity](https://img.shields.io/badge/maturity-pre--alpha-orange)
![Python](https://img.shields.io/badge/python-3.11+-3776AB?logo=python&logoColor=white)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Status:** Pre-alpha. The synchronous producer works against a running [Holocron broker](https://github.com/jedi-knights/holocron) at wire-protocol v10. Consumer support, batching, idempotency, compression, and TLS are not yet implemented — see the Features list below.

## Table of contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Overview

Holocron is a Go-native distributed log broker — append-only topics, per-partition strict ordering, replayable consumers, Raft replication. The Go SDK ships in the broker repository.

This repository is the **Python client**. Once the wire protocol stabilises, this package will let Python applications produce records to topics, consume from them (individually or as part of a consumer group), and manage offsets — the same shape as the Go SDK in [`jedi-knights/holocron`](https://github.com/jedi-knights/holocron).

For the broker itself, see the main repository. For an introduction to brokers, topics, and consumer groups, see [What is event-driven architecture?](https://github.com/jedi-knights/holocron/blob/main/docs/eda.md) in the broker repo.

## Features

What's shipped today:

- Synchronous `Producer.send` over TCP — one record per call, returns the broker-assigned offset
- `DefaultPartitioner` — FNV-1a 32-bit of the key (Go-compatible), atomic round-robin for keyless records
- Pluggable `Partitioner` and `Transport` protocols
- Typed exception hierarchy mapped from broker status codes
- Typed package (`py.typed` marker), strict mypy clean
- CI on Python 3.11, 3.12, 3.13

What's planned (tracking the broker capability matrix):

- Batched producer (`PublishBatch` + linger window)
- Idempotent producer (`HeaderProducerID` / `HeaderProducerSeq` stamping)
- LZ4 compression
- Rate-limit and `NotLeader` retry handling
- Consumer with per-partition offset tracking and rewind
- Consumer groups with range-assignment rebalancing
- TLS and mTLS support
- Schema registry client
- `asyncio` API alongside the synchronous one

## Requirements

- Python **3.11 or newer**
- A running [Holocron broker](https://github.com/jedi-knights/holocron) once client functionality lands

## Installation

From PyPI (once the first functional release is cut):

```bash
pip install holocron
```

With `uv`:

```bash
uv add holocron
```

From source:

```bash
git clone https://github.com/jedi-knights/holocron-python.git
cd holocron-python
uv sync
```

## Usage

Produce one record to a running broker at `localhost:9092`:

```python
from holocron import Producer, Record, TcpTransport

with TcpTransport.connect("localhost:9092") as transport, Producer(transport) as producer:
    offset = producer.send("events", Record(key=b"user-42", value=b'{"action":"login"}'))
    print(f"appended at offset {offset}")
```

The broker assigns the offset on append. Records with the same key always land on the same partition (FNV-1a 32-bit hash of the key, matching the Go SDK), so per-key ordering is preserved.

Swap in a custom partitioner by passing any object that satisfies the `Partitioner` protocol:

```python
from holocron import Partitioner, Producer, Record, TcpTransport

class FirstPartitionOnly:
    def partition(self, record: Record, num_partitions: int) -> int:
        return 0

with TcpTransport.connect("localhost:9092") as transport:
    producer = Producer(transport, partitioner=FirstPartitionOnly())
    producer.send("events", Record(value=b"hello"))
```

Consumer support is not yet implemented; fetching, subscribing, and consumer groups will land in subsequent releases.

## Configuration

The producer takes its broker address and connection options as constructor arguments — there are no environment variables in v1.

| Setting | Default | Where |
|---|---|---|
| Broker address | required | `TcpTransport.connect("host:port")` |
| Dial timeout (seconds) | `5.0` | `TcpTransport.connect(..., timeout=...)` |
| Credential | anonymous | `TcpTransport.connect(..., credential_kind=..., credential=...)` |
| Partitioner | `DefaultPartitioner()` | `Producer(transport, partitioner=...)` |

TLS, mTLS, and `HOLOCRON_*` environment-variable fallbacks land alongside the consumer in a follow-up release.

## Development

```bash
uv sync --all-groups           # install runtime + dev dependencies
uv run pytest                  # run the test suite
uv run ruff check .            # lint
uv run ruff format --check .   # formatting check
uv run mypy                    # type check
```

Run the full pre-push check in one shot:

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
```

## Contributing

Issues and pull requests are welcome. Please read [`CONTRIBUTING.md`](CONTRIBUTING.md) before opening a PR. The short version:

- One PR, one concern; Conventional Commits for the title.
- Add tests with the change.
- Wire-protocol-breaking changes must land in lockstep with the corresponding broker PR.

## License

[MIT](LICENSE).
