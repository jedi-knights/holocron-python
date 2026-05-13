# holocron-python

Python SDK for the [Holocron](https://github.com/jedi-knights/holocron) message broker.

![CI](https://github.com/jedi-knights/holocron-python/actions/workflows/ci.yml/badge.svg)
![Maturity](https://img.shields.io/badge/maturity-pre--alpha-orange)
![Python](https://img.shields.io/badge/python-3.11+-3776AB?logo=python&logoColor=white)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Status:** Pre-alpha scaffolding. The package is published as a placeholder; no client surface is implemented yet. The on-disk format, wire protocol, and public APIs of the broker change without notice until its first tagged release.

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

- Package scaffolding (`pip install holocron`)
- Typed package (`py.typed` marker)
- CI on Python 3.11, 3.12, 3.13

What's planned (tracking the broker capability matrix):

- Producer with key-based partitioning, batching, and idempotent send
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

The package currently exposes only its version. A real producer/consumer surface lands once the broker's wire protocol stabilises.

```python
import holocron

print(holocron.__version__)
```

```
0.0.1
```

The intended shape (subject to change) will mirror the Go SDK:

```python
# Forward-looking sketch — not yet implemented.
from holocron import Producer, Consumer, Record

with Producer("localhost:9092") as p:
    p.send("events", Record(key=b"user-42", value=b'{"action":"login"}'))

with Consumer("localhost:9092", group="audit") as c:
    c.subscribe("events")
    for record in c.poll(max_records=32):
        print(record.key, record.value)
```

## Configuration

N/A — there is no client surface yet, so there is nothing to configure. Once the producer and consumer ship, connection settings (broker address, TLS, auth tokens) will be documented here and accepted both as constructor arguments and as `HOLOCRON_*` environment variables to match the broker's convention.

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
