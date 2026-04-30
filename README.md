# ZigMQ

[简体中文](README.zh-CN.md)

ZigMQ is a lightweight in-memory message queue and pub/sub server written in Zig. It ships as a single binary, uses a small text protocol, and is designed for fast local deployment, predictable behavior, and straightforward hacking.

## Why ZigMQ

- One server, two messaging patterns: point-to-point queues and pub/sub topics.
- Single binary, no external service dependency, easy to drop into scripts, prototypes, and internal tools.
- Sharded queue/topic management with lock-free queue operations and snapshot-based fanout, so hot paths stay simple and fast.
- Clear queue-full semantics, explicit error responses, and automatic queue growth up to a configured ceiling.
- Built-in unit, protocol, stress, and benchmark harnesses for safe iteration.

## What It Does Well

| Area | ZigMQ advantage |
| --- | --- |
| Deployment | One binary, one port, zero extra infrastructure |
| Protocol | Human-readable text protocol that is easy to test with `nc`, `telnet`, or raw sockets |
| Messaging | FIFO queues plus pub/sub topics in the same process |
| Performance model | Low-allocation hot paths, auto-growing ring buffers, sharded state locks |
| Operability | Simple `PING`, `INFO`, `QUEUES`, `TOPICS`, `SUBS` commands |
| Developer experience | Modular Zig codebase and repeatable regression scripts |

## Install

### Option 1: Use a release package

Release assets are published as:

- `zigmq-v0.5.0-linux-x86_64.tar.gz`
- `zigmq-v0.5.0-macos-aarch64.tar.gz`
- `SHA256SUMS.txt`

Extract the archive and run `./zigmq`.

### Option 2: Build from source

Requirements:

- Zig `0.15.2`

```bash
git clone https://github.com/reasonz/zigMQ.git
cd zigMQ
zig build -Doptimize=ReleaseFast
./zig-out/bin/zigmq
```

## Quick Start

Start the server:

```bash
./zig-out/bin/zigmq --port 8388 --capacity 1024 --max-capacity 16384
```

The default port is `8388`, so `./zig-out/bin/zigmq` also listens on `8388` unless you override it with `--port`.

Queue example:

```text
send jobs hello world
+OK

peek jobs
$11
hello world

recv jobs
$11
hello world
```

Pub/Sub example:

```text
sub news
+OK

pub news shipped
+news:shipped
+OK 1
```

## Python SDK

If you want to use ZigMQ from Python, install the official SDK from PyPI:

```bash
pip install zigmq
```

Repository:

- [reasonz/zigMQ-python-SDK](https://github.com/reasonz/zigMQ-python-SDK)

Queue example:

```python
from zigmq import Client

with Client() as mq:
    mq.send("jobs", "hello world")
    print(mq.peek("jobs"))
    print(mq.recv("jobs"))
```

Pub/Sub example:

```python
from zigmq import Client, Subscriber

with Subscriber() as sub:
    sub.subscribe("news")

    with Client() as mq:
        delivered = mq.publish("news", "shipped")
        print(delivered)

    event = sub.get(timeout=1.0)
    print(event.topic, event.message)
```

The SDK connects to `127.0.0.1:8388` by default, which matches the current ZigMQ server default port.

## Command Reference

| Command | Example | Description |
| --- | --- | --- |
| `send <queue> <msg>` | `send jobs hello` | Push a message into a queue |
| `recv <queue>` | `recv jobs` | Pop the next message from a queue |
| `peek <queue>` | `peek jobs` | Read the head message without removing it |
| `len <queue>` | `len jobs` | Get queue length |
| `mq <queue>` | `mq jobs` | Create a queue explicitly |
| `queues` | `queues` | List queues |
| `sub <topic>` | `sub news` | Subscribe current connection to a topic |
| `unsub [topic]` | `unsub news` | Unsubscribe one topic or all subscriptions |
| `pub <topic> <msg>` | `pub news shipped` | Broadcast to topic subscribers |
| `topics` | `topics` | List active topics and subscriber counts |
| `subs` | `subs` | List subscriptions for the current connection |
| `ping` | `ping` | Health check |
| `info` | `info` | Server version and capacity info |

Protocol response shapes:

- Success: `+OK\r\n`
- Error: `-ERR <message>\r\n`
- Bulk message: `$<length>\r\n<body>\r\n`
- Broadcast: `+<topic>:<message>\r\n`

## Quality Gates

```bash
zig build test
zig build protocol-test
zig build stress-test
zig build benchmark
```

What they cover:

- `test`: unit coverage for queue, connection, pub/sub, and server internals
- `protocol-test`: end-to-end correctness for queue, pub/sub, pipelining, and cleanup semantics
- `stress-test`: concurrent regression scenarios
- `benchmark`: local throughput baseline harness

## Performance Baseline

Representative local baseline on April 30, 2026 (v0.5.0):

- Machine: Apple Silicon macOS
- Zig: `0.15.2`
- Build: `Debug` (development baseline)
- Transport: loopback TCP
- Command: `zig build benchmark`

| Scenario | Result |
| --- | --- |
| `queue_contention` | `64,000 ops/s`, `15.6 us/op` |
| `independent_queue_roundtrips` | `42,700 ops/s`, `23.4 us/op` |
| `pubsub_fanout publish_rate` | `9,100 msg/s` |
| `pubsub_fanout delivery_rate` | `91,500 deliveries/s` |
| `pipelined_send` | `1,580,000 msg/s`, `0.6 us/op` |
| `pipelined_recv` | `1,350,000 msg/s`, `0.7 us/op` |

These numbers are intended as a practical local baseline, not a formal lab benchmark. They are most useful for comparing changes across commits on the same machine.

Pipelined send/receive throughput demonstrates the server-side processing capability (~1.5M msg/s). Synchronous scenarios are bounded by Python client round-trip latency over loopback TCP.

## Changelog

### v0.5.0 (2026-04-30)

- **Lock-free queue operations**: Replaced mutex-based queue push/pop with CAS-based lock-free MPMC ring buffer algorithm. Queue hot paths no longer serialize on mutex, laying the foundation for thread-pool based connection handling.
- **Allocator fix**: Replaced `smp_allocator` (fixed-size bump allocator) with `GeneralPurposeAllocator` to prevent OOM in long-running servers.
- **Write buffering**: Added per-connection write buffer with response batching to reduce TCP syscalls and support protocol pipelining.
- **Operation dispatch**: Replaced string-based command matching (`isOp`) with a pre-parsed `Operation` enum for faster command dispatch.
- **Inline message storage**: Messages ≤32 bytes are stored inline (no heap allocation) in the ring buffer.
- **Protocol hardening**: Fixed INFO response buffer overflow, corrected `broadcastSnapshot` return type.
- **Minimum ring buffer capacity**: Enforced minimum capacity of 2 for algorithmic correctness of the lock-free sequence protocol.

## Release Workflow

Build release archives:

```bash
python3 scripts/package_release.py
```

Publish the GitHub release:

```bash
python3 scripts/publish_release.py
```

The package script writes release artifacts to `dist/release/` and produces `SHA256SUMS.txt`.

## Project Status

ZigMQ is a compact, high-signal messaging server for learning, internal tools, automation backends, and early-stage product infrastructure. It is not trying to out-feature Redis or NATS; it is trying to stay small, understandable, and fast enough to be genuinely useful.
