# ZigMQ

[简体中文](README.zh-CN.md)

ZigMQ is a lightweight in-memory message queue and pub/sub server written in Zig. It ships as a single binary, uses a small text protocol, and is designed for fast local deployment, predictable behavior, and straightforward hacking.

## Why ZigMQ

- One server, two messaging patterns: point-to-point queues and pub/sub topics.
- Single binary, no external service dependency, easy to drop into scripts, prototypes, and internal tools.
- Sharded queue/topic locking with snapshot-based fanout, so hot paths stay simple and safe.
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

- `zigmq-v0.4.0-linux-x86_64.tar.gz`
- `zigmq-v0.4.0-macos-aarch64.tar.gz`
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
./zig-out/bin/zigmq --port 6379 --capacity 1024 --max-capacity 16384
```

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

Representative local baseline on April 4, 2026:

- Machine: Apple Silicon macOS
- Zig: `0.15.2`
- Build: `ReleaseFast`
- Transport: loopback TCP
- Command: `zig build benchmark`

| Scenario | Result |
| --- | --- |
| `queue_contention` | `45,659 ops/s`, `21.9 us/op` |
| `independent_queue_roundtrips` | `46,002 ops/s`, `21.7 us/op` |
| `pubsub_fanout publish_rate` | `12,765 msg/s` |
| `pubsub_fanout delivery_rate` | `76,589 deliveries/s` |

These numbers are intended as a practical local baseline, not a formal lab benchmark. They are most useful for comparing changes across commits on the same machine.

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
