#!/usr/bin/env python3
"""Concurrent stress and baseline throughput harness for ZigMQ."""

from __future__ import annotations

import argparse
import dataclasses
import socket
import sys
import threading
import time
from typing import Callable

from zigmq_testlib import (
    DEFAULT_EXE,
    Client,
    TestFailure,
    expect_equal,
    resolve_exe,
    running_server,
)


@dataclasses.dataclass
class ScenarioResult:
    name: str
    ok: bool
    detail: str = ""
    metrics: dict[str, str] = dataclasses.field(default_factory=dict)


def parse_bulk_response(response: bytes) -> str:
    if not response.startswith(b"$"):
        raise TestFailure(f"expected bulk response, got {response!r}")

    try:
        header, body, trailer = response.split(b"\r\n", 2)
    except ValueError as exc:
        raise TestFailure(f"malformed bulk response: {response!r}") from exc

    expected_len = int(header[1:])
    if trailer != b"":
        raise TestFailure(f"unexpected trailing bytes in bulk response: {response!r}")
    if len(body) != expected_len:
        raise TestFailure(
            f"bulk length mismatch: expected {expected_len}, got {len(body)}"
        )
    return body.decode("utf-8")


def format_rate(value: float) -> str:
    return f"{value:,.0f}/s"


def send_repeated(sock: socket.socket, command: bytes, count: int) -> None:
    max_batch_bytes = 1 << 20
    per_batch = max(1, max_batch_bytes // max(1, len(command)))
    remaining = count
    while remaining > 0:
        batch_count = min(remaining, per_batch)
        sock.sendall(command * batch_count)
        remaining -= batch_count


def recv_simple_lines(sock: socket.socket, count: int) -> None:
    seen = 0
    pending = b""
    saw_error = False

    while seen < count:
        chunk = sock.recv(65536)
        if not chunk:
            raise TestFailure("server closed the connection unexpectedly")
        if b"-ERR" in chunk:
            saw_error = True

        pending += chunk
        line_count = pending.count(b"\r\n")
        if line_count == 0:
            continue

        seen += line_count
        last_line_end = pending.rfind(b"\r\n")
        pending = pending[last_line_end + 2 :]

    if saw_error:
        raise TestFailure("pipelined command stream returned an error response")


def recv_bulk_responses(sock: socket.socket, count: int) -> None:
    seen = 0
    pending = bytearray()

    while seen < count:
        chunk = sock.recv(65536)
        if not chunk:
            raise TestFailure("server closed the connection unexpectedly")
        pending.extend(chunk)

        while pending:
            if pending[0] != ord("$"):
                raise TestFailure(f"expected bulk response, got {bytes(pending[:64])!r}")

            header_end = pending.find(b"\r\n")
            if header_end == -1:
                break

            try:
                body_len = int(pending[1:header_end])
            except ValueError as exc:
                raise TestFailure(f"invalid bulk response header: {bytes(pending[:64])!r}") from exc

            response_len = header_end + 2 + body_len + 2
            if len(pending) < response_len:
                break
            if pending[response_len - 2 : response_len] != b"\r\n":
                raise TestFailure(f"malformed bulk response: {bytes(pending[:64])!r}")

            del pending[:response_len]
            seen += 1
            if seen >= count:
                break


def run_pipelined_lines(
    sock: socket.socket,
    command: bytes,
    count: int,
    window: int,
) -> None:
    remaining = count
    while remaining > 0:
        batch_count = min(remaining, window)
        send_repeated(sock, command, batch_count)
        recv_simple_lines(sock, batch_count)
        remaining -= batch_count


def run_pipelined_bulk(
    sock: socket.socket,
    command: bytes,
    count: int,
    window: int,
) -> None:
    remaining = count
    while remaining > 0:
        batch_count = min(remaining, window)
        send_repeated(sock, command, batch_count)
        recv_bulk_responses(sock, batch_count)
        remaining -= batch_count


def scenario_pipelined_send(exe_path: str, args: argparse.Namespace) -> dict[str, str]:
    queue_name = "pipeline-send"
    total_messages = args.pipeline_messages
    payload = "x" * args.pipeline_payload_bytes
    command = f"send {queue_name} {payload}\r\n".encode("utf-8")

    with running_server(
        exe_path,
        [
            "--capacity",
            str(args.capacity),
            "--max-capacity",
            str(max(args.max_capacity, total_messages)),
        ],
    ) as server:
        sock = socket.create_connection((server.host, server.port), timeout=args.deadline_seconds)
        sock.settimeout(args.deadline_seconds)
        try:
            start = time.perf_counter()
            run_pipelined_lines(sock, command, total_messages, args.pipeline_window)
            duration = time.perf_counter() - start
        finally:
            sock.close()

        admin = Client(server.host, server.port, timeout=args.deadline_seconds)
        try:
            expect_equal(
                admin.request(f"len {queue_name}", timeout=args.deadline_seconds),
                f"+{total_messages}\r\n".encode("utf-8"),
                "pipelined send final len",
            )
        finally:
            admin.close()

    return {
        "messages": str(total_messages),
        "payload_bytes": str(args.pipeline_payload_bytes),
        "throughput": format_rate(total_messages / duration),
        "avg_us_per_op": f"{(duration * 1_000_000) / total_messages:.1f}",
    }


def scenario_pipelined_recv(exe_path: str, args: argparse.Namespace) -> dict[str, str]:
    queue_name = "pipeline-recv"
    total_messages = args.pipeline_messages
    payload = "x" * args.pipeline_payload_bytes
    send_command = f"send {queue_name} {payload}\r\n".encode("utf-8")
    recv_command = f"recv {queue_name}\r\n".encode("utf-8")

    with running_server(
        exe_path,
        [
            "--capacity",
            str(args.capacity),
            "--max-capacity",
            str(max(args.max_capacity, total_messages)),
        ],
    ) as server:
        sock = socket.create_connection((server.host, server.port), timeout=args.deadline_seconds)
        sock.settimeout(args.deadline_seconds)
        try:
            run_pipelined_lines(sock, send_command, total_messages, args.pipeline_window)

            start = time.perf_counter()
            run_pipelined_bulk(sock, recv_command, total_messages, args.pipeline_window)
            duration = time.perf_counter() - start
        finally:
            sock.close()

        admin = Client(server.host, server.port, timeout=args.deadline_seconds)
        try:
            expect_equal(
                admin.request(f"len {queue_name}", timeout=args.deadline_seconds),
                b"+0\r\n",
                "pipelined recv final len",
            )
        finally:
            admin.close()

    return {
        "messages": str(total_messages),
        "payload_bytes": str(args.pipeline_payload_bytes),
        "throughput": format_rate(total_messages / duration),
        "avg_us_per_op": f"{(duration * 1_000_000) / total_messages:.1f}",
    }


def warmup(exe_path: str) -> None:
    with running_server(exe_path, ["--capacity", "1024", "--max-capacity", "4096"]) as server:
        client = Client(server.host, server.port, timeout=2.0)
        try:
            for i in range(200):
                payload = f"warm-{i}"
                expect_equal(client.request(f"send warmup {payload}"), b"+OK\r\n", "warmup send")
                _ = parse_bulk_response(client.request("recv warmup"))
        finally:
            client.close()


def scenario_queue_contention(exe_path: str, args: argparse.Namespace) -> dict[str, str]:
    queue_name = "stress-q"
    total_messages = args.queue_producers * args.queue_messages
    start_event = threading.Event()
    producers_done = threading.Event()
    errors: list[str] = []
    produced: list[str] = []
    received: list[str] = []
    received_set: set[str] = set()
    duplicates: set[str] = set()
    lock = threading.Lock()

    with running_server(
        exe_path,
        [
            "--capacity",
            str(args.capacity),
            "--max-capacity",
            str(max(args.max_capacity, total_messages)),
        ],
    ) as server:
        def producer_thread(producer_id: int) -> None:
            client = Client(server.host, server.port, timeout=1.0)
            try:
                start_event.wait()
                for msg_index in range(args.queue_messages):
                    payload = f"p{producer_id}-m{msg_index}"
                    expect_equal(
                        client.request(f"send {queue_name} {payload}"),
                        b"+OK\r\n",
                        f"producer {producer_id} send {msg_index}",
                    )
                    with lock:
                        produced.append(payload)
            except Exception as exc:
                with lock:
                    errors.append(f"producer {producer_id}: {exc}")
            finally:
                client.close()

        def consumer_thread(consumer_id: int) -> None:
            client = Client(server.host, server.port, timeout=1.0)
            deadline = time.monotonic() + args.deadline_seconds
            try:
                start_event.wait()
                while time.monotonic() < deadline:
                    with lock:
                        if len(received) >= total_messages:
                            return

                    response = client.request(f"recv {queue_name}", timeout=1.0)
                    if response in (b"-ERR empty\r\n", b"-ERR queue not found\r\n"):
                        if producers_done.is_set():
                            with lock:
                                if len(received) >= total_messages:
                                    return
                        time.sleep(0.001)
                        continue

                    payload = parse_bulk_response(response)
                    with lock:
                        if payload in received_set:
                            duplicates.add(payload)
                        else:
                            received_set.add(payload)
                            received.append(payload)
                        if len(received) >= total_messages:
                            return

                with lock:
                    errors.append(
                        f"consumer {consumer_id}: deadline exceeded after receiving {len(received)} messages"
                    )
            except Exception as exc:
                with lock:
                    errors.append(f"consumer {consumer_id}: {exc}")
            finally:
                client.close()

        producer_threads = [
            threading.Thread(target=producer_thread, args=(i,), daemon=True)
            for i in range(args.queue_producers)
        ]
        consumer_threads = [
            threading.Thread(target=consumer_thread, args=(i,), daemon=True)
            for i in range(args.queue_consumers)
        ]

        for thread in producer_threads + consumer_threads:
            thread.start()

        start = time.perf_counter()
        start_event.set()

        for thread in producer_threads:
            thread.join(timeout=args.deadline_seconds)
        producers_done.set()
        for thread in consumer_threads:
            thread.join(timeout=args.deadline_seconds)

        duration = time.perf_counter() - start

        if any(thread.is_alive() for thread in producer_threads + consumer_threads):
            errors.append("queue contention threads did not finish before timeout")

        produced_set = set(produced)
        missing = produced_set - received_set
        unexpected = received_set - produced_set

        admin = Client(server.host, server.port, timeout=1.0)
        try:
            queue_len = admin.request(f"len {queue_name}")
        finally:
            admin.close()

        if queue_len != b"+0\r\n":
            errors.append(f"queue length after stress expected +0, got {queue_len!r}")
        if duplicates:
            errors.append(f"duplicate deliveries detected: {sorted(duplicates)[:5]!r}")
        if len(produced) != total_messages:
            errors.append(
                f"expected {total_messages} produced messages, got {len(produced)}"
            )
        if len(received) != total_messages:
            errors.append(
                f"expected {total_messages} received messages, got {len(received)}"
            )
        if missing:
            errors.append(f"missing messages: {sorted(missing)[:5]!r}")
        if unexpected:
            errors.append(f"unexpected messages: {sorted(unexpected)[:5]!r}")

    if errors:
        raise TestFailure("\n".join(errors))

    ops = total_messages * 2
    return {
        "messages": str(total_messages),
        "ops": str(ops),
        "throughput": format_rate(ops / duration),
        "avg_us_per_op": f"{(duration * 1_000_000) / ops:.1f}",
    }


def scenario_pubsub_fanout(exe_path: str, args: argparse.Namespace) -> dict[str, str]:
    topic = "fanout"
    expected_lines = [f"+{topic}:msg-{i}" for i in range(args.pub_messages)]
    start_event = threading.Event()
    ready_count = 0
    ready_lock = threading.Lock()
    all_ready = threading.Event()
    errors: list[str] = []
    deliveries: dict[int, list[str]] = {}

    with running_server(
        exe_path,
        [
            "--capacity",
            str(args.capacity),
            "--max-capacity",
            str(max(args.max_capacity, args.queue_messages * args.queue_producers)),
        ],
    ) as server:
        def subscriber_thread(subscriber_id: int) -> None:
            nonlocal ready_count
            client = Client(server.host, server.port, timeout=1.0)
            buffer = b""
            lines: list[str] = []
            deadline = time.monotonic() + args.deadline_seconds
            try:
                expect_equal(
                    client.request(f"sub {topic}"),
                    b"+OK\r\n",
                    f"subscriber {subscriber_id} sub",
                )
                with ready_lock:
                    ready_count += 1
                    if ready_count == args.pubsub_subscribers:
                        all_ready.set()

                start_event.wait()

                client.sock.settimeout(0.5)
                while len(lines) < args.pub_messages and time.monotonic() < deadline:
                    try:
                        chunk = client.sock.recv(4096)
                    except socket.timeout:
                        continue

                    if not chunk:
                        raise TestFailure("subscriber connection closed unexpectedly")
                    buffer += chunk

                    while b"\r\n" in buffer:
                        raw_line, buffer = buffer.split(b"\r\n", 1)
                        if not raw_line:
                            continue
                        lines.append(raw_line.decode("utf-8"))
                        if len(lines) >= args.pub_messages:
                            break

                if len(lines) != args.pub_messages:
                    raise TestFailure(
                        f"subscriber {subscriber_id} expected {args.pub_messages} messages, got {len(lines)}"
                    )

                deliveries[subscriber_id] = lines
            except Exception as exc:
                errors.append(f"subscriber {subscriber_id}: {exc}")
            finally:
                client.close()

        subscriber_threads = [
            threading.Thread(target=subscriber_thread, args=(i,), daemon=True)
            for i in range(args.pubsub_subscribers)
        ]

        for thread in subscriber_threads:
            thread.start()

        if not all_ready.wait(timeout=args.deadline_seconds):
            raise TestFailure("subscribers did not finish subscribing before timeout")

        publisher = Client(server.host, server.port, timeout=1.0)
        try:
            start = time.perf_counter()
            start_event.set()

            for msg_index in range(args.pub_messages):
                expect_equal(
                    publisher.request(f"pub {topic} msg-{msg_index}"),
                    f"+OK {args.pubsub_subscribers}\r\n".encode("utf-8"),
                    f"publisher ack {msg_index}",
                )

            for thread in subscriber_threads:
                thread.join(timeout=args.deadline_seconds)

            duration = time.perf_counter() - start
        finally:
            publisher.close()

        if any(thread.is_alive() for thread in subscriber_threads):
            errors.append("pubsub subscriber threads did not finish before timeout")

        for subscriber_id in range(args.pubsub_subscribers):
            actual = deliveries.get(subscriber_id)
            if actual != expected_lines:
                errors.append(
                    f"subscriber {subscriber_id} message stream mismatch: {actual!r}"
                )

        topics = None
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            admin = Client(server.host, server.port, timeout=1.0)
            try:
                topics = admin.request("topics")
            finally:
                admin.close()

            if topics == b"+\r\n":
                break
            time.sleep(0.05)

        if topics != b"+\r\n":
            errors.append(f"topics should be empty after disconnect cleanup, got {topics!r}")

    if errors:
        raise TestFailure("\n".join(errors))

    return {
        "publishes": str(args.pub_messages),
        "subscribers": str(args.pubsub_subscribers),
        "publish_rate": format_rate(args.pub_messages / duration),
        "delivery_rate": format_rate((args.pub_messages * args.pubsub_subscribers) / duration),
    }


def scenario_independent_queue_roundtrips(exe_path: str, args: argparse.Namespace) -> dict[str, str]:
    worker_count = args.roundtrip_workers
    iterations = args.roundtrip_messages
    start_event = threading.Event()
    errors: list[str] = []

    with running_server(
        exe_path,
        [
            "--capacity",
            str(args.capacity),
            "--max-capacity",
            str(max(args.max_capacity, iterations * 2)),
        ],
    ) as server:
        def worker_thread(worker_id: int) -> None:
            client = Client(server.host, server.port, timeout=1.0)
            queue_name = f"parallel-{worker_id}"
            try:
                expect_equal(
                    client.request(f"mq {queue_name}"),
                    b"+OK\r\n",
                    f"worker {worker_id} queue create",
                )
                start_event.wait()

                for msg_index in range(iterations):
                    payload = f"w{worker_id}-m{msg_index}"
                    expect_equal(
                        client.request(f"send {queue_name} {payload}"),
                        b"+OK\r\n",
                        f"worker {worker_id} send {msg_index}",
                    )
                    actual = parse_bulk_response(client.request(f"recv {queue_name}"))
                    if actual != payload:
                        raise TestFailure(
                            f"worker {worker_id} expected {payload!r}, got {actual!r}"
                        )

                expect_equal(
                    client.request(f"len {queue_name}"),
                    b"+0\r\n",
                    f"worker {worker_id} final len",
                )
            except Exception as exc:
                errors.append(f"worker {worker_id}: {exc}")
            finally:
                client.close()

        threads = [
            threading.Thread(target=worker_thread, args=(i,), daemon=True)
            for i in range(worker_count)
        ]
        for thread in threads:
            thread.start()

        start = time.perf_counter()
        start_event.set()
        for thread in threads:
            thread.join(timeout=args.deadline_seconds)
        duration = time.perf_counter() - start

        if any(thread.is_alive() for thread in threads):
            errors.append("independent queue roundtrip threads did not finish before timeout")

    if errors:
        raise TestFailure("\n".join(errors))

    ops = worker_count * iterations * 2
    return {
        "workers": str(worker_count),
        "ops": str(ops),
        "throughput": format_rate(ops / duration),
        "avg_us_per_op": f"{(duration * 1_000_000) / ops:.1f}",
    }


def run_scenario(
    name: str,
    func: Callable[[str, argparse.Namespace], dict[str, str]],
    exe_path: str,
    args: argparse.Namespace,
) -> ScenarioResult:
    try:
        metrics = func(exe_path, args)
    except Exception as exc:
        return ScenarioResult(name=name, ok=False, detail=str(exc))

    return ScenarioResult(name=name, ok=True, metrics=metrics)


def print_summary(results: list[ScenarioResult]) -> int:
    failures = [result for result in results if not result.ok]

    for result in results:
        prefix = "PASS" if result.ok else "FAIL"
        print(f"[{prefix}] {result.name}")
        if result.metrics:
            metrics = ", ".join(
                f"{key}={value}" for key, value in result.metrics.items()
            )
            print(f"        {metrics}")
        if result.detail:
            for line in result.detail.splitlines():
                print(f"        {line}")

    print()
    print(
        f"Summary: {sum(1 for result in results if result.ok)}/{len(results)} passed, "
        f"{len(failures)} failed"
    )
    return 0 if not failures else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run concurrent stress and baseline throughput tests for ZigMQ."
    )
    parser.add_argument("--exe", default=DEFAULT_EXE, help="Path to the ZigMQ executable.")
    parser.add_argument("--capacity", type=int, default=64, help="Initial queue capacity.")
    parser.add_argument("--max-capacity", type=int, default=4096, help="Max queue capacity.")
    parser.add_argument("--queue-producers", type=int, default=4, help="Number of producer clients.")
    parser.add_argument("--queue-consumers", type=int, default=4, help="Number of consumer clients.")
    parser.add_argument("--queue-messages", type=int, default=1000, help="Messages per producer.")
    parser.add_argument("--pubsub-subscribers", type=int, default=6, help="Number of subscriber clients.")
    parser.add_argument("--pub-messages", type=int, default=500, help="Published messages per run.")
    parser.add_argument("--roundtrip-workers", type=int, default=8, help="Workers on distinct queues for parallel roundtrip benchmarking.")
    parser.add_argument("--roundtrip-messages", type=int, default=1000, help="Messages per worker in the independent-queue roundtrip scenario.")
    parser.add_argument("--pipeline-messages", type=int, default=200000, help="Messages per pipelined benchmark scenario.")
    parser.add_argument("--pipeline-payload-bytes", type=int, default=16, help="Payload size for pipelined benchmark scenarios.")
    parser.add_argument("--pipeline-window", type=int, default=4096, help="Commands in flight per pipelined benchmark window.")
    parser.add_argument("--deadline-seconds", type=float, default=10.0, help="Per-scenario timeout budget.")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Use smaller workloads suitable for frequent local regression runs.",
    )
    return parser.parse_args()


def apply_quick_profile(args: argparse.Namespace) -> None:
    if not args.quick:
        return

    args.queue_producers = min(args.queue_producers, 2)
    args.queue_consumers = min(args.queue_consumers, 2)
    args.queue_messages = min(args.queue_messages, 50)
    args.pubsub_subscribers = min(args.pubsub_subscribers, 3)
    args.pub_messages = min(args.pub_messages, 20)
    args.roundtrip_workers = min(args.roundtrip_workers, 4)
    args.roundtrip_messages = min(args.roundtrip_messages, 40)
    args.pipeline_messages = min(args.pipeline_messages, 1000)
    args.pipeline_window = min(args.pipeline_window, args.pipeline_messages)
    args.deadline_seconds = min(args.deadline_seconds, 6.0)


def main() -> int:
    args = parse_args()
    apply_quick_profile(args)
    args.pipeline_window = max(1, min(args.pipeline_window, args.pipeline_messages))
    exe_path = resolve_exe(args.exe)
    warmup(exe_path)

    scenarios = [
        ("queue_contention", scenario_queue_contention),
        ("independent_queue_roundtrips", scenario_independent_queue_roundtrips),
        ("pubsub_fanout", scenario_pubsub_fanout),
        ("pipelined_send", scenario_pipelined_send),
        ("pipelined_recv", scenario_pipelined_recv),
    ]
    results = [run_scenario(name, func, exe_path, args) for name, func in scenarios]
    return print_summary(results)


if __name__ == "__main__":
    sys.exit(main())
