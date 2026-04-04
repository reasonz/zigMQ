#!/usr/bin/env python3
"""End-to-end protocol regression tests for ZigMQ.

The script starts a fresh ZigMQ server per scenario, drives the text protocol
through TCP sockets, and prints a concise pass/fail summary.

Usage:
    python3 scripts/protocol_test.py
    python3 scripts/protocol_test.py --exe ./zig-out/bin/zigmq
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import sys
import time
from typing import Callable

from zigmq_testlib import (
    CURRENT_VERSION,
    DEFAULT_EXE,
    Client,
    TestFailure,
    expect_contains,
    expect_equal,
    expect_listing,
    expect_no_response,
    parse_info,
    parse_listing,
    resolve_exe,
    running_server,
)


@dataclasses.dataclass
class ScenarioResult:
    name: str
    ok: bool
    detail: str = ""


def scenario_queue_basics(exe_path: str) -> None:
    with running_server(exe_path, ["--capacity", "2", "--max-capacity", "4"]) as server:
        client = Client(server.host, server.port)
        try:
            expect_equal(client.request("ping"), b"+PONG\r\n", "PING")

            info = parse_info(client.request_all("info"))
            if info.get("initial_capacity") != "2":
                raise TestFailure(f"expected initial_capacity=2, got {info!r}")
            if info.get("max_capacity") != "4":
                raise TestFailure(f"expected max_capacity=4, got {info!r}")

            expect_equal(client.request("send jobs hello world"), b"+OK\r\n", "SEND")
            expect_equal(client.request("len jobs"), b"+1\r\n", "LEN after SEND")
            expect_equal(
                client.request("peek jobs"),
                b"$11\r\nhello world\r\n",
                "PEEK response",
            )
            expect_equal(
                client.request("recv jobs"),
                b"$11\r\nhello world\r\n",
                "RECV response",
            )
            expect_equal(client.request("recv jobs"), b"-ERR empty\r\n", "RECV empty")

            expect_equal(client.request("mq created"), b"+OK\r\n", "MQ create")
            expect_equal(client.request("LeN created"), b"+0\r\n", "case-insensitive LEN")
            expect_listing(client.request_all("queues"), {"jobs", "created"}, "QUEUES")
        finally:
            client.close()


def scenario_queue_growth_and_fifo(exe_path: str) -> None:
    with running_server(exe_path, ["--capacity", "2", "--max-capacity", "4"]) as server:
        client = Client(server.host, server.port)
        try:
            for value in ("one", "two", "three", "four"):
                expect_equal(client.request(f"send grow {value}"), b"+OK\r\n", f"SEND {value}")

            expect_equal(client.request("len grow"), b"+4\r\n", "LEN after growth")
            expect_equal(
                client.request("send grow five"),
                b"-ERR queue full\r\n",
                "SEND beyond max capacity",
            )

            for value in ("one", "two", "three", "four"):
                payload = f"${len(value)}\r\n{value}\r\n".encode("utf-8")
                expect_equal(client.request("recv grow"), payload, f"FIFO order for {value}")
        finally:
            client.close()


def scenario_pubsub_flow(exe_path: str) -> None:
    with running_server(exe_path, ["--capacity", "2", "--max-capacity", "4"]) as server:
        sub_a = Client(server.host, server.port)
        sub_b = Client(server.host, server.port)
        pub = Client(server.host, server.port)
        admin = Client(server.host, server.port)

        try:
            expect_equal(sub_a.request("sub news"), b"+OK\r\n", "SUB news on A")
            expect_equal(sub_b.request("sub news"), b"+OK\r\n", "SUB news on B")
            expect_equal(sub_b.request("sub tech"), b"+OK\r\n", "SUB tech on B")
            expect_listing(sub_b.request_all("subs"), {"news", "tech"}, "SUBS on B")

            pub.send("pub news hello")
            expect_equal(sub_a.recv_response(), b"+news:hello\r\n", "broadcast to A")
            expect_equal(sub_b.recv_response(), b"+news:hello\r\n", "broadcast to B")
            expect_equal(pub.recv_response(), b"+OK 2\r\n", "PUB acknowledgement")

            expect_listing(admin.request_all("topics"), {"news(2)", "tech(1)"}, "TOPICS before UNSUB")

            expect_equal(sub_b.request("unsub news"), b"+OK\r\n", "UNSUB news on B")
            expect_listing(admin.request_all("topics"), {"news(1)", "tech(1)"}, "TOPICS after UNSUB")

            pub.send("pub news again")
            expect_equal(sub_a.recv_response(), b"+news:again\r\n", "broadcast after partial UNSUB")
            expect_equal(pub.recv_response(), b"+OK 1\r\n", "PUB acknowledgement after UNSUB")
            expect_no_response(sub_b, timeout=0.2, label="UNSUB should stop delivery")

            expect_equal(sub_a.request("unsub"), b"+OK\r\n", "UNSUB all on A")
            sub_b.close()
            time.sleep(0.1)
            expect_listing(admin.request_all("topics"), set(), "TOPICS after disconnect cleanup")
        finally:
            sub_a.close()
            sub_b.close()
            pub.close()
            admin.close()


def scenario_pipelining(exe_path: str) -> None:
    with running_server(exe_path, ["--capacity", "2", "--max-capacity", "4"]) as server:
        client = Client(server.host, server.port)
        try:
            client.send_raw(b"PiNg\r\nINFO\r\n")
            combined = client.recv_response(timeout=1.0)
            expect_contains(combined, b"+PONG\r\n", "pipelined PING")
            expect_contains(
                combined,
                f"+ZigMQ {CURRENT_VERSION}\r\n".encode("utf-8"),
                "pipelined INFO",
            )
        finally:
            client.close()


def scenario_empty_topic_cleanup(exe_path: str) -> None:
    with running_server(exe_path, ["--capacity", "2", "--max-capacity", "4"]) as server:
        client = Client(server.host, server.port)
        try:
            expect_equal(client.request("pub ghost hello"), b"+OK 0\r\n", "PUB with zero subscribers")
            topics = client.request_all("topics")
            if parse_listing(topics):
                raise TestFailure(
                    "publishing to an empty topic leaves metadata behind; "
                    f"TOPICS returned {decode_text(topics)!r}"
                )
        finally:
            client.close()


def run_scenario(
    name: str,
    func: Callable[[str], None],
    exe_path: str,
) -> ScenarioResult:
    try:
        func(exe_path)
    except Exception as exc:
        return ScenarioResult(
            name=name,
            ok=False,
            detail=str(exc),
        )

    return ScenarioResult(name=name, ok=True)


def print_summary(results: list[ScenarioResult]) -> int:
    failures = [result for result in results if not result.ok]

    for result in results:
        prefix = "PASS" if result.ok else "FAIL"

        print(f"[{prefix}] {result.name}")
        if result.detail:
            for line in result.detail.splitlines():
                print(f"        {line}")

    passed = sum(1 for result in results if result.ok)
    print()
    print(f"Summary: {passed}/{len(results)} passed, {len(failures)} failed")

    return 0 if not failures else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ZigMQ protocol regression tests.")
    parser.add_argument(
        "--exe",
        default=DEFAULT_EXE,
        help="Path to the ZigMQ executable to test.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    exe_path = resolve_exe(args.exe)

    if not os.path.exists(exe_path):
        print(f"Executable not found: {exe_path}", file=sys.stderr)
        print("Build it first with: zig build", file=sys.stderr)
        return 2

    scenarios = [
        ("queue_basics", scenario_queue_basics),
        ("queue_growth_and_fifo", scenario_queue_growth_and_fifo),
        ("pubsub_flow", scenario_pubsub_flow),
        ("pipelining", scenario_pipelining),
        ("empty_topic_cleanup", scenario_empty_topic_cleanup),
    ]

    results = [
        run_scenario(name, func, exe_path)
        for name, func in scenarios
    ]
    return print_summary(results)


if __name__ == "__main__":
    sys.exit(main())
