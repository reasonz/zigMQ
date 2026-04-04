"""Shared test helpers for ZigMQ protocol and stress harnesses."""

from __future__ import annotations

import contextlib
import dataclasses
import os
import signal
import socket
import subprocess
import time
from pathlib import Path
from typing import Iterable


DEFAULT_EXE = "./zig-out/bin/zigmq"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CURRENT_VERSION = (PROJECT_ROOT / "VERSION").read_text(encoding="utf-8").strip()
READ_TIMEOUT = 0.8
IDLE_TIMEOUT = 0.08
STARTUP_TIMEOUT = 5.0


class TestFailure(AssertionError):
    pass


class Client:
    def __init__(self, host: str, port: int, timeout: float = READ_TIMEOUT) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.pending = bytearray()

    def close(self) -> None:
        with contextlib.suppress(OSError):
            self.sock.close()

    def send(self, command: str) -> None:
        self.sock.sendall(command.encode("utf-8") + b"\r\n")

    def send_raw(self, payload: bytes) -> None:
        self.sock.sendall(payload)

    def _extract_one_response(self) -> bytes | None:
        if not self.pending:
            return None

        if self.pending[0] == ord("$"):
            header_end = bytes(self.pending).find(b"\r\n")
            if header_end == -1:
                return None

            header = bytes(self.pending[1:header_end])
            try:
                body_len = int(header.decode("utf-8"))
            except ValueError as exc:
                raise TestFailure(f"invalid bulk response header: {bytes(self.pending)!r}") from exc

            total_len = header_end + 2 + body_len + 2
            if len(self.pending) < total_len:
                return None

            response = bytes(self.pending[:total_len])
            del self.pending[:total_len]
            return response

        line_end = bytes(self.pending).find(b"\r\n")
        if line_end == -1:
            return None

        response = bytes(self.pending[: line_end + 2])
        del self.pending[: line_end + 2]
        return response

    def recv_one_response(self, timeout: float | None = None) -> bytes:
        timeout = self.timeout if timeout is None else timeout
        self.sock.settimeout(timeout)

        response = self._extract_one_response()
        if response is not None:
            return response

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                data = self.sock.recv(4096)
            except socket.timeout:
                continue

            if not data:
                raise TestFailure("server closed the connection unexpectedly")

            self.pending.extend(data)
            response = self._extract_one_response()
            if response is not None:
                return response

        raise TestFailure("timed out waiting for server response")

    def recv_response(self, timeout: float | None = None) -> bytes:
        timeout = self.timeout if timeout is None else timeout
        self.sock.settimeout(timeout)

        chunks: list[bytes] = []
        if self.pending:
            chunks.append(bytes(self.pending))
            self.pending.clear()

        first_chunk_deadline = time.monotonic() + timeout
        while not chunks:
            if time.monotonic() > first_chunk_deadline:
                raise TestFailure("timed out waiting for server response")
            try:
                data = self.sock.recv(4096)
            except socket.timeout:
                continue

            if not data:
                raise TestFailure("server closed the connection unexpectedly")
            chunks.append(data)

        while True:
            self.sock.settimeout(IDLE_TIMEOUT)
            try:
                data = self.sock.recv(4096)
            except socket.timeout:
                break

            if not data:
                break
            chunks.append(data)

        self.sock.settimeout(timeout)
        return b"".join(chunks)

    def request(self, command: str, timeout: float | None = None) -> bytes:
        self.send(command)
        return self.recv_one_response(timeout=timeout)

    def request_all(self, command: str, timeout: float | None = None) -> bytes:
        self.send(command)
        return self.recv_response(timeout=timeout)


@dataclasses.dataclass
class ServerHandle:
    process: subprocess.Popen[str]
    host: str
    port: int
    command: list[str]

    def shutdown(self) -> tuple[str, str]:
        if self.process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                self.process.send_signal(signal.SIGINT)

        try:
            stdout, stderr = self.process.communicate(timeout=2.0)
        except subprocess.TimeoutExpired:
            self.process.kill()
            stdout, stderr = self.process.communicate(timeout=2.0)

        return stdout, stderr


def resolve_exe(path: str) -> str:
    return os.path.abspath(path)


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.contextmanager
def running_server(exe_path: str, extra_args: Iterable[str]) -> Iterable[ServerHandle]:
    host = "127.0.0.1"
    port = find_free_port()
    command = [exe_path, "--port", str(port), *extra_args]
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    handle = ServerHandle(process=process, host=host, port=port, command=command)

    try:
        wait_for_server(handle)
        yield handle
    finally:
        stdout, stderr = handle.shutdown()
        if process.returncode not in (0, 1, -signal.SIGINT):
            raise TestFailure(
                "server exited unexpectedly\n"
                f"command: {' '.join(command)}\n"
                f"exit_code: {process.returncode}\n"
                f"stdout:\n{stdout}\n"
                f"stderr:\n{stderr}"
            )


def wait_for_server(handle: ServerHandle) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        if handle.process.poll() is not None:
            stdout, stderr = handle.process.communicate(timeout=0.2)
            raise TestFailure(
                "server terminated during startup\n"
                f"command: {' '.join(handle.command)}\n"
                f"exit_code: {handle.process.returncode}\n"
                f"stdout:\n{stdout}\n"
                f"stderr:\n{stderr}"
            )

        try:
            probe = socket.create_connection((handle.host, handle.port), timeout=0.2)
        except OSError as exc:
            last_error = exc
            time.sleep(0.05)
            continue

        probe.close()
        return

    raise TestFailure(f"server did not start listening in time: {last_error}")


def decode_text(data: bytes) -> str:
    return data.decode("utf-8")


def expect_equal(actual: bytes, expected: bytes, label: str) -> None:
    if actual != expected:
        raise TestFailure(
            f"{label} mismatch\nexpected: {expected!r}\nactual:   {actual!r}"
        )


def expect_contains(haystack: bytes, needle: bytes, label: str) -> None:
    if needle not in haystack:
        raise TestFailure(
            f"{label} missing substring\nneedle: {needle!r}\nactual: {haystack!r}"
        )


def parse_listing(response: bytes) -> list[str]:
    text = decode_text(response)
    if not text.startswith("+"):
        raise TestFailure(f"listing response must start with '+': {text!r}")

    body = text[1:]
    if body == "\r\n":
        return []

    body = body.rstrip("\r\n")
    if not body:
        return []
    return body.split("\r\n")


def expect_listing(response: bytes, expected_items: set[str], label: str) -> None:
    actual_items = set(parse_listing(response))
    if actual_items != expected_items:
        raise TestFailure(
            f"{label} mismatch\nexpected: {sorted(expected_items)!r}\n"
            f"actual:   {sorted(actual_items)!r}"
        )


def parse_info(response: bytes) -> dict[str, str]:
    text = decode_text(response).rstrip("\r\n")
    lines = text.split("\r\n")
    if not lines or not lines[0].startswith("+ZigMQ"):
        raise TestFailure(f"invalid INFO response: {text!r}")

    info = {"banner": lines[0][1:]}
    for line in lines[1:]:
        key, sep, value = line.partition(":")
        if sep != ":":
            raise TestFailure(f"invalid INFO field: {line!r}")
        info[key] = value
    return info


def expect_no_response(client: Client, timeout: float, label: str) -> None:
    try:
        data = client.recv_response(timeout=timeout)
    except TestFailure:
        return

    raise TestFailure(f"{label} expected no response, but got {data!r}")
