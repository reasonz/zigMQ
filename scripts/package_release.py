#!/usr/bin/env python3
"""Build Linux/macOS release archives for ZigMQ."""

from __future__ import annotations

import argparse
import hashlib
import shutil
import stat
import subprocess
import tarfile
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DIST_DIR = ROOT / "dist" / "release"
VERSION = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
ARCHIVE_PREFIX = f"zigmq-v{VERSION}"
TARGETS = {
    "linux-x86_64": {
        "zig_target": "x86_64-linux-musl",
        "archive_suffix": "linux-x86_64",
        "platform": "Linux",
        "arch": "x86_64",
    },
    "macos-aarch64": {
        "zig_target": "aarch64-macos",
        "archive_suffix": "macos-aarch64",
        "platform": "macOS",
        "arch": "arm64",
    },
}


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def clean_stage(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def ensure_executable(path: Path) -> None:
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def build_target(key: str, spec: dict[str, str]) -> Path:
    stage_dir = DIST_DIR / f"{ARCHIVE_PREFIX}-{spec['archive_suffix']}"
    archive_path = DIST_DIR / f"{stage_dir.name}.tar.gz"

    clean_stage(stage_dir)
    if archive_path.exists():
        archive_path.unlink()

    run(
        [
            "zig",
            "build",
            f"-Dtarget={spec['zig_target']}",
            "-Doptimize=ReleaseFast",
        ]
    )

    binary_src = ROOT / "zig-out" / "bin" / "zigmq"
    if not binary_src.exists():
        raise FileNotFoundError(f"built binary not found: {binary_src}")

    binary_dst = stage_dir / "zigmq"
    shutil.copy2(binary_src, binary_dst)
    ensure_executable(binary_dst)

    for name in ("README.md", "README.zh-CN.md", "VERSION"):
        shutil.copy2(ROOT / name, stage_dir / name)

    metadata = textwrap.dedent(
        f"""\
        ZigMQ Release Package
        Version: {VERSION}
        Target: {spec['zig_target']}
        Platform: {spec['platform']}
        Architecture: {spec['arch']}
        Binary: zigmq
        """
    )
    (stage_dir / "RELEASE.txt").write_text(metadata, encoding="utf-8")

    with tarfile.open(archive_path, "w:gz", format=tarfile.PAX_FORMAT) as tar:
        tar.add(stage_dir, arcname=stage_dir.name)

    print(f"[PACKAGED] {key} -> {archive_path}")
    return archive_path


def write_checksums(files: list[Path]) -> Path:
    checksum_path = DIST_DIR / "SHA256SUMS.txt"
    lines: list[str] = []
    for path in files:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{digest}  {path.name}")
    checksum_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[CHECKSUMS] {checksum_path}")
    return checksum_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ZigMQ release archives.")
    parser.add_argument(
        "--targets",
        nargs="*",
        choices=sorted(TARGETS.keys()),
        default=sorted(TARGETS.keys()),
        help="Which target archives to build.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    DIST_DIR.mkdir(parents=True, exist_ok=True)

    archives = [build_target(key, TARGETS[key]) for key in args.targets]
    checksum_path = write_checksums(archives)

    print()
    print("Release assets:")
    for path in [*archives, checksum_path]:
        print(f"  {path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
