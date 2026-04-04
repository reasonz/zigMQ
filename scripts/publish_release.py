#!/usr/bin/env python3
"""Package and publish a GitHub release for ZigMQ."""

from __future__ import annotations

import json
import mimetypes
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
VERSION = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
TAG = f"v{VERSION}"
DIST_DIR = ROOT / "dist" / "release"


def run(cmd: list[str], *, capture: bool = False) -> str:
    result = subprocess.run(
        cmd,
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=capture,
    )
    return result.stdout.strip() if capture else ""


def github_repo() -> tuple[str, str]:
    url = run(["git", "remote", "get-url", "origin"], capture=True)
    match = re.search(r"github\.com[:/](.+?)/(.+?)(?:\.git)?$", url)
    if not match:
        raise RuntimeError(f"unsupported GitHub remote: {url}")
    return match.group(1), match.group(2)


def github_token() -> str:
    result = subprocess.run(
        ["git", "credential", "fill"],
        cwd=ROOT,
        input="protocol=https\nhost=github.com\n\n",
        text=True,
        capture_output=True,
        check=True,
    )
    fields: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            fields[key] = value
    token = fields.get("password", "")
    if not token:
        raise RuntimeError("no GitHub credential found via git credential helper")
    return token


def github_request(
    method: str,
    url: str,
    token: str,
    *,
    data: bytes | None = None,
    content_type: str = "application/json",
) -> dict | list | None:
    request = urllib.request.Request(url, method=method, data=data)
    request.add_header("Accept", "application/vnd.github+json")
    request.add_header("Authorization", f"Bearer {token}")
    request.add_header("X-GitHub-Api-Version", "2022-11-28")
    if data is not None:
        request.add_header("Content-Type", content_type)

    try:
        with urllib.request.urlopen(request) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API {method} {url} failed: {exc.code} {detail}") from exc

    if not payload:
        return None
    return json.loads(payload.decode("utf-8"))


def ensure_release_assets() -> list[Path]:
    run([sys.executable, str(ROOT / "scripts" / "package_release.py")])
    assets = sorted(DIST_DIR.glob(f"zigmq-v{VERSION}-*.tar.gz"))
    checksum = DIST_DIR / "SHA256SUMS.txt"
    if not assets or not checksum.exists():
        raise RuntimeError("release assets were not generated")
    return [*assets, checksum]


def ensure_tag() -> None:
    tags = run(["git", "tag", "--list", TAG], capture=True)
    if not tags:
        run(["git", "tag", "-a", TAG, "-m", f"Release {TAG}"])


def push_branch_and_tag() -> str:
    branch = run(["git", "branch", "--show-current"], capture=True)
    if not branch:
        raise RuntimeError("cannot publish from detached HEAD")
    run(["git", "push", "origin", branch])
    run(["git", "push", "origin", TAG])
    return branch


def release_body() -> str:
    return (
        f"## ZigMQ {VERSION}\n\n"
        "- Modular queue + pub/sub server written in Zig\n"
        "- Sharded queue/topic locking and lower-allocation message paths\n"
        "- Built-in unit, protocol, stress, and benchmark harnesses\n"
        "- Bilingual project documentation and packaged Linux/macOS binaries\n\n"
        "Assets:\n"
        "- Linux x86_64 musl archive\n"
        "- macOS arm64 archive\n"
        "- SHA256SUMS.txt\n"
    )


def get_or_create_release(owner: str, repo: str, token: str) -> dict:
    base_url = f"https://api.github.com/repos/{owner}/{repo}"
    tag_url = f"{base_url}/releases/tags/{TAG}"
    try:
        existing = github_request("GET", tag_url, token)
    except RuntimeError as exc:
        if " 404 " not in str(exc):
            raise
        existing = None

    payload = json.dumps(
        {
            "tag_name": TAG,
            "name": TAG,
            "body": release_body(),
            "draft": False,
            "prerelease": False,
        }
    ).encode("utf-8")

    if existing:
        release_id = existing["id"]
        return github_request(
            "PATCH",
            f"{base_url}/releases/{release_id}",
            token,
            data=payload,
        )

    created = github_request("POST", f"{base_url}/releases", token, data=payload)
    if not isinstance(created, dict):
        raise RuntimeError("unexpected release creation response")
    return created


def upload_assets(owner: str, repo: str, release: dict, token: str, assets: list[Path]) -> None:
    existing_assets = github_request(
        "GET",
        f"https://api.github.com/repos/{owner}/{repo}/releases/{release['id']}/assets",
        token,
    )
    existing_by_name = {
        asset["name"]: asset
        for asset in existing_assets
    } if isinstance(existing_assets, list) else {}

    for asset in assets:
        if asset.name in existing_by_name:
            github_request(
                "DELETE",
                f"https://api.github.com/repos/{owner}/{repo}/releases/assets/{existing_by_name[asset.name]['id']}",
                token,
            )

        upload_url = f"https://uploads.github.com/repos/{owner}/{repo}/releases/{release['id']}/assets"
        url = f"{upload_url}?{urllib.parse.urlencode({'name': asset.name})}"
        content_type = mimetypes.guess_type(asset.name)[0] or "application/octet-stream"
        github_request(
            "POST",
            url,
            token,
            data=asset.read_bytes(),
            content_type=content_type,
        )
        print(f"[UPLOADED] {asset.name}")


def main() -> int:
    owner, repo = github_repo()
    token = github_token()
    assets = ensure_release_assets()
    ensure_tag()
    branch = push_branch_and_tag()
    release = get_or_create_release(owner, repo, token)
    upload_assets(owner, repo, release, token, assets)

    print()
    print(f"Published {TAG} from branch {branch}")
    print(release["html_url"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
