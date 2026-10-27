#!/usr/bin/env python3
"""Publish the workspace's crates to crates.io, one at a time, in dependency order.

Resumable: a crate whose version is already on crates.io is skipped, so a rerun picks up where a
failed one stopped. On 429 (crates.io allows a burst of new crates, then about one per ten
minutes) it sleeps until the "try again after" time the response gives, then retries.

    publish-crates.py [--dry-run] [--exclude NAME ...]

`--dry-run` prints the plan (what's published, what's skipped) and publishes nothing. Needs
CARGO_REGISTRY_TOKEN for a real run. Run from the repository root.
"""

from __future__ import annotations

import argparse
import email.utils
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime

API = "https://crates.io/api/v1/crates"
USER_AGENT = "atlatl-release (https://github.com/camconn/atlatl)"
MAX_WAIT_S = 75 * 60
MAX_TRIES = 30
RETRY_AFTER = re.compile(r"try again after (.+?) (?:and|or) ", re.IGNORECASE)


def members(exclude: set[str]) -> list[tuple[str, str]]:
    """Publishable workspace members as (name, version), dependencies first."""
    meta = json.loads(
        subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    pkgs = {
        p["name"]: p
        for p in meta["packages"]
        if p["id"] in meta["workspace_members"]
        and p.get("publish") != []
        and p["name"] not in exclude
    }
    deps = {
        n: {d["name"] for d in p["dependencies"] if d["name"] in pkgs and d.get("kind") != "dev"}
        for n, p in pkgs.items()
    }
    order: list[str] = []
    while len(order) < len(pkgs):
        ready = sorted(n for n in pkgs if n not in order and deps[n] <= set(order))
        if not ready:
            sys.exit(f"publish-crates: dependency cycle among {sorted(set(pkgs) - set(order))}")
        order += ready
    return [(n, pkgs[n]["version"]) for n in order]


def published(name: str, version: str) -> bool:
    req = urllib.request.Request(f"{API}/{name}/{version}", headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30):
            return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def wait_s(output: str) -> float:
    """Seconds until crates.io's "try again after" time, else ten minutes."""
    m = RETRY_AFTER.search(output)
    if m:
        try:
            at = email.utils.parsedate_to_datetime(m.group(1))
            return max(0.0, (at - datetime.now(UTC)).total_seconds()) + 15
        except (TypeError, ValueError):
            pass
    return 600.0


def publish(name: str, version: str) -> None:
    for attempt in range(1, MAX_TRIES + 1):
        r = subprocess.run(
            ["cargo", "publish", "-p", name, "--locked"],
            check=False,
            capture_output=True,
            text=True,
        )
        sys.stdout.write(r.stdout)
        sys.stderr.write(r.stderr)
        if r.returncode == 0:
            return
        out = r.stdout + r.stderr
        if "already exists" in out and published(name, version):
            return
        if "429" not in out:
            sys.exit(f"publish-crates: {name} {version} failed")
        s = min(wait_s(out), MAX_WAIT_S)
        print(f"publish-crates: rate limited on {name}; waiting {s / 60:.1f} min (try {attempt})")
        sys.stdout.flush()
        time.sleep(s)
    sys.exit(f"publish-crates: {name} {version} still rate limited after {MAX_TRIES} tries")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--exclude", action="append", default=[])
    args = ap.parse_args()
    plan = members(set(args.exclude))
    for name, version in plan:
        if published(name, version):
            print(f"publish-crates: {name} {version} is already on crates.io; skipped")
        elif args.dry_run:
            print(f"publish-crates: would publish {name} {version}")
        else:
            print(f"publish-crates: publishing {name} {version}")
            sys.stdout.flush()
            publish(name, version)


if __name__ == "__main__":
    main()
