#!/usr/bin/env python3
"""Check a built dagster-atlatl wheel's layout and repackage it as the standalone tarball.

The wheel's `.data/scripts/atlatl` and `.data/data/{lib,share}/atlatl/...` must be the only data
entries (spec 09 §1). The tarball holds the same prefix tree:
  atlatl-<ver>-<platform>/{bin/atlatl, lib/atlatl/, share/atlatl/, LICENSE-THIRD-PARTY}

Usage: wheel-to-tarball.py <wheel> <platform> <out dir> <LICENSE-THIRD-PARTY>
"""

from __future__ import annotations

import io
import os
import sys
import tarfile
import time
import zipfile
from pathlib import Path

SQLITE = "lib/atlatl/libsqlite3.so.0"


def fail(msg: str) -> None:
    sys.exit(f"wheel-to-tarball: {msg}")


def main() -> None:
    if len(sys.argv) != 5:
        fail("usage: wheel-to-tarball.py <wheel> <platform> <out dir> <LICENSE-THIRD-PARTY>")
    wheel, platform, out, notices = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3]), sys.argv[4]
    dist, version = wheel.name.split("-")[:2]
    data = f"{dist}-{version}.data/"
    osx = platform.startswith("osx")
    libs = ["lib/atlatl/libduckdb.dylib"] if osx else ["lib/atlatl/libduckdb.so", SQLITE]
    files: dict[str, bytes] = {}
    with zipfile.ZipFile(wheel) as z:
        for name in z.namelist():
            if not name.startswith(data):
                continue
            rel = name[len(data) :]
            if rel == "scripts/atlatl":
                files["bin/atlatl"] = z.read(name)
            elif rel.startswith(("data/lib/atlatl/", "data/share/atlatl/")):
                files[rel[len("data/") :]] = z.read(name)
            else:
                fail(f"unexpected data file in wheel: {name}")
    for w in ["bin/atlatl", *libs, "share/atlatl/LICENSE-THIRD-PARTY"]:
        if w not in files:
            fail(f"wheel is missing {w}")
    if osx and any(f.startswith("lib/atlatl/libsqlite3") for f in files):
        fail("macOS uses the system libsqlite3; the wheel must not ship one")
    ext = f"share/atlatl/extensions/{os.environ.get('DUCKDB_VERSION', 'v1.5.6')}/{platform}/"
    if not any(f.startswith(ext) for f in files):
        fail(f"wheel has no extensions for {platform}")
    files["LICENSE-THIRD-PARTY"] = Path(notices).read_bytes()

    # The binary's (Cargo) version, as the release names it; the wheel's is its PEP 440 form.
    top = f"atlatl-{os.environ.get('VERSION', version)}-{platform}"
    out.mkdir(parents=True, exist_ok=True)
    dest = out / f"{top}.tar.gz"
    mtime = int(os.environ.get("SOURCE_DATE_EPOCH", time.time()))
    with tarfile.open(dest, "w:gz") as t:
        dirs = sorted({str(Path(f).parent) for f in files} - {"."})
        for d in [top, *(f"{top}/{p}" for p in _with_parents(dirs))]:
            info = tarfile.TarInfo(d)
            info.type, info.mode, info.mtime = tarfile.DIRTYPE, 0o755, mtime
            t.addfile(info)
        for name in sorted(files):
            info = tarfile.TarInfo(f"{top}/{name}")
            info.size, info.mtime = len(files[name]), mtime
            info.mode = 0o755 if name in ("bin/atlatl", *libs) else 0o644
            t.addfile(info, io.BytesIO(files[name]))
    print(dest)


def _with_parents(dirs: list[str]) -> list[str]:
    out: set[str] = set()
    for d in dirs:
        p = Path(d)
        out.update(str(x) for x in [p, *p.parents] if str(x) != ".")
    return sorted(out)


if __name__ == "__main__":
    main()
