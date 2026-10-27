"""The atlatl binary and DuckDB extensions bundled in this wheel (as in dagster-atlatl)."""

from __future__ import annotations

import os
import shutil
import sysconfig
from importlib import metadata
from pathlib import Path

BIN_ENV = "ATLATL_BIN"
EXTENSION_DIR_ENV = "ATLATL_EXTENSION_DIR"
DIST = "dbt-atlatl"
_EXE = "atlatl.exe" if os.name == "nt" else "atlatl"
_EXT_PARTS = ("share", "atlatl", "extensions")


class AtlatlBinaryError(RuntimeError):
    """The atlatl binary is missing or unusable."""


def _installed(parts: tuple[str, ...]) -> Path | None:
    """The installed path whose RECORD entry contains `parts`, cut after them.

    The wheel's `.data/{scripts,data}` land wherever the install scheme puts them (venv, `--user`,
    `--prefix`, `--target`); RECORD knows where, `sysconfig`'s default scheme may not.
    """
    try:
        files = metadata.distribution(DIST).files or []
    except metadata.PackageNotFoundError:
        return None
    n = len(parts)
    for f in files:
        fp = f.parts
        for i in range(len(fp) - n + 1):
            if fp[i : i + n] == parts:
                located = Path(str(f.locate())).resolve()
                return located.parents[len(fp) - i - n - 1] if i + n < len(fp) else located
    return None


def binary_path(configured: str | None = None) -> Path:
    """`ATLATL_BIN`, else `configured`, else the binary this wheel installed, else PATH."""
    for candidate in (os.environ.get(BIN_ENV), configured):
        if candidate:
            p = Path(candidate)
            if not p.is_file():
                raise AtlatlBinaryError(f"atlatl binary {candidate} is not a file")
            return p
    for bundled in (_installed(("bin", _EXE)), Path(sysconfig.get_path("scripts")) / _EXE):
        if bundled is not None and bundled.is_file():
            return bundled
    found = shutil.which(_EXE)
    if found:
        return Path(found)
    raise AtlatlBinaryError(
        f"atlatl binary not found: set {BIN_ENV}, the plugin's `binary`, "
        "or install a platform wheel of dbt-atlatl"
    )


def extension_dir() -> Path | None:
    """`ATLATL_EXTENSION_DIR`, else `<prefix>/share/atlatl/extensions` from the wheel.

    `None` lets the binary apply its own default (`<exe>/../share/atlatl/extensions`).
    """
    override = os.environ.get(EXTENSION_DIR_ENV)
    if override:
        return Path(override)
    for bundled in (_installed(_EXT_PARTS), Path(sysconfig.get_path("data")).joinpath(*_EXT_PARTS)):
        if bundled is not None and bundled.is_dir():
            return bundled
    return None
