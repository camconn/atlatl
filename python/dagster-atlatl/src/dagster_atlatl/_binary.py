"""Locating the bundled atlatl binary and its DuckDB extension directory."""

from __future__ import annotations

import os
import shutil
import subprocess
import sysconfig
from importlib import metadata
from pathlib import Path

from packaging.version import Version

BIN_ENV = "ATLATL_BIN"
EXTENSION_DIR_ENV = "ATLATL_EXTENSION_DIR"
DIST = "dagster-atlatl"
_EXE = "atlatl.exe" if os.name == "nt" else "atlatl"
_EXT_PARTS = ("share", "atlatl", "extensions")


class AtlatlBinaryError(RuntimeError):
    """The atlatl binary is missing, unusable, or doesn't match this package."""


def package_version() -> str:
    try:
        return metadata.version(DIST)
    except metadata.PackageNotFoundError:
        return "0.0.0+unknown"


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


def binary_path() -> Path:
    """`ATLATL_BIN`, else the binary installed by this wheel, else `atlatl` on PATH."""
    override = os.environ.get(BIN_ENV)
    if override:
        p = Path(override)
        if not p.is_file():
            raise AtlatlBinaryError(f"{BIN_ENV}={override} is not a file")
        return p
    for bundled in (_installed(("bin", _EXE)), Path(sysconfig.get_path("scripts")) / _EXE):
        if bundled is not None and bundled.is_file():
            return bundled
    found = shutil.which(_EXE)
    if found:
        return Path(found)
    raise AtlatlBinaryError(
        f"atlatl binary not found: set {BIN_ENV} or install a platform wheel of dagster-atlatl"
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


def binary_version(binary: Path | None = None) -> str:
    """The version reported by `atlatl --version` (`atlatl <version>`)."""
    exe = binary or binary_path()
    try:
        out = subprocess.run(
            [str(exe), "--version"], capture_output=True, text=True, check=True, timeout=30
        ).stdout
    except (OSError, subprocess.SubprocessError) as e:
        raise AtlatlBinaryError(f"running {exe} --version failed: {e}") from e
    parts = out.split()
    if len(parts) < 2 or parts[0] != "atlatl":
        raise AtlatlBinaryError(f"unexpected `{exe} --version` output: {out!r}")
    return parts[1]


def check_binary_version(binary: Path | None = None) -> str:
    """Package and binary versions must match exactly, unless `ATLATL_BIN` overrides the binary."""
    exe = binary or binary_path()
    found = binary_version(exe)
    expected = package_version()
    # The binary prints Cargo's SemVer (0.1.0-rc.1), the package has its PEP 440 form (0.1.0rc1).
    if os.environ.get(BIN_ENV) is None and Version(found) != Version(expected):
        raise AtlatlBinaryError(
            f"atlatl binary {exe} is version {found}, but dagster-atlatl is {expected}"
        )
    return found
