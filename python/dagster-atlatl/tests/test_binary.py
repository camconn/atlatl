from __future__ import annotations

import stat
from collections.abc import Callable
from importlib import metadata
from pathlib import Path

import pytest

import dagster_atlatl
from dagster_atlatl import _binary


def paths_at(root: Path) -> Callable[[str], str]:
    def get_path(_name: str) -> str:
        return str(root)

    return get_path


def fake_binary(tmp_path: Path, output: str) -> Path:
    exe = tmp_path / "atlatl"
    exe.write_text(f"#!/bin/sh\necho '{output}'\n")
    exe.chmod(exe.stat().st_mode | stat.S_IXUSR)
    return exe


def not_installed(_name: str) -> metadata.Distribution:
    raise metadata.PackageNotFoundError(_name)


@pytest.fixture(autouse=True)
def isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(_binary.metadata, "distribution", not_installed)
    monkeypatch.delenv(_binary.BIN_ENV, raising=False)
    monkeypatch.delenv(_binary.EXTENSION_DIR_ENV, raising=False)
    empty = tmp_path / "prefix"
    empty.mkdir()
    monkeypatch.setattr(_binary.sysconfig, "get_path", paths_at(empty))
    monkeypatch.setenv("PATH", str(tmp_path / "nowhere"))


def test_override_wins(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    exe = fake_binary(tmp_path, "atlatl 9.9.9")
    monkeypatch.setenv(_binary.BIN_ENV, str(exe))
    assert dagster_atlatl.binary_path() == exe
    assert dagster_atlatl.check_binary_version() == "9.9.9"


def test_override_must_exist(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(_binary.BIN_ENV, str(tmp_path / "missing"))
    with pytest.raises(dagster_atlatl.AtlatlBinaryError, match="is not a file"):
        dagster_atlatl.binary_path()


def test_missing_binary() -> None:
    with pytest.raises(dagster_atlatl.AtlatlBinaryError, match="not found"):
        dagster_atlatl.binary_path()


def test_bundled_script_then_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    bundled = fake_binary(scripts, "atlatl 0.1.0")
    monkeypatch.setattr(_binary.sysconfig, "get_path", paths_at(scripts))
    assert dagster_atlatl.binary_path() == bundled

    on_path = tmp_path / "bin"
    on_path.mkdir()
    fake_binary(on_path, "atlatl 0.1.0")
    bundled.unlink()
    monkeypatch.setenv("PATH", str(on_path))
    assert dagster_atlatl.binary_path() == on_path / "atlatl"


def test_version_mismatch_is_an_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    exe = fake_binary(tmp_path, "atlatl 0.0.1")
    monkeypatch.setattr(_binary, "package_version", lambda: "0.1.0")
    with pytest.raises(dagster_atlatl.AtlatlBinaryError, match=r"is version 0\.0\.1"):
        dagster_atlatl.check_binary_version(exe)


def test_unexpected_version_output(tmp_path: Path) -> None:
    exe = fake_binary(tmp_path, "other 1.5.20")
    with pytest.raises(dagster_atlatl.AtlatlBinaryError, match="unexpected"):
        dagster_atlatl.binary_version(exe)


def test_extension_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    assert dagster_atlatl.extension_dir() is None
    data = tmp_path / "data"
    ext = data / "share" / "atlatl" / "extensions"
    ext.mkdir(parents=True)
    monkeypatch.setattr(_binary.sysconfig, "get_path", paths_at(data))
    assert dagster_atlatl.extension_dir() == ext
    monkeypatch.setenv(_binary.EXTENSION_DIR_ENV, "/opt/ext")
    assert dagster_atlatl.extension_dir() == Path("/opt/ext")


def installed_wheel(prefix: Path) -> metadata.Distribution:
    """A wheel install as RECORD describes it: `.data/scripts` → bin, `.data/data` → prefix."""
    site = prefix / "lib" / "python3.12" / "site-packages"
    info = site / "dagster_atlatl-0.1.0.dist-info"
    info.mkdir(parents=True)
    (info / "METADATA").write_text("Metadata-Version: 2.1\nName: dagster-atlatl\nVersion: 0.1.0\n")
    rel = [
        "dagster_atlatl/__init__.py",
        "../../../bin/atlatl",
        "../../../lib/atlatl/libduckdb.so",
        "../../../share/atlatl/extensions/v1.5.6/linux_amd64/ducklake.duckdb_extension",
    ]
    for r in rel:
        f = (site / r).resolve()
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text("")
    fake_binary(prefix / "bin", "atlatl 0.1.0")
    (info / "RECORD").write_text("".join(f"{r},,\n" for r in rel))
    return metadata.PathDistribution(info)


def test_resolves_from_record(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    prefix = tmp_path / "user"
    dist = installed_wheel(prefix)

    def distribution(_name: str) -> metadata.Distribution:
        return dist

    monkeypatch.setattr(_binary.metadata, "distribution", distribution)
    # A different scheme's scripts dir also has an atlatl; RECORD wins.
    other = tmp_path / "other"
    other.mkdir()
    fake_binary(other, "atlatl 0.0.1")
    monkeypatch.setattr(_binary.sysconfig, "get_path", paths_at(other))
    assert dagster_atlatl.binary_path() == (prefix / "bin" / "atlatl").resolve()
    assert dagster_atlatl.extension_dir() == (prefix / "share" / "atlatl" / "extensions").resolve()
