#!/usr/bin/env python3
"""Smoke-test an installed atlatl prefix (a venv with the wheel, or an unpacked tarball).

Checks `bin/atlatl --version`, then loads the bundled `lib/atlatl/libduckdb` through its C API
and LOADs every bundled extension from `share/atlatl/extensions` with installs and autoloading
disabled (so nothing can be downloaded), including a DuckLake write. Then `bin/atlatl` itself
attaches lakes that need every bundled extension, with `ATLATL_EXTENSION_DIR` unset, `HOME`
elsewhere (no `~/.duckdb`) and HTTP proxied to a dead port, so it must find
`share/atlatl/extensions` next to itself and stay offline. On Linux `bin/atlatl` must resolve
`libsqlite3.so.0` to the bundled `lib/atlatl/` copy (macOS: the system `/usr/lib/libsqlite3.dylib`),
and a lake -> SQLite serving replication bootstraps and then applies a change. CI also runs it
without a network.

Usage: smoke-test-prefix.py <prefix> <version> <platform> [--package[=dbt_atlatl]]
`--package` also checks that `dagster_atlatl` (imported from this interpreter) resolves the
binary and extension directory inside <prefix>; `--package=dbt_atlatl` checks `dbt_atlatl`.
"""

from __future__ import annotations

import ctypes
import os
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import closing
from pathlib import Path

EXTENSIONS = ("ducklake", "postgres_scanner", "httpfs", "aws")
DUCKDB_VERSION = "v1.5.6"


def fail(msg: str) -> None:
    sys.exit(f"smoke-test: {msg}")


def check_version(prefix: Path, version: str) -> None:
    out = subprocess.run(
        [str(prefix / "bin" / "atlatl"), "--version"], capture_output=True, text=True, check=True
    ).stdout.strip()
    if out != f"atlatl {version}":
        fail(f"`atlatl --version` printed {out!r}, want 'atlatl {version}'")
    print(out)


class DuckDB:
    def __init__(self, lib: Path, config: dict[str, str]) -> None:
        self.c = ctypes.CDLL(str(lib))
        c = self.c
        vp = ctypes.c_void_p
        c.duckdb_create_config.argtypes = [ctypes.POINTER(vp)]
        c.duckdb_set_config.argtypes = [vp, ctypes.c_char_p, ctypes.c_char_p]
        c.duckdb_open_ext.argtypes = [
            ctypes.c_char_p,
            ctypes.POINTER(vp),
            vp,
            ctypes.POINTER(ctypes.c_char_p),
        ]
        c.duckdb_connect.argtypes = [vp, ctypes.POINTER(vp)]
        c.duckdb_query.argtypes = [vp, ctypes.c_char_p, ctypes.c_void_p]
        c.duckdb_result_error.argtypes = [ctypes.c_void_p]
        c.duckdb_result_error.restype = ctypes.c_char_p
        c.duckdb_destroy_result.argtypes = [ctypes.c_void_p]
        c.duckdb_library_version.restype = ctypes.c_char_p
        cfg = vp()
        if c.duckdb_create_config(ctypes.byref(cfg)) != 0:
            fail("duckdb_create_config failed")
        for k, v in config.items():
            if c.duckdb_set_config(cfg, k.encode(), v.encode()) != 0:
                fail(f"duckdb_set_config {k}={v} failed")
        self.db = vp()
        err = ctypes.c_char_p()
        if c.duckdb_open_ext(None, ctypes.byref(self.db), cfg, ctypes.byref(err)) != 0:
            fail(f"duckdb_open_ext: {err.value!r}")
        self.conn = vp()
        if c.duckdb_connect(self.db, ctypes.byref(self.conn)) != 0:
            fail("duckdb_connect failed")

    def version(self) -> str:
        return (self.c.duckdb_library_version() or b"").decode()

    def run(self, sql: str) -> None:
        result = ctypes.create_string_buffer(256)  # sizeof(duckdb_result) is 48 on 64-bit
        state = self.c.duckdb_query(self.conn, sql.encode(), result)
        err = self.c.duckdb_result_error(result)
        self.c.duckdb_destroy_result(result)
        if state != 0:
            fail(f"{sql!r}: {(err or b'').decode()}")


def bundled_duckdb(prefix: Path) -> DuckDB:
    libs = list((prefix / "lib" / "atlatl").glob("libduckdb.*"))
    if len(libs) != 1:
        fail(f"want exactly one libduckdb in {prefix}/lib/atlatl, found {libs}")
    return DuckDB(
        libs[0],
        {
            "extension_directory": str(prefix / "share" / "atlatl" / "extensions"),
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
            "allow_community_extensions": "false",
            "custom_extension_repository": "http://127.0.0.1:9",
        },
    )


def check_extensions(prefix: Path, platform: str) -> None:
    ext_dir = prefix / "share" / "atlatl" / "extensions"
    staged = sorted(
        p.stem for p in (ext_dir / DUCKDB_VERSION / platform).glob("*.duckdb_extension")
    )
    if staged != sorted(EXTENSIONS):
        fail(f"extensions for {platform}: {staged}, want {sorted(EXTENSIONS)}")
    db = bundled_duckdb(prefix)
    if db.version() != DUCKDB_VERSION:
        fail(f"libduckdb is {db.version()}, want {DUCKDB_VERSION}")
    db.run(
        f"SELECT CASE WHEN platform = '{platform}' THEN 1 ELSE error('platform ' || platform) END"
        " FROM pragma_platform()"
    )
    for e in EXTENSIONS:
        db.run(f"LOAD {e}")
    names = ", ".join(f"'{e}'" for e in EXTENSIONS)
    db.run(
        "SELECT error('not loaded from the bundle: ' || string_agg(extension_name, ', '))"
        f" FROM duckdb_extensions() WHERE extension_name IN ({names})"
        f" AND (NOT loaded OR NOT starts_with(install_path, '{ext_dir}'))"
        " HAVING count(*) > 0"
    )
    with tempfile.TemporaryDirectory() as t:
        db.run(f"ATTACH 'ducklake:{t}/m.ducklake' AS lake (DATA_PATH '{t}/data/')")
        db.run("CREATE TABLE lake.x AS SELECT range AS i FROM range(10)")
        db.run("SELECT CASE WHEN count(*) = 10 THEN 1 ELSE error('bad count') END FROM lake.x")
        db.run("DETACH lake")
    print(f"libduckdb {db.version()}: loaded {', '.join(EXTENSIONS)} offline on {platform}")


DEAD = "http://127.0.0.1:9"


def atlatl(
    prefix: Path, project: Path, home: Path, *args: str, **env: str
) -> subprocess.CompletedProcess[str]:
    base = {"PATH": os.environ.get("PATH", ""), "HOME": str(home)}
    for k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "all_proxy"):
        base[k] = DEAD
    return subprocess.run(
        [str(prefix / "bin" / "atlatl"), "--project", str(project), *args],
        capture_output=True,
        text=True,
        env={**base, **env},
        timeout=120,
    )


def check_binary_extensions(prefix: Path) -> None:
    with tempfile.TemporaryDirectory() as t:
        root = Path(t)
        home = root / "home"
        home.mkdir()
        s3 = root / "s3"
        s3.mkdir()
        # ducklake + httpfs + aws. Nothing touches the (dead) endpoint until data is written.
        (s3 / "atlatl.yaml").write_text(
            "version: 1\nstate: {lake: lake}\nconnections:\n  lake:\n    type: ducklake\n"
            f'    catalog: {{type: duckdb, path: "{s3}/meta.ducklake"}}\n'
            '    data_path: "s3://atlatl-smoke/lake/"\n'
            "    storage: {type: s3, provider: config, key_id: smoke-offline-key,"
            " secret: smoke-offline-secret, region: us-east-1,"
            f" endpoint: {DEAD}, url_style: path}}\n"
        )
        r = atlatl(prefix, s3, home, "state", "show")
        if r.returncode != 0 or r.stdout.strip() != "[]":
            fail(f"atlatl state show (s3 lake) exited {r.returncode}: {r.stdout}{r.stderr}")
        pg = root / "pg"
        pg.mkdir()
        # ducklake + postgres: the extension's own connect error proves it loaded.
        (pg / "atlatl.yaml").write_text(
            "version: 1\nstate: {lake: lake}\nconnections:\n  lake:\n    type: ducklake\n"
            "    catalog: {type: postgres, host: 127.0.0.1, port: 9, database: smoke, user: smoke,"
            " password: smoke-offline-pw, sslmode: disable}\n"
            f'    data_path: "{pg}/data/"\n    storage: {{type: local}}\n'
        )
        r = atlatl(prefix, pg, home, "state", "show")
        if r.returncode == 0 or "Unable to connect to Postgres" not in r.stderr:
            fail(f"atlatl state show (postgres catalog) exited {r.returncode}: {r.stderr}")
        # Control: the variable does steer the lookup, so the passes above used the fallback.
        empty = root / "no-extensions"
        empty.mkdir()
        r = atlatl(prefix, s3, home, "state", "show", ATLATL_EXTENSION_DIR=str(empty))
        if r.returncode == 0 or "not found" not in r.stderr:
            fail(f"ATLATL_EXTENSION_DIR={empty} should fail to load: {r.returncode} {r.stderr}")
    print("atlatl: found its bundled extensions with ATLATL_EXTENSION_DIR unset, offline")


def check_sqlite_lib(prefix: Path, platform: str) -> None:
    exe = prefix / "bin" / "atlatl"
    bundled = sorted((prefix / "lib" / "atlatl").glob("libsqlite3*"))
    if platform.startswith("osx"):
        if bundled:
            fail(f"macOS uses the system libsqlite3, but {bundled} is bundled")
        deps = subprocess.run(["otool", "-L", str(exe)], capture_output=True, text=True, check=True)
        if "/usr/lib/libsqlite3.dylib " not in deps.stdout:
            fail(f"{exe} does not link /usr/lib/libsqlite3.dylib:\n{deps.stdout}")
        print("libsqlite3: /usr/lib/libsqlite3.dylib (system)")
        return
    lib = prefix / "lib" / "atlatl" / "libsqlite3.so.0"
    if bundled != [lib] or lib.is_symlink() or not lib.is_file():
        fail(f"want {lib} as the only libsqlite3*, a regular file; found {bundled}")
    env = {k: v for k, v in os.environ.items() if not k.startswith("LD_")}
    trace = subprocess.run(
        [str(exe)],
        capture_output=True,
        text=True,
        check=True,
        env={**env, "LD_TRACE_LOADED_OBJECTS": "1"},
    ).stdout
    found = [
        line.split("=>", 1)[1].split("(", 1)[0].strip()
        for line in trace.splitlines()
        if line.strip().startswith("libsqlite3.so.0 =>")
    ]
    if len(found) != 1 or Path(found[0]).resolve() != lib.resolve():
        fail(f"{exe} resolves libsqlite3.so.0 to {found}, not {lib}:\n{trace}")
    print(f"libsqlite3: {found[0]}")


def check_sqlite_target(prefix: Path) -> None:
    with tempfile.TemporaryDirectory() as t:
        root = Path(t)
        home = root / "home"
        home.mkdir()
        attach = f"ATTACH 'ducklake:{root}/meta.ducklake' AS lake (DATA_PATH '{root}/data/')"
        db = bundled_duckdb(prefix)
        db.run("LOAD ducklake")
        db.run(attach)
        db.run("CREATE SCHEMA lake.src")
        db.run(
            "CREATE TABLE lake.src.item AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)"
        )
        db.run("DETACH lake")
        app = root / "app.db"
        (root / "atlatl.yaml").write_text(
            "version: 1\nconnections:\n  lake:\n    type: ducklake\n"
            f'    catalog: {{type: duckdb, path: "{root}/meta.ducklake"}}\n'
            f'    data_path: "{root}/data/"\n    storage: {{type: local}}\n'
            f'  app: {{type: sqlite, path: "{app}"}}\n'
        )
        (root / "replications").mkdir()
        (root / "replications" / "serve.yaml").write_text(
            "replications:\n  serve:\n    source: lake\n    target: app\n"
            "    defaults: {mode: cdc}\n    streams:\n"
            "      - {id: s/item, from: {table: src.item}, object: item, primary_key: [id]}\n"
        )

        def rows() -> list[tuple[int, str]]:
            with closing(sqlite3.connect(app)) as c:
                return c.execute("SELECT id, name FROM item ORDER BY id").fetchall()

        r = atlatl(prefix, root, home, "cdc", "bootstrap", "serve", "--mode", "reload")
        if r.returncode != 0:
            fail(f"atlatl cdc bootstrap (sqlite) exited {r.returncode}: {r.stdout}{r.stderr}")
        if rows() != [(1, "a"), (2, "b")]:
            fail(f"sqlite after bootstrap: {rows()}")
        db.run(attach)
        db.run("UPDATE lake.src.item SET name = 'B' WHERE id = 2")
        db.run("INSERT INTO lake.src.item VALUES (3, 'c')")
        db.run("DETACH lake")
        r = atlatl(prefix, root, home, "run")
        if r.returncode != 0:
            fail(f"atlatl run (sqlite) exited {r.returncode}: {r.stdout}{r.stderr}")
        if rows() != [(1, "a"), (2, "B"), (3, "c")]:
            fail(f"sqlite after run: {rows()}")
    print("atlatl: served a lake table into SQLite (bootstrap, then a change), offline")


def check_dbt_package(prefix: Path, version: str) -> None:
    from importlib import metadata

    import dbt_atlatl  # type: ignore[import-not-found]  # what dbt-duckdb imports
    from dbt_atlatl import _binary  # type: ignore[import-not-found]

    exe = _binary.binary_path().resolve()
    if exe != (prefix / "bin" / "atlatl").resolve():
        fail(f"dbt_atlatl binary_path() is {exe}, not under {prefix}")
    ext = _binary.extension_dir()
    if ext is None or ext.resolve() != (prefix / "share" / "atlatl" / "extensions").resolve():
        fail(f"dbt_atlatl extension_dir() is {ext}")
    from packaging.version import Version

    # PEP 440 for the package, Cargo's SemVer for the binary: 0.1.0rc1 is 0.1.0-rc.1.
    if Version(metadata.version("dbt-atlatl")) != Version(version):
        fail(f"dbt-atlatl is {metadata.version('dbt-atlatl')}, not {version}")
    print(f"dbt_atlatl {version}: {exe}, {ext} ({dbt_atlatl.Plugin.__name__})")


def check_package(prefix: Path, version: str) -> None:
    import dagster_atlatl  # type: ignore[import-not-found]

    exe = dagster_atlatl.binary_path().resolve()
    if exe != (prefix / "bin" / "atlatl").resolve():
        fail(f"dagster_atlatl.binary_path() is {exe}, not under {prefix}")
    ext = dagster_atlatl.extension_dir()
    if ext is None or ext.resolve() != (prefix / "share" / "atlatl" / "extensions").resolve():
        fail(f"dagster_atlatl.extension_dir() is {ext}")
    if dagster_atlatl.check_binary_version() != version:
        fail("binary and package versions differ")
    print(f"dagster_atlatl {version}: {exe}, {ext}")


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--package")]
    if len(args) != 3:
        fail("usage: smoke-test-prefix.py <prefix> <version> <platform> [--package[=dbt_atlatl]]")
    prefix, version, platform = Path(args[0]).resolve(), args[1], args[2]
    check_version(prefix, version)
    check_sqlite_lib(prefix, platform)
    check_extensions(prefix, platform)
    check_binary_extensions(prefix)
    check_sqlite_target(prefix)
    if "--package" in sys.argv:
        check_package(prefix, version)
    if "--package=dbt_atlatl" in sys.argv:
        check_dbt_package(prefix, version)


if __name__ == "__main__":
    main()
