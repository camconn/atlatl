# Spike S6: packaging

Date: 2026-09-28. Proof: the S6 spike (code since removed; superseded by `.github/workflows/release.yml` and `scripts/`). It had a binary shaped like the release `atlatl` (`--version` and `check-extensions`), a maturin `bindings = "bin"` project in `python/`, and `stage-extensions.sh`.

## Verdict

**Confirmed for Linux x86_64 and aarch64. macOS arm64 is designed but must build on a macOS runner.**

- DuckDB is linked **dynamically against the official `libduckdb` from the GitHub release** (`.github/workflows/release.yml`).
- The wheel carries one fixed prefix layout, which is the same layout as the standalone tarball. The binary finds everything relative to itself.

```
<name>-<ver>.data/scripts/atlatl                              → <prefix>/bin/atlatl            (the real binary; no Python launcher)
<name>-<ver>.data/data/lib/atlatl/libduckdb.{so,dylib}        → <prefix>/lib/atlatl/
<name>-<ver>.data/data/share/atlatl/extensions/v1.5.3/<plat>/ → <prefix>/share/atlatl/extensions/
```

- `RUNPATH` is `$ORIGIN/deps:$ORIGIN:$ORIGIN/../lib/atlatl`, set in `.cargo/config.toml`. The first two serve `target/` in development; release builds should keep only `$ORIGIN/../lib/atlatl` (M0.20).
- The library lives in its own `lib/atlatl/` subdirectory, so a system-wide pip install can never shadow, or be shadowed by, another `libduckdb`. This machine had a distro `libduckdb.so` in `/usr/lib64` that an rpath-less binary silently picked up.
- The extension directory is `ATLATL_EXTENSION_DIR`, else `<exe>/../share/atlatl/extensions`. `dagster_atlatl.extension_dir()` resolves the same path through `sysconfig.get_path("data")`.

## 1. Results

| Target | Built with | Wheel | Verified |
|---|---|---|---|
| `manylinux_2_28_x86_64` | maturin in `quay.io/pypa/manylinux_2_28_x86_64` (1m40s incl. toolchain) | **66.0 MiB** | installed with `uv pip` into a clean Python 3.12 venv. `bin/atlatl-s6` is an ELF binary; `ldd` resolves `libduckdb.so` to `<venv>/lib/atlatl/`; `--version` prints DuckDB v1.5.3; `check-extensions` under `unshare -rn` (no network) loads ducklake, postgres, httpfs and aws and writes a DuckLake table |
| `manylinux_2_28_aarch64` | `maturin build --target aarch64-unknown-linux-gnu --zig` on the x86_64 host (1m27s) | **59.0 MiB** | the wheel's prefix tree, run under `qemu-aarch64` with an arm64 Debian sysroot: `--version`, then `check-extensions` offline with the official `linux_arm64` extensions |
| `macosx_11_0_arm64` | not buildable on Linux: zig can't link `CoreFoundation.framework` without Apple's SDK | est. ~65 MiB after thinning | the official `libduckdb.dylib` is universal (108 MB, x86_64 + arm64) with install name `@rpath/libduckdb.dylib`, so `@executable_path/../lib/atlatl` works. CI builds on `macos-14` and runs `lipo -thin arm64` before staging |

`auditwheel show` confirms the x86_64 binary's symbol needs, dominated by `libduckdb.so`'s: `GLIBC_2.28` at most, and `GLIBCXX_3.4.22` / `CXXABI_1.3.11`. That constrains the tag to `manylinux_2_28_x86_64`, which matches the official extensions' own baseline (they're built on manylinux_2_28).

Wheel contents for x86_64 (uncompressed → compressed): libduckdb.so 67.1 → 22.7 MiB, postgres_scanner 39.6 → 14.2, ducklake 34.2 → 12.1, aws 27.1 → 9.2, httpfs 20.5 → 7.2, and the binary 1.2 → 0.5. The installed size is about 190 MB. That is well under PyPI's default 100 MB per-file limit, compressed.

## 2. Findings

- **maturin's default repair doesn't fit.** With `--auditwheel repair`, maturin copies `libduckdb.so` into `<pkg>.libs/` and turns `bin/atlatl` into a Python launcher. The launcher `execv`s `site-packages/<pkg>.scripts/atlatl`, so signals and fds survive, but every call pays a Python start, and the binary can't find `share/` relative to itself. The build therefore uses **`--auditwheel skip`** and stages `lib/atlatl/` itself. `auditwheel show` is run as a check instead.
- **The Linux release build must use glibc 2.28.** The `manylinux_2_28` container gives that. `--zig` targeting `*-linux-gnu.2.28` is also fine now that DuckDB isn't compiled: the Rust code only uses DuckDB's C API, so there are no C++ runtime concerns. (With the earlier `cc` bundled build, zig's libc++ would have been ABI-incompatible with the official extensions, and so would manylinux2014's old C++ ABI.)
- **macOS needs a macOS runner.** There's no SDK cross-link from Linux.
- **The duckdb crate's own download** (`DUCKDB_DOWNLOAD_LIB=1`) happens at build time, from GitHub. Offline or hermetic builds set `DUCKDB_LIB_DIR` to a pre-fetched copy instead.

## 3. Spec changes

- 09 §1: the wheel layout above, sizes, `--auditwheel skip` plus `auditwheel show`, and a macOS runner for arm64.
- 01 §2 and §4: DuckDB is linked dynamically against the official release library (the old "fallback" is now the design).
