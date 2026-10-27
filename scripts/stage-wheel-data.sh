#!/bin/sh
# Stage one platform's libduckdb, DuckDB extensions and third-party notices into the prefix tree
# the wheel installs (maturin `data` dir) and the tarball ships (spec 09 §1):
#   lib/atlatl/libduckdb.{so,dylib}
#   lib/atlatl/libsqlite3.so.0 (Linux only; macOS uses the system /usr/lib/libsqlite3.dylib)
#   share/atlatl/extensions/<duckdb version>/<platform>/<ext>.duckdb_extension
#   share/atlatl/LICENSE-THIRD-PARTY
# Usage: stage-wheel-data.sh <platform> <libduckdb path> [LICENSE-THIRD-PARTY]
# STAGE_DIR overrides the output (default python/dagster-atlatl/atlatl.data/data). Extensions come
# from $ATLATL_EXTENSIONS_OUT (default <repo>/extensions, as scripts/fetch-duckdb-extensions.sh).
# SQLITE_LIB is the libsqlite3.so.0 to ship (default: the one ldconfig resolves, i.e. the build
# image's, which the binary linked against); symlinks are resolved.
set -eu

die() {
    echo "stage-wheel-data: $*" >&2
    exit 1
}

[ "$#" -ge 2 ] || die "usage: stage-wheel-data.sh <platform> <libduckdb path> [LICENSE-THIRD-PARTY]"
platform="$1"
lib="$2"
notices="${3:-}"
root="$(cd "$(dirname "$0")/.." && pwd)"
version="${DUCKDB_VERSION:-v1.5.6}"
src="${ATLATL_EXTENSIONS_OUT:-$root/extensions}/$version/$platform"
data="${STAGE_DIR:-$root/python/dagster-atlatl/atlatl.data/data}"
extensions="${ATLATL_EXTENSIONS:-ducklake postgres_scanner httpfs aws}"

abi=""
case "$platform" in
    linux_amd64) want=libduckdb.so abi=x86-64 ;;
    linux_arm64) want=libduckdb.so abi=AArch64 ;;
    osx_arm64) want=libduckdb.dylib ;;
    *) die "unknown platform: $platform" ;;
esac
sqlite=""
if [ -n "$abi" ]; then
    sqlite="${SQLITE_LIB:-$(PATH="$PATH:/sbin:/usr/sbin" ldconfig -p 2>/dev/null |
        sed -n "s/^[[:space:]]*libsqlite3\.so\.0 (.*$abi.*) => //p" | head -n 1)}"
    [ -n "$sqlite" ] || die "ldconfig finds no $abi libsqlite3.so.0; install the SQLite library or set SQLITE_LIB"
    [ -f "$sqlite" ] || die "no such file: $sqlite"
fi
[ -f "$lib" ] || die "no such file: $lib"
[ "$(basename "$lib")" = "$want" ] || die "$platform needs $want, got $(basename "$lib")"
[ -z "$notices" ] || [ -f "$notices" ] || die "no such file: $notices"

rm -rf "$data/lib/atlatl" "$data/share/atlatl"
ext="$data/share/atlatl/extensions/$version/$platform"
mkdir -p "$data/lib/atlatl" "$ext"
cp "$lib" "$data/lib/atlatl/$want"
[ -z "$sqlite" ] || cp -L "$sqlite" "$data/lib/atlatl/libsqlite3.so.0"
for name in $extensions; do
    f="$src/$name.duckdb_extension"
    [ -s "$f" ] || die "missing $f (run scripts/fetch-duckdb-extensions.sh $platform)"
    cp "$f" "$ext/"
done
[ -z "$notices" ] || cp "$notices" "$data/share/atlatl/LICENSE-THIRD-PARTY"
du -sh "$data/lib/atlatl" "$ext"
