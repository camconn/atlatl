#!/bin/sh
# Download the DuckDB extensions atlatl loads into DuckDB's directory layout:
#   $ATLATL_EXTENSIONS_OUT/<version>/<platform>/<name>.duckdb_extension
# ATLATL_EXTENSIONS_OUT defaults to $ATLATL_EXTENSION_DIR, then <repo>/extensions.
# Usage: fetch-duckdb-extensions.sh [platform...]   (default: the host's)
# Platforms: linux_amd64 linux_arm64 osx_arm64. Files already present are skipped.
# ATLATL_EXTENSIONS overrides the space-separated extension list.
set -eu

version="${DUCKDB_VERSION:-v1.5.6}"
base_url="${DUCKDB_EXTENSIONS_URL:-https://extensions.duckdb.org}"
root="$(cd "$(dirname "$0")/.." && pwd)"
out="${ATLATL_EXTENSIONS_OUT:-${ATLATL_EXTENSION_DIR:-$root/extensions}}"
extensions="${ATLATL_EXTENSIONS:-ducklake postgres_scanner httpfs aws delta}"

die() {
    echo "fetch-duckdb-extensions: $*" >&2
    exit 1
}

host_platform() {
    case "$(uname -s)/$(uname -m)" in
        Linux/x86_64 | Linux/amd64) echo linux_amd64 ;;
        Linux/aarch64 | Linux/arm64) echo linux_arm64 ;;
        Darwin/arm64) echo osx_arm64 ;;
        *) die "unsupported host $(uname -s)/$(uname -m); pass a platform" ;;
    esac
}

command -v curl >/dev/null 2>&1 || die "curl not found"
command -v gzip >/dev/null 2>&1 || die "gzip not found"

[ "$#" -gt 0 ] || set -- "$(host_platform)"

for platform in "$@"; do
    case "$platform" in
        linux_amd64 | linux_arm64 | osx_arm64) ;;
        *) die "unknown platform: $platform" ;;
    esac
    dir="$out/$version/$platform"
    mkdir -p "$dir"
    for name in $extensions; do
        dest="$dir/$name.duckdb_extension"
        if [ -s "$dest" ]; then
            echo "skip  $dest"
            continue
        fi
        url="$base_url/$version/$platform/$name.duckdb_extension.gz"
        tmp="$dest.tmp.$$"
        trap 'rm -f "$tmp" "$tmp.gz"' EXIT INT TERM
        echo "fetch $url"
        curl -fsSL --retry 3 --retry-connrefused -o "$tmp.gz" "$url" || die "download failed: $url"
        gzip -dc "$tmp.gz" >"$tmp" || die "gunzip failed: $url"
        [ -s "$tmp" ] || die "empty extension: $url"
        mv "$tmp" "$dest"
        rm -f "$tmp.gz"
    done
done
