#!/bin/sh
# Generate LICENSE-THIRD-PARTY for the release artefacts (wheel and tarball):
#   - libduckdb, the bundled extensions and (Linux) libsqlite3, from the checked-in licenses/ tree
#     (scripts/fetch-license-texts.sh);
#   - the Rust crates linked into `atlatl`, via cargo-about.
# Usage: third-party-licenses.sh [output]   (default: <repo>/target/LICENSE-THIRD-PARTY)
# CARGO_ABOUT names the cargo-about binary; otherwise PATH, then a prebuilt copy in target/tools
# (downloaded on first use).
set -eu

root="$(cd "$(dirname "$0")/.." && pwd)"
out="${1:-$root/target/LICENSE-THIRD-PARTY}"
about_version=0.9.2

die() {
    echo "third-party-licenses: $*" >&2
    exit 1
}

find_about() {
    if [ -n "${CARGO_ABOUT:-}" ]; then
        echo "$CARGO_ABOUT"
        return
    fi
    if command -v cargo-about >/dev/null 2>&1; then
        command -v cargo-about
        return
    fi
    case "$(uname -s)/$(uname -m)" in
        Linux/x86_64) triple=x86_64-unknown-linux-musl ;;
        Linux/aarch64 | Linux/arm64) triple=aarch64-unknown-linux-musl ;;
        Darwin/arm64) triple=aarch64-apple-darwin ;;
        *) die "no prebuilt cargo-about for $(uname -s)/$(uname -m); set CARGO_ABOUT" ;;
    esac
    dir="$root/target/tools/cargo-about-$about_version-$triple"
    if [ ! -x "$dir/cargo-about" ]; then
        mkdir -p "$root/target/tools"
        curl -fsSL --retry 3 \
            "https://github.com/EmbarkStudios/cargo-about/releases/download/$about_version/cargo-about-$about_version-$triple.tar.gz" |
            tar -xz -C "$root/target/tools" || die "cargo-about download failed"
    fi
    echo "$dir/cargo-about"
}

about="$(find_about)"
rust="$(mktemp)"
trap 'rm -f "$rust"' EXIT INT TERM
"$about" generate --locked \
    -c "$root/licenses/about.toml" \
    -m "$root/crates/atlatl-cli/Cargo.toml" \
    -o "$rust" \
    "$root/licenses/about.hbs" || die "cargo-about failed"

mkdir -p "$(dirname "$out")"
{
    cat <<'EOF'
Third-party software in atlatl
==============================

atlatl bundles the official DuckDB v1.5.6 library (lib/atlatl/) and the DuckDB extensions
ducklake, postgres_scanner, httpfs and aws (share/atlatl/extensions/). They are MIT licensed and
statically link the third-party components below, whose licence texts and NOTICE files follow.
The Linux builds also bundle SQLite (lib/atlatl/libsqlite3.so.0), which is in the public domain
(sqlite/LICENSE.md below); macOS builds use the system SQLite. The atlatl binary itself links
the Rust crates listed in part 2.

DuckDB is a trademark of the DuckDB Foundation.

================================================================================
Part 1: DuckDB, its extensions, the components they link, and SQLite
================================================================================
EOF
    (cd "$root/licenses" && find . -type f ! -name 'about.*' | sed 's|^\./||' | LC_ALL=C sort) |
        while read -r f; do
            printf '\n--------------------------------------------------------------------------------\n%s\n\n' "$f"
            cat "$root/licenses/$f"
        done
    cat <<'EOF'

================================================================================
Part 2: Rust crates
================================================================================
EOF
    cat "$rust"
} >"$out"
echo "wrote $out"
