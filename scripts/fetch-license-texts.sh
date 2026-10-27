#!/bin/sh
# Re-fetch the licence and NOTICE texts of the C/C++ code shipped in the wheel and tarball
# (libduckdb and its extensions, at the pinned versions, and the
# Linux libsqlite3) into licenses/. The result is checked in; run this only when a pin changes.
set -eu

root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/licenses"
gh=https://raw.githubusercontent.com

duckdb=v1.5.6
ducklake=ac7595b0a1305bea3d4cfaca763b0ce964c763a2
postgres_ext=318dabb2474fc3789b0199301a2e661e19c1b4fc
httpfs=4bc690dba4496c765777a0269d48fdbaff7cdc11
aws_ext=28c853c084a6e3acd36d7b8018c42438bb8c5a33
# SQLite is public domain in every release; LICENSE.md postdates the build images' 3.26.
sqlite=version-3.50.4

# <dest under licenses/> <url>
manifest() {
    cat <<EOF
duckdb/LICENSE $gh/duckdb/duckdb/$duckdb/LICENSE
EOF
    for d in brotli concurrentqueue fast_float fastpforlib fmt fsst httplib hyperloglog jaro_winkler \
        jemalloc libpg_query lz4 mbedtls miniz parquet pcg pdqsort re2 ska_sort skiplist snappy \
        snowball tdigest utf8proc vergesort yyjson zstd; do
        echo "duckdb/third_party/$d/LICENSE $gh/duckdb/duckdb/$duckdb/third_party/$d/LICENSE"
    done
    cat <<EOF
duckdb/third_party/tdigest/NOTICES $gh/duckdb/duckdb/$duckdb/third_party/tdigest/NOTICES
duckdb/third_party/thrift/LICENSE $gh/duckdb/duckdb/$duckdb/third_party/thrift/thrift/LICENSE
duckdb/third_party/thrift/NOTICE $gh/apache/thrift/v0.22.0/NOTICE
duckdb/third_party/parquet/NOTICE $gh/apache/parquet-format/apache-parquet-format-2.11.0/NOTICE
ducklake/LICENSE $gh/duckdb/ducklake/$ducklake/LICENSE
ducklake/croaring/LICENSE $gh/RoaringBitmap/CRoaring/v4.5.0/LICENSE
postgres_scanner/LICENSE $gh/duckdb/duckdb-postgres/$postgres_ext/LICENSE
postgres_scanner/libpq/COPYRIGHT $gh/postgres/postgres/REL_18_3/COPYRIGHT
httpfs/LICENSE $gh/duckdb/duckdb-httpfs/$httpfs/LICENSE
aws/LICENSE $gh/duckdb/duckdb-aws/$aws_ext/LICENSE
aws/aws-sdk-cpp/LICENSE $gh/aws/aws-sdk-cpp/1.11.702/LICENSE
aws/aws-sdk-cpp/NOTICE $gh/aws/aws-sdk-cpp/1.11.702/NOTICE.txt
aws/aws-crt-cpp/LICENSE $gh/awslabs/aws-crt-cpp/v0.36.0/LICENSE
aws/aws-crt-cpp/NOTICE $gh/awslabs/aws-crt-cpp/v0.36.0/NOTICE
aws/s2n-tls/LICENSE $gh/aws/s2n-tls/v1.6.2/LICENSE
aws/s2n-tls/NOTICE $gh/aws/s2n-tls/v1.6.2/NOTICE
common/openssl/LICENSE.txt $gh/openssl/openssl/openssl-3.6.0/LICENSE.txt
common/curl/COPYING $gh/curl/curl/curl-8_17_0/COPYING
common/zlib/LICENSE $gh/madler/zlib/v1.3.1/LICENSE
sqlite/LICENSE.md $gh/sqlite/sqlite/$sqlite/LICENSE.md
EOF
    # aws-c-* at the vcpkg port versions (vcpkg 84bab45d). All Apache-2.0 (text as aws-sdk-cpp);
    # only their NOTICEs differ. aws-checksums 0.2.7 has no NOTICE.
    for lib in aws-c-auth:0.9.4 aws-c-cal:0.9.13 aws-c-common:0.12.6 aws-c-compression:0.3.1 \
        aws-c-event-stream:0.5.9 aws-c-http:0.10.7 aws-c-io:0.24.0 aws-c-mqtt:0.13.3 \
        aws-c-s3:0.11.3 aws-c-sdkutils:0.2.4; do
        name=${lib%%:*}
        echo "aws/$name/NOTICE $gh/awslabs/$name/v${lib##*:}/NOTICE"
    done
}

command -v curl >/dev/null 2>&1 || { echo "fetch-license-texts: curl not found" >&2; exit 1; }
manifest | while read -r dest url; do
    mkdir -p "$(dirname "$out/$dest")"
    curl -fsSL --retry 3 -o "$out/$dest" "$url" || { echo "fetch-license-texts: $url" >&2; exit 1; }
    echo "$dest"
done
