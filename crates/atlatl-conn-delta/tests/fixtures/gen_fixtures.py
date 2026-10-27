# Builds the Delta test fixtures with delta-rs, except `dv_spark` (delta-rs test data `table-with-dv-small`, Apache-2.0). Usage:
#   uv run --with deltalake --with pyarrow python gen_fixtures.py .
import datetime as dt
import decimal
import shutil
import sys
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

out = Path(sys.argv[1])
GENERATED = ["types", "partitioned", "evolved", "dv_attempt", "merged"]
for name in GENERATED:
    shutil.rmtree(out / name, ignore_errors=True)
out.mkdir(parents=True, exist_ok=True)
UTC = dt.timezone.utc

# 1. Every type we care about, unpartitioned, one version.
types_schema = pa.schema(
    [
        ("id", pa.int64()),
        ("i32", pa.int32()),
        ("i16", pa.int16()),
        ("i8", pa.int8()),
        ("f32", pa.float32()),
        ("f64", pa.float64()),
        ("b", pa.bool_()),
        ("s", pa.string()),
        ("bin", pa.binary()),
        ("d", pa.date32()),
        ("dec_10_2", pa.decimal128(10, 2)),
        ("dec_38_10", pa.decimal128(38, 10)),
        ("ts_tz", pa.timestamp("us", tz="UTC")),
        ("ts_ntz", pa.timestamp("us")),
        ("st", pa.struct([("a", pa.int32()), ("b", pa.string())])),
        ("li", pa.list_(pa.int64())),
        ("ls", pa.list_(pa.struct([("k", pa.string()), ("v", pa.float64())]))),
        ("m", pa.map_(pa.string(), pa.int32())),
    ]
)
n = 3
types = pa.table(
    {
        "id": [1, 2, 3],
        "i32": [1, -2, None],
        "i16": [1, 2, 3],
        "i8": [1, 2, 3],
        "f32": [1.5, None, -0.25],
        "f64": [1.25, 2.5, None],
        "b": [True, False, None],
        "s": ["a", "ü", None],
        "bin": [b"\x00\x01", b"", None],
        "d": [dt.date(2024, 1, 31), dt.date(1970, 1, 1), None],
        "dec_10_2": [decimal.Decimal("12345678.90"), decimal.Decimal("-0.01"), None],
        "dec_38_10": [
            decimal.Decimal("1234567890123456789012345678.0123456789"),
            decimal.Decimal("0"),
            None,
        ],
        "ts_tz": [dt.datetime(2024, 3, 10, 12, 0, 0, 123456, tzinfo=UTC), None, dt.datetime(1969, 12, 31, 23, 59, 59, tzinfo=UTC)],
        "ts_ntz": [dt.datetime(2024, 3, 10, 12, 0, 0, 123456), dt.datetime(2000, 1, 1), None],
        "st": [{"a": 1, "b": "x"}, None, {"a": None, "b": "z"}],
        "li": [[1, 2], [], None],
        "ls": [[{"k": "p", "v": 1.0}], None, []],
        "m": [[("k1", 1), ("k2", 2)], [], None],
    },
    schema=types_schema,
)
assert types.num_rows == n
write_deltalake(out / "types", types)

# 2. Partitioned (string + int), several versions: 3 appends, a delete, an overwrite of one
# partition, then a checkpoint.
def part(start, count, dts, regions):
    ids = list(range(start, start + count))
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "dt": pa.array([dts[i % len(dts)] for i in range(count)], pa.string()),
            "region": pa.array([regions[i % len(regions)] for i in range(count)], pa.int32()),
            "v": pa.array([float(i) for i in ids], pa.float64()),
        }
    )

p = out / "partitioned"
write_deltalake(p, part(0, 1000, ["2024-01-01", "2024-01-02"], [1, 2]), partition_by=["dt", "region"])  # v0
write_deltalake(p, part(1000, 1000, ["2024-01-03"], [1, 2]), mode="append")  # v1
write_deltalake(p, part(2000, 1000, ["2024-01-04"], [3]), mode="append")  # v2
DeltaTable(p).delete("id < 100")  # v3: -100 rows
write_deltalake(
    p,
    part(5000, 10, ["2024-01-04"], [3]),
    mode="overwrite",
    predicate="dt = '2024-01-04' AND region = 3",
)  # v4: partition 2024-01-04/3 replaced, 1000 -> 10
DeltaTable(p).create_checkpoint()

# 3. Schema evolution: an added column at v1, a widening-free append at v2.
e = out / "evolved"
write_deltalake(e, pa.table({"id": pa.array([1, 2], pa.int64()), "a": ["x", "y"]}))  # v0
write_deltalake(
    e,
    pa.table({"id": pa.array([3], pa.int64()), "a": ["z"], "b": pa.array([30], pa.int32())}),
    mode="append",
    schema_mode="merge",
)  # v1
write_deltalake(
    e,
    pa.table({"id": pa.array([4], pa.int64()), "a": ["w"], "b": pa.array([40], pa.int32())}),
    mode="append",
)  # v2

# 4. Deletion vectors, if delta-rs will write them.
dv = out / "dv_attempt"
write_deltalake(
    dv,
    pa.table({"id": pa.array(range(100), pa.int64())}),
    configuration={"delta.enableDeletionVectors": "true"},
)
DeltaTable(dv).delete("id % 10 = 0")
log = (dv / "_delta_log" / "00000000000000000001.json").read_text()
print("dv_attempt: delete wrote a deletion vector:", '"deletionVector":{' in log)

# 5. A production-style table: CDF on, partitioned, MERGE upserts with schema_mode=merge.
m = out / "merged"
write_deltalake(
    m,
    pa.table({"id": pa.array([1, 2, 3], pa.int64()), "p": ["a", "a", "b"], "v": [1, 2, 3]}),
    partition_by=["p"],
    configuration={"delta.enableChangeDataFeed": "true"},
)  # v0
src = pa.table({"id": pa.array([2, 4], pa.int64()), "p": ["a", "b"], "v": [20, 40]})
(
    DeltaTable(m)
    .merge(src, "t.id = s.id", source_alias="s", target_alias="t")
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute()
)  # v1: id 2 -> 20, id 4 inserted; writes _change_data/

for name in GENERATED:
    t = DeltaTable(out / name)
    print(name, "version", t.version(), "protocol", t.protocol())
