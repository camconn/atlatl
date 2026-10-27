"""`python -m dbt_atlatl sources`: writes (or with --check verifies) dbt sources from the plan."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dbt_atlatl._run import AtlatlRunError
from dbt_atlatl._sources import load_plan, render, tables


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m dbt_atlatl")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sources", help="dbt sources for the tables atlatl writes to DuckDB files")
    s.add_argument("--project", type=Path, required=True, help="atlatl project root")
    s.add_argument("--out", type=Path, required=True, help="sources YAML to write")
    s.add_argument("--connection", action="append", default=[], help="only these duckdb targets")
    s.add_argument("--prefix", default="", help="prepended to each source name (the schema)")
    s.add_argument("--database", help="the sources' dbt database, when not the target's")
    s.add_argument("--binary", help="atlatl binary (default: ATLATL_BIN, else PATH)")
    s.add_argument("--profiles-dir", help="for `dbt:` connections (default: DBT_PROFILES_DIR)")
    s.add_argument("--target", help="for `dbt:` connections (default: DBT_TARGET)")
    s.add_argument("--check", action="store_true", help="exit 1 if --out is not up to date")
    args = ap.parse_args(argv)

    def warn(msg: str) -> None:
        print(f"warning: {msg}", file=sys.stderr)

    try:
        env: dict[str, str] = {}
        if args.profiles_dir:
            env["DBT_PROFILES_DIR"] = str(Path(args.profiles_dir).resolve())
        if args.target:
            env["DBT_TARGET"] = args.target
        plan = load_plan(args.project, args.binary, env)
    except AtlatlRunError as e:
        print(e, file=sys.stderr)
        return 2
    text = render(tables(plan, args.connection, warn), prefix=args.prefix, database=args.database)
    out: Path = args.out
    if args.check:
        if not out.is_file() or out.read_text() != text:
            print(f"{out} is out of date; run without --check", file=sys.stderr)
            return 1
        return 0
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
