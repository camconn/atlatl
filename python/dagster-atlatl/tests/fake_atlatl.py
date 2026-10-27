"""A stand-in for `atlatl` honouring the `plan` / `run` contract dagster-atlatl relies on.

Environment:
- FAKE_ATLATL_PLAN: JSON file printed by `plan --static --json`.
- FAKE_ATLATL_SCENARIO: JSON file describing the run (see `run`).
- FAKE_ATLATL_CALLS: each `run` appends its argv as one JSON line.
- FAKE_ATLATL_SIGNALS: signals received are appended, one name per line.
- FAKE_ATLATL_STATE: succeeded replications per external run id (for `--skip-succeeded`).
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

Json = dict[str, Any]
RUN_ID = "01FAKE"


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="atlatl")
    p.add_argument("command", nargs="?")
    p.add_argument("--version", action="store_true")
    p.add_argument("--project")
    p.add_argument("--log-format")
    p.add_argument("--events")
    p.add_argument("--report")
    p.add_argument("--static", action="store_true")
    p.add_argument("--json", action="store_true")
    p.add_argument("--env")
    p.add_argument("--replication", action="append", default=[])
    p.add_argument("--stream", action="append", default=[])
    p.add_argument("--external-run-id")
    p.add_argument("--skip-succeeded", action="store_true")
    p.add_argument("--var", action="append", default=[])
    return p


def log(level: str, message: str, **fields: Any) -> None:
    rec = {"timestamp": now(), "level": level, "fields": {"message": message, **fields}}
    print(json.dumps(rec), file=sys.stderr, flush=True)


def record_signal(name: str) -> None:
    if path := os.environ.get("FAKE_ATLATL_SIGNALS"):
        with open(path, "a") as f:
            f.write(name + "\n")


class Run:
    def __init__(self, args: argparse.Namespace, scenario: Json) -> None:
        self.args = args
        self.sc = scenario
        self.plan: Json = json.loads(Path(os.environ["FAKE_ATLATL_PLAN"]).read_text())
        fd = args.events
        self.events = os.fdopen(int(fd.removeprefix("fd:")), "w", buffering=1) if fd else None
        self.report: Json = {
            "v": 1,
            "run_id": RUN_ID,
            "external_run_id": args.external_run_id,
            "atlatl_version": "0.0.0-fake",
            "env": args.env,
            "plan_hash": self.plan["plan_hash"],
            "started_at": now(),
            "finished_at": None,
            "status": "succeeded",
            "exit_code": 0,
            "killed_by": None,
            "error": None,
            "warnings": [],
            "engine": {},
            "replications": [],
        }
        self.state_path = os.environ.get("FAKE_ATLATL_STATE")

    def emit(self, type_: str, **body: Any) -> None:
        if self.events:
            ev = {"v": 1, "ts": now(), "run_id": RUN_ID, "type": type_, **body}
            self.events.write(json.dumps(ev) + "\n")

    def write_report(self, status: str, code: int) -> None:
        self.report.update(status=status, exit_code=code, finished_at=now())
        if self.args.report and not self.sc.get("no_report"):
            tmp = Path(self.args.report + ".tmp")
            tmp.write_text(json.dumps(self.report))
            tmp.rename(self.args.report)

    def succeeded_before(self) -> set[str]:
        if not self.state_path or not Path(self.state_path).exists():
            return set()
        return set(json.loads(Path(self.state_path).read_text()).get(self.args.external_run_id, []))

    def remember(self, name: str) -> None:
        if not self.state_path:
            return
        p = Path(self.state_path)
        state: dict[str, list[str]] = json.loads(p.read_text()) if p.exists() else {}
        state.setdefault(self.args.external_run_id, []).append(name)
        p.write_text(json.dumps(state))

    def entry(
        self, rep: Json, streams: list[Json], cfg: Json, status: str, instance: Json | None = None
    ) -> Json:
        stream_status = cfg.get("streams", {})
        out: list[Json] = []
        for s in streams:
            default = "succeeded" if status in ("succeeded", "up_to_date") else "rolled_back"
            members: list[Json] = [
                {
                    "member": m["name"],
                    "object": f"catalog.out.{m['table']}",
                    "duration_ms": 500,
                    "retries": [],
                    "rows": {
                        "extracted": 10,
                        "staged": 10,
                        "inserted": 7,
                        "updated": 2,
                        "deleted": 0,
                        "unchanged": 1,
                        "rejected": 0,
                        **cfg.get("rows", {}),
                    },
                    "bytes": {"read": 100},
                    "state": {"before": None, "after": {"watermark": "2026-09-01"}},
                    "lake_snapshot": 42 if status in ("succeeded", "failed") else None,
                    "chunks": 1,
                    "cdc": cfg.get("cdc"),
                    "http": cfg.get("http"),
                    "schema": cfg.get("schema"),
                    "rejects": {"new": 0, "resolved": 0, "open": 0, "samples": []},
                    "warnings": [],
                }
                for m in s["members"]
            ]
            out.append(
                {
                    "stream_id": s["id"],
                    "asset_key": s["asset"]["key"],
                    "status": stream_status.get(s["id"], default),
                    "duration_ms": 1500,
                    "members": members,
                }
            )
        hops = cfg.get(
            "hops",
            [{"target": rep["target"]["name"], "status": "committed", "lake_snapshot": 42}]
            if status == "succeeded"
            else [{"target": rep["target"]["name"], "status": "failed", "lake_snapshot": None}],
        )
        return {
            "replication": rep["name"],
            "op": rep["op"],
            "status": status,
            "started_at": now(),
            "duration_ms": 2000,
            "attempts": cfg.get("attempts", 1),
            "hops": hops,
            "error": cfg.get("error")
            if status not in ("succeeded", "up_to_date", "skipped")
            else None,
            "streams": out,
            **({"instance": instance} if instance else {}),
        }

    def selected(self) -> list[tuple[Json, list[Json]]]:
        out: list[tuple[Json, list[Json]]] = []
        for rep in self.plan["replications"]:
            if self.args.replication and rep["name"] not in self.args.replication:
                continue
            streams = [
                s for s in rep["streams"] if not self.args.stream or s["id"] in self.args.stream
            ]
            if streams:
                out.append((rep, streams))
        return out

    def on_term(self, _sig: int, _frame: Any) -> None:
        record_signal("TERM")
        if self.sc.get("ignore_term"):
            return
        self.report["killed_by"] = "sigterm"
        self.write_report("killed", 143)
        os._exit(143)

    def main(self) -> int:
        signal.signal(signal.SIGTERM, self.on_term)
        for line in self.sc.get("stderr", []):
            log("INFO", line)
        if (code := self.sc.get("exit")) == 2:
            log("ERROR", "config error: unknown connection `nope`")
            self.report["error"] = {
                "class": "config",
                "code": None,
                "message": "unknown connection `nope`",
            }
            self.write_report("config_error", 2)
            return 2
        sel = self.selected()
        self.emit(
            "run.started",
            env=self.args.env,
            selection={"replications": self.args.replication, "streams": self.args.stream},
            plan_hash=self.plan["plan_hash"],
            atlatl_version="0.0.0-fake",
            duckdb_version="1.5.6",
        )
        skip = self.succeeded_before() if self.args.skip_succeeded else set[str]()
        failed = succeeded = 0
        units: list[tuple[Json, list[Json], Json | None]] = []
        for rep, streams in sel:
            keys: list[str] | None = self.sc.get("instances", {}).get(rep["name"])
            if keys is None:
                units.append((rep, streams, None))
            else:
                units.extend(
                    ({**rep, "name": f"{rep['name']}[{k}]"}, streams, {"of": rep["name"], "key": k})
                    for k in keys
                )
        for i, (rep, streams, instance) in enumerate(units):
            cfg: Json = self.sc.get("replications", {}).get(rep["name"], {})
            if rep["name"] in skip:
                entry = self.entry(rep, streams, cfg, "skipped", instance)
                entry["streams"] = []
                self.report["replications"].append(entry)
                self.emit("replication.completed", **entry)
                continue
            self.emit(
                "replication.started",
                replication=rep["name"],
                op=rep["op"],
                streams=[s["id"] for s in streams],
            )
            for s in streams:
                self.emit(
                    "stream.progress",
                    replication=rep["name"],
                    stream_id=s["id"],
                    phase="extract",
                    rows_so_far=10,
                    bytes_so_far=100,
                    chunk=1,
                )
            if cfg.get("retry"):
                self.emit(
                    "retrying",
                    replication=rep["name"],
                    stream_id=streams[0]["id"],
                    level="request",
                    attempt=1,
                    error_class="transient",
                    message="connection reset",
                    wait_ms=10,
                )
            if self.sc.get("hang"):
                (Path(os.environ["FAKE_ATLATL_SIGNALS"]).parent / "hung").write_text("")
                time.sleep(30)
                log("ERROR", "hung for 30s without being terminated")
                return 98
            status = cfg.get("status", "succeeded")
            entry = self.entry(rep, streams, cfg, status, instance)
            self.report["replications"].append(entry)
            self.emit("replication.completed", **entry)
            if status in ("succeeded", "up_to_date"):
                succeeded += 1
                self.remember(rep["name"])
            else:
                failed += 1
            if i == 0 and (gate := self.sc.get("gate")):
                deadline = time.monotonic() + 10
                while not Path(gate).exists():
                    if time.monotonic() > deadline:
                        log("ERROR", "gate timeout: first materialization was not streamed")
                        return 99
                    time.sleep(0.02)
        for w in self.sc.get("warnings", []):
            self.emit("warning", code="fake", message=w)
        if code == 3:
            self.report["error"] = {"class": "internal", "code": "panic", "message": "boom"}
            status, code = "internal_error", 3
        elif failed:
            status, code = ("partial_failure" if succeeded else "failed"), 1
        else:
            status, code = "succeeded", int(code or 0)
        counts = {
            "succeeded": succeeded,
            "failed": failed,
            "skipped": len(skip),
            "killed": 0,
            "not_started": 0,
        }
        self.emit(
            "run.completed",
            status=status,
            exit_code=code,
            counts=counts,
            report_path=self.args.report,
        )
        self.write_report(status, code)
        return code


def main() -> int:
    args, _ = parser().parse_known_args()
    if args.version:
        print("atlatl 0.0.0-fake")
        return 0
    if args.command == "plan":
        if err := os.environ.get("FAKE_ATLATL_PLAN_ERROR"):
            print(err, file=sys.stderr)
            return 2
        print(Path(os.environ["FAKE_ATLATL_PLAN"]).read_text())
        return 0
    if args.command != "run":
        print(f"fake atlatl: unsupported command {args.command}", file=sys.stderr)
        return 2
    if calls := os.environ.get("FAKE_ATLATL_CALLS"):
        with open(calls, "a") as f:
            f.write(json.dumps(sys.argv[1:]) + "\n")
    scenario: Json = {}
    if path := os.environ.get("FAKE_ATLATL_SCENARIO"):
        scenario = json.loads(Path(path).read_text())
    if attempts := scenario.get("attempts"):
        n = len(Path(calls).read_text().splitlines()) if calls else 1
        scenario = attempts[min(n, len(attempts)) - 1]
    return Run(args, scenario).main()


if __name__ == "__main__":
    sys.exit(main())
