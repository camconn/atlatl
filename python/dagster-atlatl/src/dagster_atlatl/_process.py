"""Runs one `atlatl run` child: an events pipe, stderr/stdout forwarding, and SIGTERM → SIGKILL."""

from __future__ import annotations

import contextlib
import json
import os
import queue
import subprocess
import threading
from collections.abc import Callable, Generator, Mapping, Sequence
from dataclasses import dataclass
from typing import IO, Any

from dagster_atlatl._results import Json, obj

EVENTS_FD_PLACEHOLDER = "{events_fd}"
_POLL_S = 0.1


@dataclass(frozen=True)
class Event:
    body: Mapping[str, Any]


@dataclass(frozen=True)
class LogLine:
    level: str
    message: str


Item = Event | LogLine

_LEVELS = {"trace": "debug", "debug": "debug", "info": "info", "warn": "warning", "error": "error"}


def parse_log_line(line: str) -> LogLine:
    """A `--log-format json` line from stderr, or plain text."""
    rec: Json = {}
    with contextlib.suppress(ValueError):
        rec = obj(json.loads(line))
    if not rec:
        return LogLine("info", line)
    level = _LEVELS.get(str(rec.get("level", "info")).lower(), "info")
    fields = obj(rec.get("fields"))
    msg = str(fields.get("message", ""))
    extra = " ".join(f"{k}={v}" for k, v in fields.items() if k != "message")
    return LogLine(level, f"{msg} {extra}".strip() if extra else msg or line)


class AtlatlProcess:
    """Iterate to run the child; `returncode` is set once iteration ends normally.

    `argv` contains `EVENTS_FD_PLACEHOLDER`, replaced by the write end of the events pipe
    (inherited via `pass_fds`, so the number is the parent's). Any exit from iteration
    other than normal completion (an interrupt, `GeneratorExit`, an error) terminates the child.
    """

    def __init__(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        terminate_grace_s: float = 5.0,
        on_terminate: Callable[[str], None] | None = None,
    ) -> None:
        self.argv = list(argv)
        self.env = dict(env) if env is not None else None
        self.terminate_grace_s = terminate_grace_s
        self.on_terminate: Callable[[str], None] = on_terminate or _ignore
        self.returncode: int | None = None
        self.pid: int | None = None

    def __iter__(self) -> Generator[Item, None, None]:
        r, w = os.pipe()
        argv = [a.replace(EVENTS_FD_PLACEHOLDER, str(w)) for a in self.argv]
        try:
            proc = subprocess.Popen(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                pass_fds=(w,),
                env=self.env,
            )
        except BaseException:
            os.close(r)
            os.close(w)
            raise
        os.close(w)
        self.pid = proc.pid
        q: queue.Queue[Item | None] = queue.Queue()
        assert proc.stdout is not None
        assert proc.stderr is not None
        readers = [
            threading.Thread(target=_read_events, args=(os.fdopen(r, "rb"), q), daemon=True),
            threading.Thread(target=_read_log, args=(proc.stderr, q), daemon=True),
            threading.Thread(target=_read_log, args=(proc.stdout, q), daemon=True),
        ]
        for t in readers:
            t.start()
        finished = False
        try:
            open_readers = len(readers)
            while open_readers:
                try:
                    item = q.get(timeout=_POLL_S)
                except queue.Empty:
                    continue
                if item is None:
                    open_readers -= 1
                else:
                    yield item
            while proc.poll() is None:
                try:
                    proc.wait(timeout=_POLL_S)
                except subprocess.TimeoutExpired:
                    continue
            self.returncode = proc.returncode
            finished = True
        finally:
            if not finished:
                self._terminate(proc)

    def _terminate(self, proc: subprocess.Popen[bytes]) -> None:
        if proc.poll() is not None:
            self.returncode = proc.returncode
            return
        self.on_terminate(f"sending SIGTERM to atlatl (pid {proc.pid})")
        proc.terminate()
        try:
            proc.wait(timeout=self.terminate_grace_s)
        except subprocess.TimeoutExpired:
            self.on_terminate(
                f"atlatl (pid {proc.pid}) still running after {self.terminate_grace_s:g}s; "
                "sending SIGKILL"
            )
            proc.kill()
            proc.wait()
        self.returncode = proc.returncode


def _ignore(_msg: str) -> None:
    pass


def _read_events(f: IO[bytes], q: queue.Queue[Item | None]) -> None:
    with f:
        for raw in f:
            line = raw.decode("utf-8", "replace").strip()
            if not line:
                continue
            try:
                body = json.loads(line)
            except ValueError:
                q.put(LogLine("warning", f"unparseable atlatl event: {line}"))
                continue
            if ev := obj(body):
                q.put(Event(ev))
    q.put(None)


def _read_log(f: IO[bytes], q: queue.Queue[Item | None]) -> None:
    with f:
        for raw in f:
            line = raw.decode("utf-8", "replace").rstrip("\r\n")
            if line.strip():
                q.put(parse_log_line(line))
    q.put(None)
