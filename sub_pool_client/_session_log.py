"""A per-machine event log of what each sp-* session did.

`-v` only helps when the user thought to pass it before the failure. This log
is always on, so "it said Not logged in an hour ago" can be answered after
the fact: one JSON line per event in `~/.sub-pool/logs/sp-<provider>.log`,
shared by every concurrent session on the machine (each line carries its
session id and pid, so they can be told apart and lined up in time).

What gets an event: the session's start (client version, platform, mode,
how many other sessions were running), the lease, every token refresh and
swap, health turns, auth signals and what the recovery did, credential-file
checks (`_cred_guard`), the child's exit and the release.

Never logged: tokens, keys, prompts, model output. A line of child output is
recorded only when a detector matched it, truncated to `SAMPLE_CHARS`.

`SUB_POOL_SESSION_LOG=0` turns it off; `SUB_POOL_SESSION_LOG=<path>` moves
it. Writes are best-effort and never raise into the session. Each line goes
out in one `write` on an O_APPEND fd, so concurrent sessions don't interleave
inside a line. Past `MAX_BYTES` the file rotates once to `.1`.
"""
from __future__ import annotations

import json
import logging
import os
import platform
import secrets
import sys
import tempfile
import time
from contextvars import ContextVar
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

MAX_BYTES = 5 << 20
SAMPLE_CHARS = 200


# The running session's log. Set once in `run_session`; every task it spawns
# inherits it, so helpers deep in the refresh / swap paths can `emit` without
# threading a logger through signatures.
CURRENT: ContextVar["SessionLog | None"] = ContextVar("sp_session_log", default=None)


def emit(name: str, **fields: Any) -> None:
    """Record an event on the current session's log, if there is one."""
    slog = CURRENT.get()
    if slog is not None:
        slog.event(name, **fields)


def error_fields(e: BaseException) -> dict[str, Any]:
    """Secret-free description of a failure: type, HTTP status, short text."""
    out: dict[str, Any] = {"error": type(e).__name__}
    resp = getattr(e, "response", None)
    status = getattr(resp, "status_code", None)
    if status is not None:
        out["status"] = status
        try:
            out["body"] = sample(resp.text)
        except Exception:  # noqa: BLE001
            pass
    else:
        out["message"] = sample(str(e))
    return out


def default_path(root: Path, provider: str) -> Path | None:
    raw = os.environ.get("SUB_POOL_SESSION_LOG", "").strip()
    if raw.lower() in ("0", "false", "no", "off"):
        return None
    if raw and raw.lower() not in ("1", "true", "yes", "on"):
        return Path(raw).expanduser()
    return root / "logs" / f"sp-{provider}.log"


def sample(line: str) -> str:
    text = line.strip()
    return text if len(text) <= SAMPLE_CHARS else text[:SAMPLE_CHARS] + "…"


def other_sessions(provider: str) -> int:
    """sp-<provider> session dirs alive in the temp dir right now (ours
    excluded by the caller counting before it creates its own)."""
    try:
        return sum(1 for p in Path(tempfile.gettempdir()).glob(f"sp-{provider}-*")
                   if p.is_dir())
    except OSError:
        return -1


class SessionLog:
    def __init__(self, path: Path | None, provider: str, *,
                 current: dict | None = None, verbose: bool = False):
        self.path = path
        self.provider = provider
        self.session = secrets.token_hex(4)
        self.current = current if current is not None else {}
        self.verbose = verbose
        self.started = time.monotonic()
        self.events: list[dict[str, Any]] = []  # in-memory copy, for tests

    def event(self, name: str, **fields: Any) -> None:
        lease = self.current.get("lease") or {}
        entry: dict[str, Any] = {
            "ts": round(time.time(), 3),
            "at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "session": self.session, "pid": os.getpid(),
            "provider": self.provider, "event": name,
            "t": round(time.monotonic() - self.started, 1),
        }
        if lease:
            entry["account"] = lease.get("account")
            entry["lease_id"] = lease.get("lease_id")
        entry.update(fields)
        self.events.append(entry)
        if self.verbose:
            log.info("%s %s", name, json.dumps(fields, sort_keys=True, default=str))
        if self.path is None:
            return
        try:
            data = (json.dumps(entry, sort_keys=True, default=str) + "\n").encode()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            try:
                if self.path.stat().st_size > MAX_BYTES:
                    os.replace(self.path, self.path.with_name(self.path.name + ".1"))
            except FileNotFoundError:
                pass
            fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                os.write(fd, data)
            finally:
                os.close(fd)
        except Exception as e:  # noqa: BLE001 — logging must never break a session
            log.debug("session log write failed: %s", e)

    def start(self, *, client_version: str, bin_path: str, args: tuple[str, ...],
              interactive: bool, watch_output: bool, others: int,
              stripped_env: list[str]) -> None:
        self.event(
            "session_start",
            client_version=client_version,
            platform=f"{platform.system()} {platform.release()} {platform.machine()}",
            python=platform.python_version(),
            bin=bin_path,
            # Flag names only: values can be prompts.
            flags=sorted({a.split("=", 1)[0] for a in args if a.startswith("-")}),
            interactive=interactive, watch_output=watch_output,
            stdin_tty=_isatty(sys.stdin), stdout_tty=_isatty(sys.stdout),
            other_sessions=others, stripped_env=stripped_env,
        )


def _isatty(stream: Any) -> bool | None:
    try:
        return bool(stream.isatty())
    except Exception:  # noqa: BLE001
        return None
