"""Per-session token usage, reported to the pool on the final lease release.

The pool sees how long a key holds leases, never what the sessions on them
consumed — only the client can, from whatever the upstream CLI tells it. So
the release that ends a client session carries a `usage` object (contract:
`DELETE /credentials/lease/{id}`): one report per session, covering everything
it did across any account swaps. Swap releases carry none. A client that
can't work its usage out sends no `usage` key at all, never zeros it doesn't
actually know, so the pool can count honestly how many leases reported.

Everything here is a side channel on a cleanup path: parsing never raises
into the caller, file reads are bounded, and the final tally runs off the
event loop under a deadline (`collect`), so a huge transcript costs its own
report and nothing else.

Sources, per client:

  - `ClaudeResults` — the SDK's `ResultMessage` / the CLI's stream-json
    `{"type": "result"}` lines (PooledClient, piped sp-claude).
  - `ClaudeTranscripts` — `$CLAUDE_CONFIG_DIR/projects/*/<session>.jsonl`, for
    an interactive sp-claude whose output we can't read.
  - `CodexRollouts` — `$CODEX_HOME/sessions/**/rollout-*.jsonl` (sp-codex,
    PooledCodexClient).

Internally a usage is a "tally": the five token fields, `cost_usd` (None =
unknown) and `models` (None = no trustworthy per-model breakdown). `to_wire`
turns one into the contract's shape, inside the server's bounds.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import json
import logging
import math
import os
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Protocol

log = logging.getLogger(__name__)

TOKEN_FIELDS = (
    "input_tokens", "output_tokens", "cache_read_tokens",
    "cache_write_tokens", "reasoning_tokens",
)

# The server 422s a release outside these, so they are enforced here.
MAX_TOKENS = 10 ** 12
MAX_COST_USD = 1e6
MAX_MODELS = 20
MAX_MODEL_NAME = 128

# Read bounds. A file past MAX_FILE_BYTES is read from its tail only; see
# `_read_tail` for how that stays correct.
MAX_FILE_BYTES = 32 << 20
MAX_FIRST_LINE_BYTES = 8 << 20     # a rollout's session_meta embeds the prompt
MAX_SCAN_ENTRIES = 200_000         # dir entries walked looking for rollouts
MAX_FILES = 2_000                  # files read per tally

COLLECT_TIMEOUT_S = 5.0
POLL_INTERVAL_S = 1.0

# Session / thread ids: uuid-shaped. Validated before they go into a glob.
_ID_RE = re.compile(r"[0-9A-Za-z][0-9A-Za-z-]{7,63}")


# ============================================================ tallies


def _int(v: Any) -> int:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return 0
    if isinstance(v, float) and not math.isfinite(v):
        return 0
    return int(v)


def _num(v: Any) -> float | None:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    v = float(v)
    return v if math.isfinite(v) else None


def _flat(src: dict | None = None, cost: Any = 0.0) -> dict:
    out = {f: _int((src or {}).get(f)) for f in TOKEN_FIELDS}
    out["cost_usd"] = _num(cost)
    return out


def _tally(models: dict | None, cost: Any = 0.0, src: dict | None = None) -> dict:
    t = _flat(src, cost)
    t["models"] = models
    return t


def _add_cost(a: float | None, b: float | None) -> float | None:
    return None if a is None or b is None else a + b


def _add_flat(a: dict | None, b: dict) -> dict:
    if a is None:
        return dict(b)
    out = {f: a[f] + b[f] for f in TOKEN_FIELDS}
    out["cost_usd"] = _add_cost(a["cost_usd"], b["cost_usd"])
    return out


def _coerce(t: Any) -> dict | None:
    """A well-formed tally from whatever `t` is — it may have round-tripped
    through a shared meta.json — or None. Never raises."""
    if not isinstance(t, dict):
        return None
    out = _tally(None, t.get("cost_usd"), t)
    models = t.get("models")
    if isinstance(models, dict):
        out["models"] = {
            str(k): _flat(v, v.get("cost_usd"))
            for k, v in models.items() if isinstance(v, dict)
        }
    return out


def merge(a: Any, b: Any) -> dict | None:
    """Sum two tallies. An unknown cost or per-model breakdown on either side
    makes the sum's unknown too: a partial figure would read as a total."""
    a, b = _coerce(a), _coerce(b)
    if a is None or b is None:
        return a or b
    out = _add_flat(a, b)
    if a["models"] is None or b["models"] is None:
        out["models"] = None
    else:
        models = dict(a["models"])
        for name, m in b["models"].items():
            models[name] = _add_flat(models.get(name), m)
        out["models"] = models
    return out


def _minus(new: dict, old: dict) -> dict:
    out = {f: max(0, new[f] - old[f]) for f in TOKEN_FIELDS}
    out["cost_usd"] = (None if new["cost_usd"] is None or old["cost_usd"] is None
                       else max(0.0, new["cost_usd"] - old["cost_usd"]))
    return out


def _shrank(new: dict, old: dict) -> bool:
    """Did a cumulative counter go backwards — i.e. restart from zero?"""
    if any(new[f] < old[f] for f in TOKEN_FIELDS):
        return True
    return (new["cost_usd"] is not None and old["cost_usd"] is not None
            and new["cost_usd"] < old["cost_usd"] - 1e-9)


def _is_zero(t: dict) -> bool:
    return not any(t[f] for f in TOKEN_FIELDS) and not t["cost_usd"]


def _sum_models(models: dict[str, dict]) -> dict:
    """A tally whose totals are the sum of its per-model entries."""
    flat = _flat()
    for m in models.values():
        flat = _add_flat(flat, m)
    return {**flat, "models": models}


def to_wire(t: Any) -> dict | None:
    """The contract's `usage` object for a tally, clamped to the server's
    bounds (out-of-range values would 422 the whole release). Never raises:
    it runs on the release path."""
    try:
        t = _coerce(t)
        if t is None:
            return None
        out = _wire_flat(t)
        if t["models"]:
            named: dict[str, dict] = {}
            for name, m in t["models"].items():
                key = name[:MAX_MODEL_NAME] or "unknown"
                named[key] = _add_flat(named.get(key), m)
            # Over the cap, keep the biggest; the breakdown is informational
            # and the top-level totals still cover everything.
            top = sorted(named.items(),
                         key=lambda kv: -sum(kv[1][f] for f in TOKEN_FIELDS))
            out["models"] = {k: _wire_flat(v) for k, v in top[:MAX_MODELS]}
        return out
    except Exception as e:  # noqa: BLE001
        log.debug("usage dropped: unusable tally (%s)", e)
        return None


def _wire_flat(t: dict) -> dict:
    out: dict[str, Any] = {f: min(max(t[f], 0), MAX_TOKENS) for f in TOKEN_FIELDS}
    cost = t["cost_usd"]
    out["cost_usd"] = None if cost is None else min(max(cost, 0.0), MAX_COST_USD)
    return out


async def collect(total: Callable[[], dict | None], *,
                  timeout: float | None = None) -> dict | None:
    """Run `total()` off the event loop, under a deadline. None on any
    failure or timeout.

    A daemon thread rather than `asyncio.to_thread`: `asyncio.run` joins the
    default executor on shutdown, so a read that overran its deadline would
    still hold up the process exit it was supposed to stay out of."""
    fut: concurrent.futures.Future = concurrent.futures.Future()

    def run() -> None:
        try:
            result, error = total(), None
        except Exception as e:  # noqa: BLE001
            result, error = None, e
        with contextlib.suppress(concurrent.futures.InvalidStateError):
            if error is None:
                fut.set_result(result)
            else:
                fut.set_exception(error)

    threading.Thread(target=run, name="sp-usage", daemon=True).start()
    try:
        return await asyncio.wait_for(
            asyncio.wrap_future(fut),
            COLLECT_TIMEOUT_S if timeout is None else timeout,
        )
    except Exception as e:  # noqa: BLE001
        log.debug("usage collection failed: %r", e)
        return None


# ============================================================ file reads


def _read_tail(path: Path, max_bytes: int | None = None) -> tuple[list[str], bool]:
    """The file's lines — or, past `max_bytes`, those of its last `max_bytes`
    (the first, partial one dropped). The flag says whether it was cut.

    Callers only ever need what happened since the session started, which is
    the end of the file; a cut is only a problem when it might have dropped
    some of that, and each caller checks for exactly that."""
    cap = MAX_FILE_BYTES if max_bytes is None else max_bytes
    with path.open("rb") as f:
        size = os.fstat(f.fileno()).st_size
        cut = size > cap
        if cut:
            f.seek(size - cap)
        data = f.read(cap)
    if cut:
        data = data[data.find(b"\n") + 1:] if b"\n" in data else b""
    return data.decode("utf-8", "replace").splitlines(), cut


def _ts(v: Any) -> float | None:
    """Epoch seconds from an ISO-8601 timestamp (`2026-09-24T17:16:52.908Z`)."""
    if not isinstance(v, str):
        return None
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _json(line: str) -> dict | None:
    try:
        d = json.loads(line)
    except (ValueError, RecursionError):
        return None
    return d if isinstance(d, dict) else None


def _window_starts_before(lines: list[str], since: float) -> bool:
    """Does a cut file's window reach back past `since`? Judged by its first
    timestamped line: if even that is after the session started, the cut
    may have dropped some of the session."""
    for line in lines[:200]:
        ts = _ts((_json(line) or {}).get("timestamp"))
        if ts is not None:
            return ts < since
    return False


# ============================================================ claude


def _claude_turn(usage: Any) -> dict:
    """An Anthropic `usage` block (a result's per-turn `usage`, a transcript
    message's `message.usage`) as a tally with no cost or models."""
    u = usage if isinstance(usage, dict) else {}
    details = u.get("output_tokens_details")
    return _tally(None, None, {
        "input_tokens": u.get("input_tokens"),
        "output_tokens": u.get("output_tokens"),
        "cache_read_tokens": u.get("cache_read_input_tokens"),
        "cache_write_tokens": u.get("cache_creation_input_tokens"),
        "reasoning_tokens": (details.get("thinking_tokens")
                             if isinstance(details, dict) else 0),
    })


def _claude_snapshot(model_usage: Any, total_cost_usd: Any) -> dict | None:
    """A result's cumulative `modelUsage` (+ `total_cost_usd`) as a tally.
    None when it carries nothing: crash / startup-error results come zeroed,
    and taking one for a counter reset would count everything twice."""
    if not isinstance(model_usage, dict):
        return None
    models: dict[str, dict] = {}
    for name, m in model_usage.items():
        if not isinstance(m, dict):
            continue
        models[str(name)] = _flat({
            "input_tokens": m.get("inputTokens"),
            "output_tokens": m.get("outputTokens"),
            "cache_read_tokens": m.get("cacheReadInputTokens"),
            "cache_write_tokens": m.get("cacheCreationInputTokens"),
            "reasoning_tokens": m.get("thinkingTokens"),
        }, m.get("costUSD"))
    if not models:
        return None
    snap = _sum_models(models)
    cost = _num(total_cost_usd)
    if cost is not None:
        snap["cost_usd"] = cost
    return None if _is_zero(snap) else snap


class ClaudeResults:
    """Usage from Claude result messages — `ResultMessage` in the SDK,
    `{"type": "result"}` lines on the CLI's json / stream-json stdout.

    Within one CLI process (a ClaudeSDKClient session, or a `claude -p`), per
    the result schema's own field docs (claude 2.1.285):

      - `modelUsage` and `total_cost_usd` are CUMULATIVE across turns — every
        result carries the running total, "read the latest result rather than
        summing" — and cover the whole query pipeline: subagents, compaction
        and other internal calls included. A mid-session `/clear` resets them.
      - `usage` is "per-turn in streaming-input sessions" and "MAIN AGENT LOOP
        ONLY" — no subagents. The docs themselves point at `modelUsage` for
        token/cost accounting.

    So totals come from `modelUsage`, as the sum of each result's increase
    over the one before it. That is the latest value in the common case, and
    stays right across a `/clear`, which the CLI announces with a
    `conversation_reset` message (`reset()`): the counters restart from zero,
    so the next result is all increase. A counter seen going backwards is
    taken the same way, for a CLI that doesn't announce it. Per-turn `usage`
    is only a fallback for a CLI that sends no `modelUsage`.

    A resumed or forked session is the exception: its first result "already
    carries the earlier turns" the transcript saved, i.e. usage some earlier
    session (and lease) already reported. With `resumed=True` the first
    result contributes only its own turn's `usage` — main loop only, and
    without a cost (nothing separates turn one's cost from the inherited
    total), so a resumed session reports no `cost_usd` or `models`.
    """

    def __init__(self, *, resumed: bool = False):
        self._resumed = resumed
        self._prev: dict | None = None    # last cumulative snapshot
        self._total: dict | None = None   # Σ increases between snapshots
        self._turns: dict | None = None   # Σ per-turn `usage` (fallback)

    @property
    def seen(self) -> bool:
        return self._turns is not None

    def reset(self) -> None:
        """The conversation was replaced mid-session and the running totals
        zeroed: the next result counts in full — nothing inherited, either."""
        self._prev = {**_flat(), "models": {}}

    def observe(self, *, total_cost_usd: Any, usage: Any, model_usage: Any) -> None:
        turn = _claude_turn(usage)
        self._turns = merge(self._turns, turn)
        snap = _claude_snapshot(model_usage, total_cost_usd)
        if snap is None:
            return
        if self._prev is None:
            part = turn if self._resumed else snap
        elif _shrank(snap, self._prev):
            part = snap
        else:
            part = _minus(snap, self._prev)
            part["models"] = {
                name: _minus(m, self._prev["models"].get(name) or _flat())
                for name, m in snap["models"].items()
            }
        self._total = merge(self._total, part)
        self._prev = snap

    def observe_message(self, message: Any) -> None:
        """An SDK `ResultMessage`."""
        self.observe(
            total_cost_usd=getattr(message, "total_cost_usd", None),
            usage=getattr(message, "usage", None),
            model_usage=getattr(message, "model_usage", None),
        )

    def observe_json(self, d: dict) -> None:
        """One parsed stdout line; anything but a result or a reset is
        ignored."""
        kind = d.get("type")
        if kind == "result":
            self.observe(total_cost_usd=d.get("total_cost_usd"),
                         usage=d.get("usage"), model_usage=d.get("modelUsage"))
        elif kind == "conversation_reset":
            self.reset()

    def total(self) -> dict | None:
        """None until a result has been seen."""
        return merge(self._total if self._total is not None else self._turns, None)


class ClaudeTranscripts:
    """Usage of an interactive `claude`, read back from its transcripts.

    Attribution is by process, not by directory. The session dir's `projects`
    is shared with every other sp-claude (and a resumed transcript holds
    turns from earlier sessions), so "files in this cwd's project dir" would
    sweep up whatever else ran there. Instead: claude records each live
    process in `$CLAUDE_CONFIG_DIR/sessions/<pid>.json` with its current
    `sessionId`, rewriting it when the session switches (`/clear`,
    `/resume`), and deletes it on exit. `poll()` reads our child's file while
    it runs, collecting every id it has had; `total()` then reads exactly
    those sessions' transcripts (and their subagents'), counting only lines
    stamped after the child started.

    Streaming writes one transcript line per content block, each repeating
    the message's usage as it stood (output grows as the message streams),
    so lines are de-duplicated on (message id, request id) keeping the
    field-wise max.

    No cost: transcripts don't carry one. Also missing: calls made outside
    the transcript (compaction, title generation and other internal helpers).
    A switch less than one poll before exit goes unseen. No `sessions/<pid>
    .json` (a claude too old to write one, or one storing it elsewhere) means
    no report.
    """

    def __init__(self, config_dir: Path, pid: int, started_at: float):
        self._config_dir = config_dir
        self._pid = pid
        self._started_at = started_at
        self.session_ids: list[str] = []

    def poll(self) -> None:
        path = self._config_dir / "sessions" / f"{self._pid}.json"
        try:
            with path.open("rb") as f:
                d = _json(f.read(1 << 16).decode("utf-8", "replace"))
        except OSError:
            return
        sid = (d or {}).get("sessionId")
        if (isinstance(sid, str) and _ID_RE.fullmatch(sid)
                and sid not in self.session_ids):
            self.session_ids.append(sid)

    def total(self) -> dict | None:
        projects = self._config_dir / "projects"
        since = self._started_at
        found = False
        best: dict[tuple, tuple[str, dict]] = {}
        for sid in list(self.session_ids):
            paths = [*projects.glob(f"*/{sid}.jsonl"),
                     *projects.glob(f"*/{sid}/subagents/*.jsonl")]
            for path in paths[:MAX_FILES]:
                lines, cut = _read_tail(path)
                found = True
                if cut and not _window_starts_before(lines, since):
                    log.debug("usage: %s too large to attribute", path.name)
                    return None
                for line in lines:
                    if '"assistant"' not in line:
                        continue
                    d = _json(line)
                    if (d is None or d.get("type") != "assistant"
                            or d.get("sessionId") != sid):
                        continue
                    ts = _ts(d.get("timestamp"))
                    msg = d.get("message")
                    if ts is None or ts < since or not isinstance(msg, dict):
                        continue
                    model = msg.get("model")
                    model = model if isinstance(model, str) and model else "unknown"
                    if model == "<synthetic>" or not isinstance(msg.get("usage"), dict):
                        continue
                    key = (msg.get("id") or d.get("uuid"), d.get("requestId"))
                    turn = _claude_turn(msg["usage"])
                    if key in best:
                        prev = best[key][1]
                        turn.update({f: max(prev[f], turn[f]) for f in TOKEN_FIELDS})
                    best[key] = (model, turn)
        if not found:
            return None
        models: dict[str, dict] = {}
        for model, turn in best.values():
            models[model] = _add_flat(models.get(model), _flat(turn, None))
        return {**_sum_models(models), "cost_usd": None}


def _claude_resumes(args: tuple[str, ...]) -> bool:
    """Does this `claude` command line continue an earlier session? Errs
    towards yes: a wrong yes costs the cost figure and turn one's subagents
    (see ClaudeResults), a wrong no re-reports an earlier session's totals."""
    for a in args:
        if a == "--":
            break
        if a in ("-r", "--resume", "-c", "--continue", "--fork-session") \
                or a.startswith("--resume="):
            return True
    return False


def _claude_output_format(args: tuple[str, ...]) -> str:
    fmt = "text"
    for i, a in enumerate(args):
        if a == "--":
            break
        if a == "--output-format" and i + 1 < len(args):
            fmt = args[i + 1]
        elif a.startswith("--output-format="):
            fmt = a.split("=", 1)[1]
    return fmt


# ============================================================ codex

# Raw codex counter names, in `info.total_token_usage` and `turn.completed`.
_CODEX_FIELDS = ("input_tokens", "cached_input_tokens", "cache_write_input_tokens",
                 "output_tokens", "reasoning_output_tokens")

# `codex exec`'s human-readable header line, on stderr.
_SESSION_ID_LINE = re.compile(r"session id:\s*([0-9A-Za-z-]{8,64})\s*$")


def _codex_raw(u: Any) -> dict:
    u = u if isinstance(u, dict) else {}
    return {f: _int(u.get(f)) for f in _CODEX_FIELDS}


def _codex_tally(raw: dict, models: dict | None) -> dict:
    """Contract fields from codex counters: codex's `input_tokens` includes
    the cached part, the contract's is uncached only. Codex never says what
    anything cost."""
    return _tally(models, None, {
        "input_tokens": max(0, raw["input_tokens"] - raw["cached_input_tokens"]),
        "output_tokens": raw["output_tokens"],
        "cache_read_tokens": raw["cached_input_tokens"],
        "cache_write_tokens": raw["cache_write_input_tokens"],
        "reasoning_tokens": raw["reasoning_output_tokens"],
    })


class _Unattributable(Exception):
    """The file can't say how much of it belongs to the session."""


def _rollout_delta(path: Path, since: float) -> dict:
    """What a rollout accumulated after `since`, as a tally.

    `token_count` events carry the thread's cumulative counters, so the
    session's share is the last value minus the last one stamped before it
    started — zero for a thread born during the session, the inherited total
    for one it resumed. Summed step by step rather than last-minus-first so
    each step can be charged to the model of the turn it happened in
    (`turn_context.model`, which `/model` can change mid-thread); a step
    whose counters went backwards is taken as a restart from zero."""
    lines, cut = _read_tail(path)
    baseline_known = not cut
    prev: dict | None = None
    model: str | None = None
    total = {f: 0 for f in _CODEX_FIELDS}
    per_model: dict[str, dict] | None = {}
    for line in lines:
        if '"turn_context"' not in line and '"token_count"' not in line:
            continue
        d = _json(line)
        payload = (d or {}).get("payload")
        if not isinstance(payload, dict):
            continue
        if d.get("type") == "turn_context":
            m = payload.get("model")
            model = m if isinstance(m, str) and m else model
            continue
        if d.get("type") != "event_msg" or payload.get("type") != "token_count":
            continue
        info = payload.get("info")
        ts = _ts(d.get("timestamp"))
        if not isinstance(info, dict) or ts is None:
            continue
        cur = _codex_raw(info.get("total_token_usage"))
        if ts < since:
            prev, baseline_known = cur, True
            continue
        if not baseline_known:
            raise _Unattributable(path.name)
        if prev is None or any(cur[f] < prev[f] for f in _CODEX_FIELDS):
            step = cur
        else:
            step = {f: cur[f] - prev[f] for f in _CODEX_FIELDS}
        prev = cur
        for f in _CODEX_FIELDS:
            total[f] += step[f]
        if per_model is not None:
            if model is None:
                per_model = None   # a step we can't charge to a model
            else:
                acc = per_model.setdefault(model, {f: 0 for f in _CODEX_FIELDS})
                for f in _CODEX_FIELDS:
                    acc[f] += step[f]
    models = None if per_model is None else {
        name: _flat(_codex_tally(raw, None), None)
        for name, raw in per_model.items() if any(raw.values())
    }
    return _codex_tally(total, models)


def _rollout_meta(path: Path) -> dict | None:
    """A rollout's identity from its first line (`session_meta`): its thread
    id, the root thread it belongs to (itself, or the thread a subagent was
    spawned under), whether it is a subagent, and its cwd."""
    try:
        with path.open("rb") as f:
            first = f.readline(MAX_FIRST_LINE_BYTES)
    except OSError:
        return None
    d = _json(first.decode("utf-8", "replace"))
    if d is None or d.get("type") != "session_meta":
        return None
    p = d.get("payload")
    if not isinstance(p, dict) or not isinstance(p.get("id"), str):
        return None
    source = p.get("source")
    spawn = source.get("subagent") if isinstance(source, dict) else None
    parent = None
    if isinstance(spawn, dict):
        parent = (spawn.get("thread_spawn") or {}).get("parent_thread_id")
    root = p.get("session_id") or parent or p["id"]
    return {"id": p["id"], "root": root, "subagent": spawn is not None,
            "cwd": p.get("cwd")}


def _recent_rollouts(sessions_dir: Path, since: float) -> list[Path]:
    """Rollout files written to since `since` (a little slack for coarse
    mtimes). A file untouched since then holds nothing of the session's."""
    found: list[Path] = []
    seen = 0
    for dirpath, dirnames, filenames in os.walk(sessions_dir):
        seen += len(dirnames) + len(filenames)
        for name in filenames:
            if not (name.startswith("rollout-") and name.endswith(".jsonl")):
                continue
            p = Path(dirpath) / name
            try:
                if p.stat().st_mtime >= since - 2.0:
                    found.append(p)
            except OSError:
                continue
        if seen > MAX_SCAN_ENTRIES:
            raise _Unattributable("too many rollouts to scan")
    return found


class CodexRollouts:
    """Usage of codex sessions, read back from the rollout files codex
    writes under `$CODEX_HOME/sessions/YYYY/MM/DD/`.

    Each rollout is one thread (subagents get their own, pointing at the
    root thread via `session_meta.session_id`; a parent's counters exclude
    its subagents'), and its `token_count` events carry the thread's
    cumulative counters — see `_rollout_delta`. Which rollouts are the
    session's depends on who else writes to that dir:

      - `exclusive` — nobody: PooledCodexClient's CODEX_HOME belongs to its
        shared lease alone, so every rollout touched since `started_at` is
        ours (whichever holder ran it).
      - a known thread id — sp-codex with its output piped: `codex exec`
        names its thread before doing anything (`{"type":"thread.started"}`
        with `--json`, a `session id:` header line without), see `on_line`.
        That thread and its subagents, nothing else — exact, however many
        other sessions share the dir.
      - neither — an interactive sp-codex, whose output we can't read, in a
        `sessions` dir shared with every other sp-codex. The one root thread
        that started in our cwd and was active during the session is taken
        to be ours; none or several, and we report nothing. Not airtight: an
        idle session can be credited a concurrent one's thread in the same
        cwd, and `codex -C <dir>` sessions never match.

    `--ephemeral` writes no rollout; with `--json`, `turn.completed`'s usage
    stands in for the thread then.
    """

    def __init__(self, codex_home: Path, started_at: float, *,
                 cwd: str | None = None, exclusive: bool = False):
        self._home = codex_home
        self._started_at = started_at
        self._cwd = cwd
        self._exclusive = exclusive
        self.thread_id: str | None = None
        self._exec_usage: dict | None = None

    def on_line(self, line: str) -> None:
        text = line.strip()
        if text.startswith("{"):
            d = _json(text)
            if d is None:
                return
            kind = d.get("type")
            if kind == "thread.started":
                self._adopt(d.get("thread_id"))
            elif kind == "turn.completed" and self.thread_id is not None:
                # One turn per `codex exec` run, so latest == total whether
                # codex means it per turn or per thread.
                self._exec_usage = _codex_tally(_codex_raw(d.get("usage")), None)
            return
        m = _SESSION_ID_LINE.match(text)
        if m:
            self._adopt(m.group(1))

    def _adopt(self, tid: Any) -> None:
        # First id only: `codex exec` runs one root thread, and its header
        # comes before any model output that could imitate one.
        if self.thread_id is None and isinstance(tid, str) and _ID_RE.fullmatch(tid):
            self.thread_id = tid

    def total(self) -> dict | None:
        try:
            files = _recent_rollouts(self._home / "sessions", self._started_at)
            metas = [(p, m) for p in files if (m := _rollout_meta(p)) is not None]
            if self._exclusive:
                chosen = [p for p, _ in metas]
            elif self.thread_id is not None:
                chosen = [p for p, m in metas if self.thread_id in (m["id"], m["root"])]
                if not chosen:
                    return merge(self._exec_usage, None)
            else:
                chosen = self._guess(metas)
            if not chosen:
                return None
            if len(chosen) > MAX_FILES:
                raise _Unattributable(f"{len(chosen)} rollouts")
            total = None
            for p in chosen:
                total = merge(total, _rollout_delta(p, self._started_at))
            return total
        except _Unattributable as e:
            log.debug("usage: cannot attribute codex rollouts (%s)", e)
            return None

    def _guess(self, metas: list[tuple[Path, dict]]) -> list[Path]:
        if self._cwd is None:
            return []
        active = set()
        for p, m in metas:
            if m["subagent"] or m["cwd"] != self._cwd:
                continue
            delta = _rollout_delta(p, self._started_at)
            if any(delta[f] for f in TOKEN_FIELDS):
                active.add(m["id"])
        if len(active) != 1:
            if active:
                log.debug("usage: %d codex threads active in %s; not guessing",
                          len(active), self._cwd)
            return []
        (root,) = active
        return [p for p, m in metas if root in (m["id"], m["root"])]


# ============================================================ sp-* CLIs


class SessionUsage(Protocol):
    """What `run_session` drives: `start` once the child is spawned,
    `on_line` per line of its output (only when it is piped), `poll` every
    POLL_INTERVAL_S while it runs, `total` once after it exits (off-loop, via
    `collect`)."""

    def start(self, *, config_dir: Path, pid: int, started_at: float,
              cwd: str) -> None: ...

    def on_line(self, line: str) -> None: ...

    def poll(self) -> None: ...

    def total(self) -> dict | None: ...


class ClaudeCliUsage:
    """sp-claude: result lines off a json / stream-json stdout when there are
    any (cumulative, with cost), else the transcripts.

    Result lines are only trusted when `--output-format` says stdout is JSON:
    in text mode stdout is the model's raw words, and those could spell out a
    result line."""

    def __init__(self, args: tuple[str, ...]):
        self._results = ClaudeResults(resumed=_claude_resumes(args))
        self._parse = _claude_output_format(args) in ("json", "stream-json")
        self._transcripts: ClaudeTranscripts | None = None

    def start(self, *, config_dir: Path, pid: int, started_at: float,
              cwd: str) -> None:
        self._transcripts = ClaudeTranscripts(config_dir, pid, started_at)

    def on_line(self, line: str) -> None:
        if self._parse and ('"result"' in line or '"conversation_reset"' in line):
            d = _json(line.strip())
            if d is not None:
                self._results.observe_json(d)

    def poll(self) -> None:
        if self._transcripts is not None:
            self._transcripts.poll()

    def total(self) -> dict | None:
        if self._results.seen:
            return self._results.total()
        return self._transcripts.total() if self._transcripts else None


class CodexCliUsage:
    """sp-codex: see CodexRollouts. The rollouts are the record either way;
    piped output only tells us which of them is ours."""

    def __init__(self, args: tuple[str, ...]):
        self._rollouts: CodexRollouts | None = None

    def start(self, *, config_dir: Path, pid: int, started_at: float,
              cwd: str) -> None:
        self._rollouts = CodexRollouts(config_dir, started_at, cwd=cwd)

    def on_line(self, line: str) -> None:
        if self._rollouts is not None:
            self._rollouts.on_line(line)

    def poll(self) -> None:
        return None

    def total(self) -> dict | None:
        return self._rollouts.total() if self._rollouts else None
