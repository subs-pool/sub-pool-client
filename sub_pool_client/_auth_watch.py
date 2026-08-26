"""Detect an upstream auth failure in the spawned CLI's output, and recover.

## The gap this closes

`run_session` had exactly one recovery trigger: the health watcher noticing
the pool had marked our account unhealthy. That covers a *pool-visible*
failure — quota COOLING, a dead refresh_token flipped INVALID, a reaped
lease. It does not cover the case where the **access token we already
handed the child is dead upstream while the pool still sees the account as
perfectly healthy**.

That happens because the pool's own probe authenticates through the refresh
chain: if the grant is alive, a refresh mints a working token and the
account reports AVAILABLE forever — even though the *specific* token sitting
in `.credentials.json` was revoked (a "log out other sessions", an
administrative token revocation, a rotation we lost the race on). The child
401s on every request, `/health` says `healthy: true` on every tick, and
nothing ever swaps. The session stays broken until its natural end.

## Why refresh-first is the whole trick

The obvious reaction to "the token is dead" is "swap accounts". That is the
wrong first move, and it is also the move that would make this module
dangerous: to swap on a detection you must be *sure* about the detection,
and we are reading a byte stream that also carries model output.

But an access-token revocation is precisely what a plain refresh fixes —
POST `/token`, the pool re-signs off the still-valid refresh_token, the new
token lands on disk, the child picks it up on its next request. So the
escalation ladder is:

    refresh  →  (still failing?)  →  report-error + swap

and a **false positive costs one redundant `/token` call**, not a spurious
account rotation. That is what lets the detector below stay simple and
slightly over-eager instead of clever and brittle.

## Why the detector can be structural here

The consumer that motivated this runs `claude` with stream-json over a pipe,
so output is newline-delimited JSON and we can match on *shape* rather than
substring-scanning prose. That matters: `sub_pool.accounts.classify_error`
already documents that substring-matching free text for words like `oauth`
produces constant false positives, and raw CLI stdout is a far worse
haystack than an error field — it carries whatever the model just wrote.

So: JSON lines whose `type` is `assistant`/`user` are skipped outright (that
is model and caller content, never transport state), and everything else is
matched on the structured error keys first, with a deliberately narrow
literal list as the fallback for non-JSON output and stderr.

## One behavioural difference, worth knowing

Interposing is invisible to a child that was already writing to a pipe — it
sees a pipe either way. But stdout and stderr, which the caller may have
merged onto one fd (`sp-claude ... > log 2>&1`), are now carried by two
independent pumps. Relative ordering BETWEEN the two streams is no longer
the kernel's; within each stream it is unchanged. Consumers that read
stream-json off stdout are unaffected, which is the case this exists for.

## Why forwarding comes first

Piping the child's stdout puts this module on the critical path of the
caller's protocol. Every rule here follows from that: forward each chunk
before looking at it, never wait for a newline to forward, never let a
detector exception escape into the pump. A bug in detection must degrade to
"no recovery", never to "the session hangs".
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from typing import Any, Awaitable, Callable, IO

log = logging.getLogger(__name__)

# Cap on how much unterminated output we hold while looking for a newline.
# A stream-json line can legitimately be large (a big tool result), so this
# is generous; past it we drop from the FRONT and keep the tail, because a
# trailing error object is what we are looking for.
MAX_LINE_BYTES = 1 << 20

# Structured keys. `error_status` is what the claude CLI emits alongside an
# upstream HTTP failure; `error` carries the code.
_AUTH_STATUSES = (401, 403)
_AUTH_CODES = frozenset({
    "authentication_failed",
    "invalid_grant",
    "unauthorized",
    "not_authenticated",
})

# Fallback for non-JSON output and stderr. Kept SHORT and specific on
# purpose — every entry here should be a string that a transport layer
# emits and prose essentially never does. Bare words like "oauth",
# "credential" or "token" are exactly the false-positive generators
# classify_error warns about, and must not be added.
_AUTH_LITERALS = (
    "oauth access token has been revoked",
    "authentication_failed",
    "invalid_grant",
)

# stream-json message types that carry model / caller content rather than
# transport state. Their payloads routinely quote error JSON verbatim —
# never match inside them.
_CONTENT_TYPES = frozenset({"assistant", "user"})

_MAX_WALK_DEPTH = 6


def _walk_for_auth(node: Any, depth: int = 0) -> bool:
    """True if any nested object carries an auth-failure status or code."""
    if depth > _MAX_WALK_DEPTH:
        return False
    if isinstance(node, dict):
        status = node.get("error_status", node.get("status"))
        if isinstance(status, (int, float)) and int(status) in _AUTH_STATUSES:
            return True
        for key in ("error", "error_code", "code", "type"):
            v = node.get(key)
            if isinstance(v, str) and v.strip().lower() in _AUTH_CODES:
                return True
        return any(_walk_for_auth(v, depth + 1) for v in node.values())
    if isinstance(node, list):
        return any(_walk_for_auth(v, depth + 1) for v in node[:64])
    return False


def looks_like_auth_failure(line: str) -> bool:
    """Does this one line of child output indicate an upstream auth failure?

    Over-eagerness is survivable (one wasted refresh); a hang is not, so this
    never raises — a malformed line is simply not a match.
    """
    text = line.strip()
    if not text:
        return False
    try:
        if text[0] in "{[":
            payload = json.loads(text)
            if isinstance(payload, dict):
                kind = payload.get("type")
                if isinstance(kind, str) and kind.lower() in _CONTENT_TYPES:
                    # Model / caller content. An assistant message that
                    # happens to discuss a 401 is not a 401.
                    return False
            return _walk_for_auth(payload)
    except (ValueError, TypeError, RecursionError):
        pass  # not JSON — fall through to the literal scan
    except Exception:  # noqa: BLE001
        return False
    low = text.lower()
    return any(lit in low for lit in _AUTH_LITERALS)


def should_watch() -> bool:
    """Whether to pipe the child's output so we can watch it.

    Default: only when stdout is already NOT a tty. Interactive `claude` is a
    full-screen TUI that needs a real terminal — handing it a pipe would
    disable the UI it exists to draw. A piped stdout means the caller is
    already consuming output programmatically (the stream-json case), where
    interposing is invisible.

    `SUB_POOL_AUTH_WATCH=0|1` forces the decision either way.
    """
    raw = os.environ.get("SUB_POOL_AUTH_WATCH", "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    try:
        return not sys.stdout.isatty()
    except Exception:  # noqa: BLE001
        # A stdout without isatty() (pytest capture, odd wrappers) is not a
        # terminal we could break.
        return True


def stdout_sink() -> IO[bytes]:
    """Byte sink for forwarded child stdout.

    Indirected through a function (rather than referencing `sys.stdout.buffer`
    at the call site) so tests can substitute a buffer, and so a `sys.stdout`
    without `.buffer` — pytest capture, some embedding wrappers — degrades to
    the text stream instead of raising at wiring time.
    """
    return getattr(sys.stdout, "buffer", sys.stdout)


def stderr_sink() -> IO[bytes]:
    """Byte sink for forwarded child stderr. See `stdout_sink`."""
    return getattr(sys.stderr, "buffer", sys.stderr)


async def pump(
    reader: "asyncio.StreamReader",
    sink: IO[bytes],
    on_line: Callable[[str], None],
    *,
    chunk_size: int = 65536,
    max_line_bytes: int = MAX_LINE_BYTES,
) -> None:
    """Copy `reader` to `sink` verbatim, feeding complete lines to `on_line`.

    Forwarding happens BEFORE inspection and without waiting for a newline,
    so interposing costs no latency and a partial line never stalls the
    caller's stream. `on_line` exceptions are swallowed: detection is a
    best-effort side channel and must never break the pipe.
    """
    buf = bytearray()
    while True:
        try:
            data = await reader.read(chunk_size)
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception as e:  # noqa: BLE001
            log.debug("output pump read failed: %s", e)
            return
        if not data:
            return
        try:
            sink.write(data)
            sink.flush()
        except Exception as e:  # noqa: BLE001
            # The caller closed our stdout. Nothing useful left to do, but
            # keep draining so the child never blocks on a full pipe.
            log.debug("output pump write failed: %s", e)
            sink = _NULL_SINK
        buf += data
        while True:
            nl = buf.find(b"\n")
            if nl < 0:
                break
            line = bytes(buf[:nl])
            del buf[:nl + 1]
            try:
                on_line(line.decode("utf-8", "replace"))
            except Exception as e:  # noqa: BLE001
                log.debug("auth detector raised (ignored): %s", e)
        if len(buf) > max_line_bytes:
            # Keep the tail: a truncated giant line's END is where a
            # trailing error object would be.
            del buf[:len(buf) - max_line_bytes]


class _NullSink:
    def write(self, data: bytes) -> int:  # noqa: D102
        return len(data)

    def flush(self) -> None:  # noqa: D102
        return None


_NULL_SINK: Any = _NullSink()


class AuthRecovery:
    """Escalating response to auth-failure signals from the child.

    Ladder, per incident:

      1. `refresh()` — POST /token. Fixes the common case (the access token
         was revoked but the grant is alive) and is cheap enough that a
         false-positive detection is a non-event.
      2. If the refresh itself fails, or a second signal arrives within
         `escalate_after_s` of a refresh that was supposed to have fixed
         things, the credential is not merely stale: `report_error()` so the
         pool cools the account and routes OTHER leases away, then `swap()`
         onto a different one.

    Order matters in step 2. `report_error` must land before `swap`, because
    the pool's strategy is what decides which account the replacement lease
    gets — cooling the bad one first is the only thing stopping it from
    handing the same account straight back (`swap_credentials` only rejects
    an identical *lease_id*, not an identical account).

    `cooldown_s` absorbs the burst: one dead token produces an auth error on
    every in-flight request, and each retry after a fix produces more before
    the new credential is picked up. Acting once per burst is the point.
    """

    def __init__(
        self,
        *,
        refresh: Callable[[], Awaitable[bool]],
        report_error: Callable[[str, str], Awaitable[None]],
        swap: Callable[[str], Awaitable[None]],
        verbose: bool = False,
        cooldown_s: float = 20.0,
        escalate_after_s: float = 120.0,
        max_actions: int = 6,
        now: Callable[[], float] = time.monotonic,
    ):
        self._refresh = refresh
        self._report_error = report_error
        self._swap = swap
        self._verbose = verbose
        self._cooldown_s = cooldown_s
        self._escalate_after_s = escalate_after_s
        self._max_actions = max_actions
        self._now = now
        self._lock = asyncio.Lock()
        self._last_at = float("-inf")
        self._refreshed = False
        self._actions = 0
        self._exhausted_logged = False
        # Test/diagnostic visibility into what the ladder actually did.
        self.history: list[str] = []

    @property
    def actions(self) -> int:
        return self._actions

    async def on_signal(self, sample: str) -> None:
        """Handle one detected auth failure. Safe to call on every match."""
        async with self._lock:
            now = self._now()
            if self._actions >= self._max_actions:
                if not self._exhausted_logged:
                    self._exhausted_logged = True
                    log.warning(
                        "auth recovery gave up after %d attempts; the session "
                        "will keep failing. Last signal: %s",
                        self._actions, sample[:200],
                    )
                return
            if now - self._last_at < self._cooldown_s:
                # Same burst, or the previous fix has not been picked up yet.
                return

            escalate = (
                self._refreshed
                and (now - self._last_at) <= self._escalate_after_s
            )
            self._last_at = now
            self._actions += 1

            if not escalate:
                if self._verbose:
                    log.info("upstream auth failure; refreshing token (%s)",
                             sample[:120])
                ok = False
                try:
                    ok = await self._refresh()
                except Exception as e:  # noqa: BLE001
                    log.warning("auth-triggered refresh raised: %s", e)
                if ok:
                    self._refreshed = True
                    self.history.append("refresh")
                    return
                # Refresh could not produce a working credential — go
                # straight to the swap rather than burning the cooldown.
                self.history.append("refresh-failed")

            self._refreshed = False
            if self._verbose:
                log.info("auth failure persists after refresh; cooling "
                         "account and swapping")
            try:
                await self._report_error("401", "upstream rejected the leased "
                                                "credential (auth failure)")
            except Exception as e:  # noqa: BLE001
                log.warning("report_error raised: %s", e)
            try:
                await self._swap("upstream auth failure")
            except Exception as e:  # noqa: BLE001
                log.warning("auth-triggered swap raised: %s", e)
            self.history.append("swap")
