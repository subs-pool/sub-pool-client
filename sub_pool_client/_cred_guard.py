"""Keep the leased credential file intact for the session's lifetime.

`claude` says "Not logged in · Please run /login" for exactly one family of
reasons: the credential it reads is missing, empty, unparseable, or carries
no scopes. A token that is merely expired or revoked gets a `401` instead —
that is `_auth_watch`'s job, not this module's.

We write that file atomically and never delete it while the child runs, yet
users running several sessions at once still see the message now and then,
and it has not reproduced locally. So this module does two things without
waiting for the root cause:

  * **Recover.** Every `interval_s`, and right away when the child prints the
    message, check the file against the lease we hold. If it is wrong, write
    it again from the lease. A long-running session gets back on its feet on
    its next request instead of being stuck until restart.
  * **Record.** Every finding is a `cred_check` event in the session log
    (`_session_log`): what the file looked like (exists, size, parses,
    scopes, expiry, whether its token is the leased one — never the token
    itself), what triggered the check, and what we did. A check the child
    triggered is recorded even when the file was fine — that rules the file
    out, which is evidence too.

Only providers that define `credential_problem` are guarded (claude today).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from sub_pool_client._session_log import SessionLog

log = logging.getLogger(__name__)

INTERVAL_S = 10.0
# The only message `claude` prints for a credential it can't use at all.
_NOT_LOGGED_IN = "not logged in"
_CONTENT_TYPES = frozenset({"assistant", "user"})


def inspect_claude_credentials(path: Path, lease: dict) -> dict[str, Any]:
    """What `path` holds, compared with `lease`. Never includes a secret."""
    out: dict[str, Any] = {
        "dir_exists": path.parent.is_dir(),
        "exists": path.exists(),
        "is_symlink": path.is_symlink(),
    }
    if not out["exists"]:
        return out
    try:
        st = path.stat()
        out["size"] = st.st_size
        out["age_s"] = round(time.time() - st.st_mtime, 1)
        raw = path.read_text(encoding="utf-8")
    except OSError as e:
        out["read_error"] = type(e).__name__
        return out
    try:
        data = json.loads(raw)
    except ValueError:
        out["parses"] = False
        return out
    out["parses"] = True
    oauth = data.get("claudeAiOauth") if isinstance(data, dict) else None
    out["has_oauth"] = isinstance(oauth, dict)
    if not isinstance(oauth, dict):
        out["keys"] = sorted(data)[:8] if isinstance(data, dict) else type(data).__name__
        return out
    scopes = oauth.get("scopes")
    out["scopes"] = len(scopes) if isinstance(scopes, list) else None
    out["has_inference_scope"] = isinstance(scopes, list) and "user:inference" in scopes
    exp = oauth.get("expiresAt")
    if isinstance(exp, (int, float)):
        out["expires_in_s"] = round(exp / 1000 - time.time(), 1)
    out["has_token"] = bool(oauth.get("accessToken"))
    out["token_matches_lease"] = oauth.get("accessToken") == lease.get("access_token")
    return out


def claude_credential_problem(diag: dict[str, Any]) -> str | None:
    """Why `claude` would refuse (or misuse) the file `diag` describes."""
    if not diag.get("dir_exists"):
        return "session_dir_missing"
    if not diag.get("exists"):
        return "missing"
    if "read_error" in diag:
        return "unreadable"
    if not diag.get("size"):
        return "empty"
    if not diag.get("parses"):
        return "unparseable"
    if not diag.get("has_oauth"):
        return "no_oauth_block"
    if not diag.get("has_token"):
        return "no_token"
    if not diag.get("has_inference_scope"):
        return "no_inference_scope"
    if not diag.get("token_matches_lease"):
        return "token_not_leased"
    return None


def looks_like_not_logged_in(line: str) -> bool:
    """Did the child just say it has no usable credential? Never raises."""
    text = line.strip()
    if not text or _NOT_LOGGED_IN not in text.lower():
        return False
    if text[0] == "{":
        try:
            payload = json.loads(text)
        except ValueError:
            return True
        if isinstance(payload, dict):
            kind = payload.get("type")
            if isinstance(kind, str) and kind.lower() in _CONTENT_TYPES:
                return False  # the model talking about logins
    return True


class CredentialGuard:
    """Periodic + on-demand check-and-rewrite of one session's credential."""

    def __init__(
        self, *, path: Path, current: dict, provider_name: str,
        inspect: Callable[[Path, dict], dict[str, Any]],
        problem: Callable[[dict[str, Any]], str | None],
        rewrite: Callable[[], None],
        slog: "SessionLog", interval_s: float = INTERVAL_S,
    ):
        self.path = path
        self.current = current
        self.provider_name = provider_name
        self._inspect = inspect
        self._problem = problem
        self._rewrite = rewrite
        self.slog = slog
        self.interval_s = interval_s
        # (lease, problem, outcome) already recorded by a periodic check: one
        # a rewrite can't fix (the lease itself carries no scope, say) would
        # otherwise log the same line every interval for the whole session.
        self._seen: set[tuple[Any, str, str]] = set()
        self._last_signal = float("-inf")

    def on_child_signal(self, trigger: str, *, cooldown_s: float = 5.0) -> None:
        """The child said it isn't logged in: check right now, inline.

        Inline rather than via the loop because `claude -p` exits right after
        printing it — a deferred check would be cancelled with the session
        and the evidence lost. It is one small file read. Debounced, since a
        burst of failing requests prints the line once each.
        """
        now = time.monotonic()
        if now - self._last_signal < cooldown_s:
            return
        self._last_signal = now
        self.check(trigger)

    def check(self, trigger: str = "periodic") -> str | None:
        """One check; rewrites and records on a problem. Returns the problem.

        Synchronous on purpose: the inspection, the rewrite and the read of
        `current["lease"]` happen with no await in between, so a refresh or a
        swap (which update the file and `current` together) can't interleave.
        """
        lease = self.current["lease"]
        try:
            diag = self._inspect(self.path, lease)
            problem = self._problem(diag)
        except Exception as e:  # noqa: BLE001 — a guard must never kill a session
            log.debug("credential check raised (ignored): %s", e)
            return None
        if problem is None and trigger == "periodic":
            return None
        action = "none"
        if problem is not None:
            try:
                self._rewrite()
                after = self._problem(self._inspect(self.path, lease))
                action = "rewritten" if after is None else f"rewrite_still_{after}"
            except Exception as e:  # noqa: BLE001
                action = f"rewrite_failed:{type(e).__name__}"
        if trigger == "periodic" and problem is not None:
            key = (lease.get("lease_id"), problem, action)
            if key in self._seen:
                return problem
            self._seen.add(key)
        self.slog.event("cred_check", trigger=trigger, problem=problem,
                        action=action, file=diag)
        if problem is not None:
            log.info("sp-%s: credential file was %s; %s", self.provider_name,
                        problem, action)
        return problem

    async def run(self) -> None:
        while True:
            self.check()
            await asyncio.sleep(self.interval_s)
