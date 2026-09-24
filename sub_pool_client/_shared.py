"""Cross-process shared credential directory for PooledClient.

A well-known directory per (pool_url, api_key, user_id, required_model)
combo holds:

    <dir>/.credentials.json    ← CLI reads/writes (matches `claude /login` format)
    <dir>/meta.json            ← {"lease_id", "holders": [pid, ...]}
    <dir>/.meta.lock           ← flock held only while mutating meta.json

Coordination rules:

- Credential refreshes are coordinated AT THE OS LEVEL by the CLI itself
  (atomic rename + 401-retry-and-reread — same as native CC multi-process).
  We never hold the flock while the CLI is running.

- meta.json has a `holders` list of pids. First arriver leases from the
  pool; later arrivers join (refcount via list length). Dead pids are
  filtered on every meta mutation so a crash doesn't leak a slot forever.

- When `holders` goes empty, the last holder reads `.credentials.json`
  (possibly rotated by the CLI mid-run) and posts it back to the pool as
  `updated_credentials` on DELETE, then removes the dir.

- Dirs are 0700 and every file in them 0600. A dir left behind by a crash
  is removed by `sweep_stale_dirs` once it is old and all its holders are
  dead; `locked_meta` retries on a fresh dir if one is removed while it
  waits for the lock.
"""
from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import random
import shutil
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Awaitable, Callable
from urllib.parse import urlsplit

import httpx

log = logging.getLogger(__name__)


DEFAULT_ROOT = Path(
    os.environ.get("SUB_POOL_CLIENT_DIR")
    or Path.home() / ".sub-pool" / "client"
)


# ---- pool URL -----------------------------------------------------------
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def check_pool_url(url: str) -> None:
    """Raise ValueError for a plain-http pool that isn't on this machine:
    every request carries the API key, and lease responses carry OAuth
    access tokens."""
    parts = urlsplit(url)
    if parts.scheme == "http" and parts.hostname not in _LOOPBACK_HOSTS:
        raise ValueError(
            f"refusing plain http:// pool URL {url!r}: the API key and leased "
            f"credentials would travel unencrypted. Use https:// (http:// is "
            f"only allowed for localhost / 127.0.0.1 / ::1)."
        )


# ---- lease requests -----------------------------------------------------
class LeaseRequestId:
    """Idempotency key for `POST /credentials/lease`.

    The pool dedupes issuance on (request_id, api_key, provider): a POST that
    repeats the id of a lease it already made returns that lease instead of
    minting another. So one id covers one logical acquisition across all its
    attempts, and is retired only once a 2xx proves we hold the lease. Any
    other outcome keeps it: a timeout, a dropped connection or a proxy 5xx can
    hide a lease the server did commit, and reusing the id after a genuine
    rejection is harmless (the pool finds no active lease under it). Without
    this, every timed-out attempt the server had committed became an orphan
    held until the TTL reaper.
    """

    def __init__(self) -> None:
        self.value = _new_request_id()

    def retire(self) -> None:
        self.value = _new_request_id()


def _new_request_id() -> str:
    return "req-" + uuid.uuid4().hex


RELEASE_ATTEMPTS = 3
RELEASE_BACKOFF_S = 0.5


async def release_lease(
    send: Callable[[], Awaitable[httpx.Response]], lease_id: str,
) -> bool:
    """DELETE a lease via `send`, retrying transient failures. True once the
    pool confirms it (2xx, or 404/410: already gone).

    Never raises — callers are cleanup paths. A lease we fail to release is
    held until the pool's TTL reaper; that used to happen silently, which is
    how leases piled up unnoticed, so the final failure is a WARNING.
    """
    problem = ""
    for attempt in range(RELEASE_ATTEMPTS):
        if attempt:
            await asyncio.sleep(RELEASE_BACKOFF_S * 2 ** (attempt - 1))
        try:
            r = await send()
        except Exception as e:  # noqa: BLE001
            problem = f"{type(e).__name__}: {e}"
            continue
        if r.is_success or r.status_code in (404, 410):
            return True
        problem = f"HTTP {r.status_code}"
        if r.status_code < 500 and r.status_code not in (408, 429):
            break  # a definitive rejection (e.g. 401/403) won't change on retry
    log.warning(
        "failed to release lease %s (%s); the pool's TTL reaper will end it",
        lease_id, problem,
    )
    return False


# ---- polling cadence ----------------------------------------------------
JITTER = 0.2


def jittered(seconds: float) -> float:
    """`seconds` ±20%, so clients started together don't poll in lockstep."""
    return seconds * random.uniform(1 - JITTER, 1 + JITTER)


# ---- token-refresh scheduling -------------------------------------------
# Shared by the sp-claude/sp-codex CLI refresh loop (_cli_core) AND both SDK
# poll loops (PooledClient / PooledCodexClient). Kept in ONE place because the
# three drifted apart twice: the CLI once regressed to a fixed cadence that
# outlived short-lived tokens (→ mid-session 401), and the exp<=0 fallback below
# once existed only in the CLI (→ codex api-key SDK leases polling every 60s).
POLL_SLACK_S = 600.0        # refresh this long BEFORE the token actually expires
POLL_MIN_SLEEP_S = 60.0     # floor so tiny-TTL tests / churn don't bash the pool
POLL_FALLBACK_S = 50 * 60.0  # no usable expiry (token_expires_at<=0, e.g. a codex
                             # api-key lease / an exp-less JWT) → fixed cadence


def next_refresh_sleep(
    expires_at: float, now: float, *,
    slack_s: float = POLL_SLACK_S,
    min_sleep_s: float = POLL_MIN_SLEEP_S,
    fallback_s: float = POLL_FALLBACK_S,
) -> float:
    """Seconds to sleep before the next pool token refresh.

    Anchored to the token's ACTUAL expiry: wake `slack_s` before it, floored at
    `min_sleep_s`. When `expires_at` carries no usable value (<=0), fall back to
    a fixed cadence instead of anchoring to a long-past deadline — that would
    refresh every `min_sleep_s` forever (a needless churn storm that hit codex
    api-key leases, whose token has no expiry).

    The two fixed cadences are jittered so a fleet that failed together (pool
    outage) doesn't retry together; the expiry-anchored one is exact, since
    jitter there could overshoot the token's life."""
    if expires_at <= 0:
        return jittered(fallback_s)
    anchored = expires_at - now - slack_s
    return anchored if anchored > min_sleep_s else jittered(min_sleep_s)


def dir_key(
    *,
    pool_url: str,
    api_key: str,
    provider: str = "claude",
    user_id: str | None = None,
    required_model: str | None = None,
) -> str:
    """Deterministic short hash. Same inputs → same shared dir → two
    concurrent PooledClient instances meet."""
    parts = [
        provider or "claude",
        pool_url.rstrip("/"),
        api_key,
        user_id or "",
        required_model or "",
    ]
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()
    return digest[:24]


def shared_dir(key: str, root: Path | None = None) -> Path:
    d = (root or DEFAULT_ROOT) / key
    d.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Also tightens a dir an older client created 0755.
    os.chmod(d, 0o700)
    return d


@dataclass
class Meta:
    lease_id: str | None = None
    holders: list[int] | None = None
    # Which holder owns the /token poll task. If this pid dies, the next
    # arrival / heartbeat tick hands leadership to any live holder.
    # Stays None when no leader has been elected yet (e.g. during
    # bring-up, or after a graceful leader exit before a takeover).
    poll_leader: int | None = None
    # Account name returned by the pool for this lease. Persisted so
    # late-arriving holders that JOIN an existing lease can report the
    # right account through `client.account` — they never saw the
    # original `POST /credentials/lease` response themselves.
    account: str | None = None
    # Immutable server-side account id for this lease (surrogate PK). Persisted
    # alongside `account` so a same-account check survives a rename, and so a
    # holder promoted to leader after a health swap can re-sync to the CURRENT
    # lease's identity from here rather than trusting its own stale cache.
    account_id: int | None = None
    # Current access_token expiry (epoch seconds) for the shared lease.
    # Persisted so a promoted leader schedules its token poll off the CURRENT
    # lease, not the one it originally joined (a health swap advances this).
    token_expires_at: float = 0.0

    @classmethod
    def from_json(cls, d: dict) -> "Meta":
        return cls(
            lease_id=d.get("lease_id"),
            holders=list(d.get("holders") or []),
            poll_leader=d.get("poll_leader"),
            account=d.get("account"),
            account_id=d.get("account_id"),
            token_expires_at=float(d.get("token_expires_at") or 0.0),
        )

    def to_json(self) -> dict:
        return {
            "lease_id": self.lease_id,
            "holders": self.holders or [],
            "poll_leader": self.poll_leader,
            "account": self.account,
            "account_id": self.account_id,
            "token_expires_at": self.token_expires_at,
        }


def _pid_alive(pid: int) -> bool:
    """True if the pid currently names a running process."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but we don't own it — rare on a single-user box,
        # but signal "alive" to avoid clobbering someone else's lease.
        return True
    return True


# Per-process serialization on top of fcntl.flock. flock is
# per-process, so two asyncio tasks in the same process would both
# acquire LOCK_EX "concurrently" without blocking each other. An
# asyncio.Lock keyed by the shared-dir path plugs that hole.
_process_locks: dict[str, asyncio.Lock] = {}


def _process_lock(key: str) -> asyncio.Lock:
    lock = _process_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _process_locks[key] = lock
    return lock


@asynccontextmanager
async def locked_meta(dir_: Path) -> AsyncIterator[Meta]:
    """Exclusive access to the shared dir's meta.json, both within this
    process (asyncio.Lock) and across processes (fcntl.flock). Caller
    mutates the yielded Meta in place; we persist on exit.
    """
    lock_path = dir_ / ".meta.lock"
    meta_path = dir_ / "meta.json"

    async with _process_lock(str(dir_.resolve())):
        while True:
            dir_.mkdir(mode=0o700, parents=True, exist_ok=True)
            # Use O_CREAT so the first arriver creates the lockfile.
            fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
            try:
                # flock is sync & blocking; release the loop while waiting so
                # a second consumer in the same process isn't completely
                # stuck if someone's running a long inter-process critical
                # section next door.
                await asyncio.to_thread(fcntl.flock, fd, fcntl.LOCK_EX)
            except BaseException:
                os.close(fd)
                raise
            if os.fstat(fd).st_nlink:
                break
            # The dir was removed (failed entry / stale sweep) while we waited
            # for its lock: start over on a fresh one.
            os.close(fd)
        try:
            try:
                raw = meta_path.read_text() if meta_path.exists() else "{}"
                data = json.loads(raw or "{}")
            except (OSError, ValueError):
                data = {}

            meta = Meta.from_json(data)
            # Filter dead pids on every read — orphan cleanup is tied to
            # the critical section so we never race with a rejoin.
            if meta.holders:
                meta.holders = [p for p in meta.holders if _pid_alive(p)]
            # A leader pid that no longer exists = no leader. The next
            # heartbeat / enter will elect a fresh one from live holders.
            if meta.poll_leader is not None and not _pid_alive(meta.poll_leader):
                meta.poll_leader = None

            try:
                yield meta
            except BaseException:
                # A failed first entry (lease POST / credential write raised)
                # leaves nothing worth keeping; remove the dir while we still
                # hold its lock rather than leak an empty one per failure.
                if not meta.lease_id and not meta.holders:
                    cleanup_dir(dir_)
                raise

            payload = json.dumps(meta.to_json(), indent=2)
            tmp = meta_path.with_suffix(meta_path.suffix + ".tmp")
            tmp_fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            os.fchmod(tmp_fd, 0o600)
            with os.fdopen(tmp_fd, "w") as f:
                f.write(payload)
            os.replace(tmp, meta_path)
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)


def bundle_to_cli_schema(bundle: dict) -> dict:
    """Translate our flat snake_case bundle (what pool stores in
    token.json / what oauth_anthropic emits) into the nested camelCase
    shape the `claude` CLI's `.credentials.json` uses.

    Pool shape (seconds since epoch):
        {"access_token", "refresh_token", "expires_at",
         "scope" (str), "subscription_type", ...}

    CLI shape (milliseconds since epoch, camelCase, nested):
        {"claudeAiOauth": {
            "accessToken", "refreshToken", "expiresAt",
            "scopes" (list[str]), "subscriptionType"
        }}
    """
    expires_s = float(bundle.get("expires_at") or 0)
    expires_ms = int(expires_s * 1000) if expires_s else 0
    scope_str = bundle.get("scope") or ""
    scopes = scope_str.split() if isinstance(scope_str, str) else list(scope_str or [])
    return {
        "claudeAiOauth": {
            "accessToken": bundle.get("access_token") or "",
            "refreshToken": bundle.get("refresh_token") or "",
            "expiresAt": expires_ms,
            "scopes": scopes,
            "subscriptionType": bundle.get("subscription_type") or "",
        }
    }


def bundle_from_cli_schema(cli_file_contents: dict) -> dict:
    """Inverse of bundle_to_cli_schema. Used when we read the (possibly
    rotated) file back out on release to post `updated_credentials` to
    the pool."""
    nested = cli_file_contents.get("claudeAiOauth") or {}
    expires_ms = int(nested.get("expiresAt") or 0)
    expires_s = expires_ms / 1000.0 if expires_ms else 0.0
    scopes = nested.get("scopes") or []
    scope_str = " ".join(scopes) if isinstance(scopes, list) else str(scopes)
    return {
        "access_token": nested.get("accessToken") or "",
        "refresh_token": nested.get("refreshToken") or "",
        "expires_at": expires_s,
        "scope": scope_str,
        "subscription_type": nested.get("subscriptionType") or "",
    }


def write_credentials(dir_: Path, bundle: dict) -> None:
    """Atomic .credentials.json write in the CLI's expected schema,
    0600 perms. `bundle` is our flat snake_case shape."""
    p = dir_ / ".credentials.json"
    tmp = p.with_suffix(p.suffix + ".tmp")
    cli_payload = bundle_to_cli_schema(bundle)
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(cli_payload, f, indent=2)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    os.replace(tmp, p)


def read_credentials(dir_: Path) -> dict | None:
    """Read .credentials.json (CLI schema) and return our flat bundle
    shape. None if the file is missing / malformed."""
    p = dir_ / ".credentials.json"
    if not p.exists():
        return None
    try:
        raw = json.loads(p.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(raw, dict) or "claudeAiOauth" not in raw:
        return None
    return bundle_from_cli_schema(raw)


def write_codex_auth(dir_: Path, auth_json: dict) -> None:
    """Atomic Codex auth.json write in $CODEX_HOME."""
    p = dir_ / "auth.json"
    tmp = p.with_suffix(p.suffix + ".tmp")
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(auth_json, f, indent=2)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    os.replace(tmp, p)


def read_codex_auth(dir_: Path) -> dict | None:
    p = dir_ / "auth.json"
    if not p.exists():
        return None
    try:
        raw = json.loads(p.read_text())
    except (OSError, ValueError):
        return None
    return raw if isinstance(raw, dict) else None


def cleanup_dir(dir_: Path) -> None:
    """Remove the shared dir. Idempotent."""
    try:
        shutil.rmtree(dir_, ignore_errors=True)
    except Exception:  # noqa: BLE001
        pass


# A dir is only swept once it has also been untouched this long: pid liveness
# alone can be wrong (another container's pids look dead here, a reused pid
# looks alive).
STALE_DIR_AGE_S = 24 * 3600
_swept = False


async def sweep_stale_dirs_once() -> None:
    """Run `sweep_stale_dirs` on DEFAULT_ROOT, at most once per process."""
    global _swept
    if _swept:
        return
    _swept = True
    await asyncio.to_thread(sweep_stale_dirs, DEFAULT_ROOT)


def sweep_stale_dirs(root: Path, max_age_s: float = STALE_DIR_AGE_S) -> None:
    """Best-effort removal of shared dirs a crashed process left behind —
    each may still hold a credential file. Removes a dir only if its mtime is
    older than `max_age_s` AND every holder in its meta.json is dead, checked
    under the dir's lock (skipped if busy). Server-side leases are left to the
    pool's TTL reaper."""
    cutoff = time.time() - max_age_s
    try:
        dirs = [d for d in root.iterdir() if d.is_dir() and not d.is_symlink()]
    except OSError:
        return
    for d in dirs:
        try:
            if d.stat().st_mtime > cutoff:
                continue
            fd = os.open(str(d / ".meta.lock"), os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                os.close(fd)
                continue
            try:
                try:
                    holders = json.loads((d / "meta.json").read_text()).get("holders")
                except (OSError, ValueError):
                    holders = None
                if not any(_pid_alive(p) for p in holders or []):
                    cleanup_dir(d)
            finally:
                os.close(fd)
        except Exception as e:  # noqa: BLE001
            log.debug("stale dir sweep skipped %s: %s", d, e)


# The DEFAULT_ROOT constant pulls from $SUB_POOL_CLIENT_DIR at import
# time; tests that want isolation should clear the env before import or
# pass `root=` explicitly.
__all__ = [
    "DEFAULT_ROOT",
    "Meta",
    "cleanup_dir",
    "dir_key",
    "locked_meta",
    "read_credentials",
    "read_codex_auth",
    "shared_dir",
    "write_codex_auth",
    "write_credentials",
]
