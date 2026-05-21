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
"""
from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
import shutil
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator


DEFAULT_ROOT = Path(
    os.environ.get("SUB_POOL_CLIENT_DIR")
    or Path.home() / ".sub-pool" / "client"
)


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
    d.mkdir(parents=True, exist_ok=True)
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

    @classmethod
    def from_json(cls, d: dict) -> "Meta":
        return cls(
            lease_id=d.get("lease_id"),
            holders=list(d.get("holders") or []),
            poll_leader=d.get("poll_leader"),
            account=d.get("account"),
        )

    def to_json(self) -> dict:
        return {
            "lease_id": self.lease_id,
            "holders": self.holders or [],
            "poll_leader": self.poll_leader,
            "account": self.account,
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
        # Use O_CREAT so the first arriver creates the lockfile.
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            # flock is sync & blocking; release the loop while waiting so
            # a second consumer in the same process isn't completely
            # stuck if someone's running a long inter-process critical
            # section next door.
            await asyncio.to_thread(fcntl.flock, fd, fcntl.LOCK_EX)
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

            yield meta

            payload = json.dumps(meta.to_json(), indent=2)
            tmp = meta_path.with_suffix(meta_path.suffix + ".tmp")
            tmp.write_text(payload)
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
