"""PooledClient — credential-broker mode with cross-process shared dir.

The pool is a credential broker, not a request proxy. This client:

  1. `__aenter__`:
       - flocks a shared dir keyed by (pool_url, api_key, user_id, model)
       - if nobody else holds a lease here: POST /credentials/lease,
         write the full OAuth bundle to $SHARED/.credentials.json
       - otherwise: just add our pid to the holders list and reuse the
         already-present .credentials.json
       - unlock
       - spawn `ClaudeSDKClient` locally with env[CLAUDE_CONFIG_DIR]=$SHARED

  2. CLI runs: multiple concurrent CLI subprocesses (from parallel
     PooledClient instances) share one .credentials.json. When the
     access_token nears expiry, whichever CLI trips first refreshes via
     Anthropic directly and writes back atomically; siblings pick up the
     new bundle on their next 401 → reread → retry cycle. Same pattern
     native CC uses for its own multi-process coordination.

  3. `__aexit__`:
       - flocks + removes our pid from holders
       - if holders now empty: read current .credentials.json (CLI may
         have rotated refresh_token mid-run), DELETE lease to pool with
         `updated_credentials=<bundle>` so the pool's refresh chain
         stays continuous, then rmtree the shared dir.

The `CLAUDE_CODE_OAUTH_TOKEN` env-var approach is gone. `CLAUDE_CONFIG_DIR`
is what the CLI writes/reads, and that's what we share.

## Cross-machine note

If two consumers on DIFFERENT machines try to hold leases for the same
account, the shared-dir mechanism can't coordinate — each host has its
own filesystem. Expect `invalid_grant` the moment one side refreshes.
Scale by adding accounts, not by cross-host sharing.
"""
from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import httpx
from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient  # type: ignore

from sub_pool_client._health_watcher import HealthWatcher
from sub_pool_client._shared import (
    cleanup_dir,
    dir_key,
    locked_meta,
    next_refresh_sleep,
    shared_dir,
    write_credentials,
)
from sub_pool_client.errors import (
    PoolAuthError,
    PoolConnectionError,
    PoolError,
    PoolUpstreamError,
)

log = logging.getLogger(__name__)


class PooledClient(ClaudeSDKClient):
    """Drop-in replacement for `ClaudeSDKClient` that leases credentials
    from a sub-pool instance before spawning the SDK subprocess."""

    def __init__(
        self,
        options: ClaudeAgentOptions | None = None,
        *,
        pool_url: str | None = None,
        api_key: str | None = None,
        user_id: str | None = None,
        request_id: str | None = None,
        required_model: str | None = None,
        required_features: list[str] | None = None,
        http_client: httpx.AsyncClient | None = None,
        health_interval_s: float = 30.0,
    ):
        self._pool_url = (pool_url or os.environ.get("SUB_POOL_URL", "")).rstrip("/")
        self._api_key = api_key or os.environ.get("SUB_POOL_KEY", "")
        if not self._pool_url:
            raise ValueError("pool_url required (set SUB_POOL_URL)")
        if not self._api_key:
            raise ValueError("api_key required (set SUB_POOL_KEY)")

        self._user_id = user_id
        self._request_id_req = request_id
        self._required_model = required_model
        self._required_features = list(required_features or [])
        self._health_interval_s = health_interval_s

        # Caller-supplied options are never mutated; we copy to attach env.
        self._base_options = copy.copy(options) if options else ClaudeAgentOptions()
        self._base_options.env = dict(self._base_options.env or {})

        # HTTP client for lease + release
        self._owned_http = http_client is None
        self._http = http_client or httpx.AsyncClient(timeout=10.0)

        # Populated on __aenter__
        self._dir: Path | None = None      # shared dir path
        self._is_first_holder = False      # we were the one who leased
        self._is_poll_leader = False       # we run the token poll task
        self._lease_id: str | None = None
        self._lease_account: str | None = None
        self._lease_account_id: int | None = None
        self._lease_request_id: str | None = None
        self._token_expires_at: float = 0.0
        self._sdk_inited = False
        self._poll_task: asyncio.Task | None = None
        self._health_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        # Set by a health swap to interrupt the poll loop's (possibly
        # multi-hour) sleep so it reschedules against the NEW account's token
        # expiry — otherwise a swap to a shorter-lived token could expire
        # unnoticed mid-sleep.
        self._poll_wake = asyncio.Event()

    # ============================================================ lifecycle
    async def __aenter__(self) -> "PooledClient":
        key = dir_key(
            pool_url=self._pool_url,
            api_key=self._api_key,
            provider="claude",
            user_id=self._user_id,
            required_model=self._required_model,
        )
        self._dir = shared_dir(key)

        await self._join_or_acquire_lease()

        # Tell the CLI where to find .credentials.json
        self._base_options.env["CLAUDE_CONFIG_DIR"] = str(self._dir)
        # Any stale override would shadow .credentials.json and defeat
        # the whole shared-refresh model, so drop it explicitly.
        self._base_options.env.pop("CLAUDE_CODE_OAUTH_TOKEN", None)

        ClaudeSDKClient.__init__(self, options=self._base_options)
        self._sdk_inited = True
        await ClaudeSDKClient.__aenter__(self)

        if self._is_poll_leader:
            self._start_leader_tasks()
        # Every holder runs the heartbeat — cheap, and takes over
        # leadership if the current leader crashed.
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        return self

    def _start_leader_tasks(self) -> None:
        """The poll leader owns two background tasks: token rotation
        (`_poll_loop`) and health-driven account swap (`_health_loop`).
        Idempotent — a heartbeat promotion or re-entry won't double-spawn."""
        if self._poll_task is None:
            self._poll_task = asyncio.create_task(self._poll_loop())
        if self._health_task is None:
            self._health_task = asyncio.create_task(self._health_loop())

    async def __aexit__(self, exc_type, exc, tb) -> None:
        for attr in ("_poll_task", "_health_task", "_heartbeat_task"):
            t = getattr(self, attr, None)
            if t is not None:
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
                setattr(self, attr, None)

        if self._sdk_inited:
            try:
                await ClaudeSDKClient.__aexit__(self, exc_type, exc, tb)
            except Exception:
                log.exception("SDK __aexit__ failed")

        await self._leave_lease(exc)

        if self._owned_http:
            await self._http.aclose()

    # ============================================================ lease state
    async def _join_or_acquire_lease(self) -> None:
        assert self._dir is not None
        my_pid = os.getpid()

        async with locked_meta(self._dir) as meta:
            if meta.lease_id and meta.holders:
                # Active lease already present — reuse it. Read identity +
                # expiry from meta (the shared source of truth): a prior
                # health swap may have advanced them past what any earlier
                # holder saw, and we may be promoted to leader later.
                self._lease_id = meta.lease_id
                self._lease_account = meta.account
                self._lease_account_id = meta.account_id
                self._token_expires_at = meta.token_expires_at
                # Always append (one entry per __aenter__, not per pid)
                # so N `async with` in the same pid are refcounted by
                # N removes on exit.
                meta.holders.append(my_pid)
                # If no live leader, take the seat. locked_meta already
                # cleared dead-pid leaders so poll_leader is either a
                # live pid or None here.
                if meta.poll_leader is None:
                    meta.poll_leader = my_pid
                    self._is_poll_leader = True
                return

            # No live holders → we're the first in. Acquire lease.
            lease = await self._post_lease()
            bundle = lease.get("credentials") or {}
            if not bundle.get("access_token"):
                raise PoolUpstreamError(
                    "MissingBundle",
                    "lease response did not include a credentials bundle",
                )
            write_credentials(self._dir, bundle)
            self._lease_id = lease["lease_id"]
            self._lease_account = lease.get("account")
            self._lease_account_id = lease.get("pool_account_id")
            self._lease_request_id = lease.get("request_id")
            self._token_expires_at = float(
                bundle.get("expires_at") or lease.get("token_expires_at") or 0.0
            )
            meta.lease_id = self._lease_id
            meta.account = self._lease_account
            meta.account_id = self._lease_account_id
            meta.token_expires_at = self._token_expires_at
            meta.holders = [my_pid]
            meta.poll_leader = my_pid
            self._is_first_holder = True
            self._is_poll_leader = True

    async def _leave_lease(self, exc: BaseException | None) -> None:
        if self._dir is None or self._lease_id is None:
            return
        my_pid = os.getpid()
        last_out = False
        lease_id_to_release: str | None = None

        async with locked_meta(self._dir) as meta:
            if meta.holders:
                # Drop our pid. If this PooledClient instance was a
                # repeat-in-same-process (same pid, multiple async with),
                # we only remove one occurrence.
                try:
                    meta.holders.remove(my_pid)
                except ValueError:
                    pass
            # If we were the poll leader, hand leadership off. The next
            # heartbeat in a surviving holder will elect itself and start
            # its own poll_loop. Guard on self._is_poll_leader: with two
            # instances in the SAME process (identical pid), a departing
            # NON-leader must not clear the live leader's seat — that would
            # trigger a spurious promotion and leave two leaders both running
            # _swap_account.
            if meta.poll_leader == my_pid and self._is_poll_leader:
                meta.poll_leader = None
            if not meta.holders:
                lease_id_to_release = meta.lease_id
                last_out = True

        if last_out and lease_id_to_release is not None:
            try:
                await self._delete_lease(lease_id_to_release, exc)
            except Exception:
                log.exception("lease release failed (best-effort)")
            cleanup_dir(self._dir)

    # ============================================================ heartbeat
    async def _heartbeat_loop(self, interval_s: float = 30.0) -> None:
        """Every tick a non-leader holder (a) re-syncs its cached lease state
        from meta and (b) promotes itself if the poll leader is gone.

        The re-sync is what keeps a non-leader honest after the leader
        health-swaps the shared lease: without it `self._lease_id` (hence
        `.account`, `report_error`, and a future promotion) would stay pinned to
        a released old lease.
        """
        try:
            while True:
                await asyncio.sleep(interval_s)
                if self._is_poll_leader or self._dir is None:
                    continue

                my_pid = os.getpid()
                promoted = False
                async with locked_meta(self._dir) as meta:
                    # Track the shared lease (a leader's health swap advances it
                    # past our cache). Guard on non-None so we never clobber a
                    # live cache with a torn/empty meta.
                    if meta.lease_id is not None:
                        self._lease_id = meta.lease_id
                        self._lease_account = meta.account
                        self._lease_account_id = meta.account_id
                        self._token_expires_at = meta.token_expires_at
                    # locked_meta already cleared dead-leader pids.
                    if meta.poll_leader is None and my_pid in (meta.holders or []):
                        meta.poll_leader = my_pid
                        promoted = True
                if promoted:
                    self._is_poll_leader = True
                    self._start_leader_tasks()
                    log.info("heartbeat: promoted self to poll leader")
                    return   # heartbeat's job done
        except asyncio.CancelledError:
            return

    # ============================================================ poll
    async def _poll_loop(self) -> None:
        """Periodically ask the pool for a fresh access_token and rewrite
        the shared .credentials.json in place. The CLI atomically picks
        up the new access_token on its next read.

        Poll schedule: wake up 10 min before current expiry (or 60s if
        already in the window), hit the pool, rewrite file, repeat. We
        do NOT rotate the refresh_token field on disk — the authoritative
        refresh_token lives pool-side; the one in the file is a
        best-effort fallback for when the pool can't be reached.
        """
        try:
            while True:
                self._poll_wake.clear()
                # Wake ~10 min before expiry (min 60s), or a fixed cadence when
                # the lease has no usable expiry — shared with the CLI + codex
                # so the three schedulers can't drift (see next_refresh_sleep).
                sleep_s = next_refresh_sleep(self._token_expires_at, time.time())
                # Interruptible sleep: a health swap sets _poll_wake so we
                # recompute the schedule against the NEW (possibly much shorter)
                # token expiry instead of oversleeping on the old account's.
                woke_early = True
                try:
                    await asyncio.wait_for(self._poll_wake.wait(), timeout=sleep_s)
                except asyncio.TimeoutError:
                    woke_early = False
                if woke_early:
                    # Swap advanced self._token_expires_at; reschedule, don't
                    # burn a poll on the freshly-written token.
                    continue

                if self._dir is None or self._lease_id is None:
                    return

                lease_id_before = self._lease_id
                try:
                    new = await self._post_poll_token()
                except Exception as e:  # noqa: BLE001
                    # If a swap rotated us mid-poll, this failure is just the
                    # OLD lease's 410 — don't clobber the new expiry the swap
                    # already set (that would force a pointless refresh 60s on).
                    if self._lease_id != lease_id_before:
                        continue
                    log.warning("lease token poll failed: %s", e)
                    # Try again in 30s; a transient 5xx / network blip
                    # shouldn't tear down the agent.
                    self._token_expires_at = time.time() + 60.0
                    continue

                # A health-driven swap may have rotated us onto a different
                # lease/account while this poll was in flight. The token we
                # just fetched belongs to the OLD lease; writing it onto the
                # NEW account's shared credentials file would clobber the swap.
                if self._lease_id != lease_id_before:
                    continue

                new_access = new.get("access_token")
                new_exp = float(new.get("token_expires_at") or 0.0)
                if not new_access or not new_exp:
                    log.warning("lease token poll returned empty bundle: %s", new)
                    continue

                # Rewrite shared .credentials.json with the new access_token
                # (keep existing refresh_token + scopes + subscriptionType).
                try:
                    _patch_access_token(self._dir, new_access, new_exp)
                except OSError as e:
                    log.warning("failed to update .credentials.json: %s", e)
                    continue

                self._token_expires_at = new_exp
                log.debug(
                    "rotated access_token for lease %s (exp=%s)",
                    self._lease_id, new_exp,
                )
        except asyncio.CancelledError:
            return

    async def _post_poll_token(self) -> dict:
        assert self._lease_id is not None
        r = await self._http.post(
            f"{self._pool_url}/credentials/lease/{self._lease_id}/token",
            headers={"authorization": f"Bearer {self._api_key}"},
        )
        if r.status_code >= 400:
            detail = _detail_or_text(r)
            raise PoolUpstreamError(
                str(detail.get("code") or f"HTTP{r.status_code}"),
                str(detail.get("message") or r.text[:500]),
            )
        return r.json()

    # ============================================================ health swap
    async def _health_loop(self) -> None:
        """Leader-only: watch the lease's account health and swap to a fresh
        account when the pool marks the current one unhealthy — quota COOLING,
        or INVALID after a revoked / auth-expired credential (see the server's
        usage-probe recovery). Reuses the same `HealthWatcher` the `sp-claude`
        CLI uses; the running `claude` subprocess re-reads `.credentials.json`
        on its next request, so the swap is transparent to the caller.

        This is what closes the "healthy-looking lease but its token is dead
        upstream" gap: without it a PooledClient would 401 for the rest of the
        session with no way to recover.
        """
        watcher = HealthWatcher(
            fetch=self._fetch_health,
            on_unhealthy=self._on_unhealthy,
            interval_s=self._health_interval_s,
        )
        try:
            await watcher.run()
        except asyncio.CancelledError:
            return

    async def _fetch_health(self) -> dict:
        lease_id = self._lease_id
        if self._dir is None or lease_id is None:
            return {"healthy": True}
        r = await self._http.get(
            f"{self._pool_url}/credentials/lease/{lease_id}/health",
            headers={"authorization": f"Bearer {self._api_key}"},
        )
        # 404 (unknown lease) / 410 (lease ended) → HealthWatcher recovers by
        # re-leasing (lease_gone). Other non-2xx are logged + retried.
        r.raise_for_status()
        return r.json()

    async def _on_unhealthy(self, health: dict) -> None:
        await self._swap_account(
            reason=str(health.get("reason") or "unhealthy"),
            lease_gone=bool(health.get("lease_gone")),
        )

    async def _swap_account(self, *, reason: str, lease_gone: bool = False) -> None:
        """Lease a replacement account and rewrite the shared credential in
        place so the running CLI picks it up on its next request. Only the poll
        leader runs this. Best-effort: if the pool has no other healthy account
        (503) we keep the current lease and let the next health tick retry.
        """
        if self._dir is None or self._lease_id is None:
            return
        old_lease_id = self._lease_id
        old_account = self._lease_account
        old_account_id = self._lease_account_id
        try:
            # Do NOT reuse the caller's request_id here: the pool dedupes leases
            # by request_id and would idempotently hand back the CURRENT
            # (still-active, unhealthy) lease instead of a fresh account — the
            # swap would silently no-op forever ("no other account" every tick).
            lease = await self._post_lease(include_request_id=False)
        except PoolError as e:
            # 503 (all other accounts cooling/invalid) or a transient pool
            # blip — don't tear down a working lease over it; retry next tick.
            log.info("health swap deferred: new lease failed: %s", e)
            return

        bundle = lease.get("credentials") or {}
        new_lease_id = lease.get("lease_id")
        new_account = lease.get("account")
        new_account_id = lease.get("pool_account_id")
        if not bundle.get("access_token") or not new_lease_id:
            log.warning("health swap: lease response missing bundle; keeping current")
            await self._release_swap_dup(new_lease_id, old_lease_id)
            return
        # Same-account guard keys on the immutable server account_id, NOT the
        # name: a delete+recreate of the same name is a DIFFERENT account and
        # must be allowed to swap in. For a cooled/unhealthy swap, the same
        # account means "nothing to swap to" — keep the current, still-usable
        # lease. But when the current lease is GONE (lease_gone, 410/404), the
        # same account on a NEW lease_id IS the recovery: keeping the dead lease
        # would strand the session 401ing. So only a byte-identical lease_id is
        # truly nothing gained once the lease is gone.
        same_lease = new_lease_id == old_lease_id
        same_account = (
            same_lease
            or (new_account_id is not None and old_account_id is not None
                and new_account_id == old_account_id)
        )
        if same_lease or (same_account and not lease_gone):
            await self._release_swap_dup(new_lease_id, old_lease_id)
            log.info("health swap: nothing to swap to (still on %s)",
                     old_account)
            return

        new_exp = float(
            bundle.get("expires_at") or lease.get("token_expires_at") or 0.0
        )
        # Commit under the meta lock so the poll loop's post-await lease_id
        # re-check sees the rotation and drops any stale token patch. No await
        # between the disk write and the meta / self advance.
        #
        # `committed` marks the point where the NEW lease becomes ours: the
        # instant on-disk creds AND in-memory state point at it — set BEFORE the
        # `async with` exit persists meta.json. If that persist fails (ENOSPC),
        # committed is already True so the finally does NOT delete the new lease:
        # self._lease_id / .credentials.json already use it, and deleting it
        # would strand us on a dead lease (health 410 → no self-heal — the exact
        # state this feature removes). meta.json just stays stale (old id) and
        # self-corrects on the next locked_meta write; the now-unreferenced OLD
        # lease is reaped by TTL.
        committed = False
        try:
            async with locked_meta(self._dir) as meta:
                write_credentials(self._dir, bundle)
                meta.lease_id = new_lease_id
                meta.account = new_account
                meta.account_id = new_account_id
                meta.token_expires_at = new_exp
                self._lease_id = new_lease_id
                self._lease_account = new_account
                self._lease_account_id = new_account_id
                self._lease_request_id = lease.get("request_id")
                self._token_expires_at = new_exp
                # Wake the poll loop to reschedule against the new expiry, and
                # take ownership — both inside the lock so a failed meta persist
                # still leaves us pointing at a live lease.
                self._poll_wake.set()
                committed = True
        finally:
            if not committed:
                # Never took ownership → release the orphaned new lease so it
                # isn't leaked to the reaper. Shield so an in-flight cancel
                # can't skip the cleanup.
                with contextlib.suppress(Exception):
                    await asyncio.shield(self._delete_lease(new_lease_id, None))

        # Release the cooled / dead lease. Shielded + best-effort so a
        # concurrent __aexit__ cancel can't leak it. (Skipped if the meta
        # persist above threw — the old lease then falls to the reaper.)
        with contextlib.suppress(Exception):
            await asyncio.shield(self._delete_lease(old_lease_id, None))
        log.info("health-swapped account %s → %s (lease %s → %s; reason: %s)",
                 old_account, new_account, old_lease_id, new_lease_id, reason[:80])

    async def _release_swap_dup(self, new_lease_id, old_lease_id) -> None:
        """Release a just-acquired lease we decided not to keep (missing bundle
        or same-account), so a deferred swap never leaks it. Shielded so an
        __aexit__ cancel landing on the DELETE can't skip the cleanup."""
        if new_lease_id and new_lease_id != old_lease_id:
            with contextlib.suppress(Exception):
                await asyncio.shield(self._delete_lease(new_lease_id, None))

    # ============================================================ HTTP
    async def _post_lease(self, *, include_request_id: bool = True) -> dict:
        body: dict[str, Any] = {"provider": "claude"}
        if self._user_id:
            body["user_id"] = self._user_id
        if include_request_id and self._request_id_req:
            body["request_id"] = self._request_id_req
        if self._required_model:
            body["required_model"] = self._required_model
        if self._required_features:
            body["required_features"] = list(self._required_features)
        try:
            r = await self._http.post(
                f"{self._pool_url}/credentials/lease",
                headers={"authorization": f"Bearer {self._api_key}"},
                json=body,
            )
        except (httpx.ConnectError, httpx.ReadTimeout) as e:
            raise PoolConnectionError("ConnectError", str(e)) from e
        except httpx.HTTPError as e:
            raise PoolConnectionError("HTTPError", str(e)) from e

        if r.status_code in (401, 403):
            raise PoolAuthError(
                f"HTTP{r.status_code}", r.text[:500] or "authorization failed",
            )
        if r.status_code >= 400:
            detail = _detail_or_text(r)
            raise PoolUpstreamError(
                str(detail.get("code") or f"HTTP{r.status_code}"),
                str(detail.get("message") or r.text[:500]),
            )
        return r.json()

    async def _delete_lease(
        self, lease_id: str, exc: BaseException | None,
    ) -> None:
        body: dict[str, Any] = {}
        if exc is not None:
            body["error_code"] = type(exc).__name__
            body["error_message"] = str(exc)[:2000]
        try:
            await self._http.request(
                "DELETE",
                f"{self._pool_url}/credentials/lease/{lease_id}",
                headers={"authorization": f"Bearer {self._api_key}"},
                json=body,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("DELETE lease %s failed (best-effort): %s", lease_id, e)

    # ============================================================ error reporting
    async def report_error(self, code: str, message: str) -> dict:
        """Mid-lease: tell the pool about a backend error (429 rate
        limit, quota exhausted, etc.) so it can mark the account COOLING
        and route parallel leases elsewhere. The lease itself stays
        alive — caller decides whether to keep using it or tear down
        and re-lease on a different account.
        """
        lease_id = await self._current_lease_id()
        if lease_id is None:
            return {}
        try:
            r = await self._http.post(
                f"{self._pool_url}/credentials/lease/{lease_id}/report-error",
                headers={"authorization": f"Bearer {self._api_key}"},
                json={"code": code, "message": message},
            )
        except Exception as e:  # noqa: BLE001
            log.warning("report_error HTTP failed: %s", e)
            return {}
        if r.status_code >= 400:
            log.warning("report_error %s: %s", r.status_code, r.text[:200])
            return {}
        return r.json()

    async def _current_lease_id(self) -> str | None:
        """The lease id to act on right now. Prefers the shared meta over our
        cached `self._lease_id`: a leader's health swap may have rotated the
        lease past a non-leader holder's cache, and acting on the released old
        lease would 410 (report_error) or misroute — so callers that reach the
        pool by lease id resolve through here."""
        if self._dir is not None:
            with contextlib.suppress(Exception):
                async with locked_meta(self._dir) as meta:
                    if meta.lease_id is not None:
                        return meta.lease_id
        return self._lease_id

    # ============================================================ introspection
    @property
    def account(self) -> str | None:
        return self._lease_account

    @property
    def lease_id(self) -> str | None:
        return self._lease_id

    @property
    def request_id(self) -> str | None:
        return self._lease_request_id


def _patch_access_token(dir_: Path, new_access: str, new_expires_s: float) -> None:
    """Rewrite dir/.credentials.json in CLI schema, overwriting only
    accessToken + expiresAt. Other fields (refreshToken, scopes, ...)
    preserved. Atomic rename, 0600 perms."""
    p = dir_ / ".credentials.json"
    try:
        payload = json.loads(p.read_text()) if p.exists() else {}
    except (OSError, ValueError):
        payload = {}
    nested = payload.setdefault("claudeAiOauth", {})
    nested["accessToken"] = new_access
    nested["expiresAt"] = int(new_expires_s * 1000)
    tmp = p.with_suffix(p.suffix + ".tmp")
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, indent=2)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    os.replace(tmp, p)


def _detail_or_text(r: httpx.Response) -> dict:
    try:
        body = r.json()
    except Exception:
        return {}
    if isinstance(body, dict):
        detail = body.get("detail") or body.get("error")
        if isinstance(detail, dict):
            return detail
    return {}
