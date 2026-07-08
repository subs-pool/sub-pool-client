"""PooledCodexClient — lease a Codex account from the pool and run
Codex CLI locally with a pool-managed auth.json.

Symmetric to `PooledClient` for claude:
  - The pool owns the OAuth refresh_token chain. The leased auth.json
    contains a fresh access_token but its refresh_token field is
    blanked out, so the local Codex CLI cannot independently rotate
    the chain (and racing rotations across concurrent leases can't
    happen).
  - A background poll task asks the pool for a refreshed access_token
    before the current one expires, then atomically rewrites the
    shared $CODEX_HOME/auth.json so the CLI picks up the new token on
    its next read.
  - On a backend error (consumer-detected 429 / auth_expired from
    upstream OpenAI), the caller can call `report_error(code, msg)`
    so the pool cools the account and routes other leases elsewhere.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from pathlib import Path
from typing import Any

import httpx

from sub_pool_client._health_watcher import HealthWatcher
from sub_pool_client._shared import (
    cleanup_dir,
    dir_key,
    locked_meta,
    shared_dir,
    write_codex_auth,
)
from sub_pool_client.errors import (
    PoolAuthError,
    PoolConnectionError,
    PoolError,
    PoolUpstreamError,
)

log = logging.getLogger(__name__)


class PooledCodexClient:
    """Context manager that exposes a leased Codex account through CODEX_HOME.

    The pool leases a sanitized Codex CLI auth.json (no refresh_token).
    Codex itself runs on the caller's machine; this client prepares an
    isolated CODEX_HOME, reference-counts shared local use, polls the
    pool for refreshed access_tokens, and releases the lease on exit.
    """

    def __init__(
        self,
        *,
        pool_url: str | None = None,
        api_key: str | None = None,
        user_id: str | None = None,
        request_id: str | None = None,
        required_model: str | None = None,
        required_features: list[str] | None = None,
        codex_bin: str = "codex",
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
        self._codex_bin = codex_bin
        self._health_interval_s = health_interval_s
        self._owned_http = http_client is None
        self._http = http_client or httpx.AsyncClient(timeout=10.0)

        self._dir: Path | None = None
        self._lease_id: str | None = None
        self._lease_account: str | None = None
        self._lease_account_id: int | None = None
        self._lease_request_id: str | None = None
        self._token_expires_at: float = 0.0

        self._poll_task: asyncio.Task | None = None
        self._health_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._is_poll_leader: bool = False
        # Set by a health swap to interrupt the poll loop's (possibly very long,
        # Codex tokens run ~10 days) sleep so it reschedules against the NEW
        # account's token expiry.
        self._poll_wake = asyncio.Event()

    async def __aenter__(self) -> "PooledCodexClient":
        key = dir_key(
            pool_url=self._pool_url,
            api_key=self._api_key,
            provider="codex",
            user_id=self._user_id,
            required_model=self._required_model,
        )
        self._dir = shared_dir(key)
        await self._join_or_acquire_lease()
        if self._is_poll_leader:
            self._start_leader_tasks()
        # Every holder runs the heartbeat — cheap, and takes over
        # leadership if the current leader exits.
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
        await self._leave_lease(exc)
        if self._owned_http:
            await self._http.aclose()

    @property
    def codex_home(self) -> Path:
        if self._dir is None:
            raise RuntimeError("PooledCodexClient is not entered")
        return self._dir

    @property
    def env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["CODEX_HOME"] = str(self.codex_home)
        # Avoid an ambient API key taking precedence over the leased auth.json.
        env.pop("OPENAI_API_KEY", None)
        return env

    @property
    def account(self) -> str | None:
        return self._lease_account

    @property
    def lease_id(self) -> str | None:
        return self._lease_id

    @property
    def request_id(self) -> str | None:
        return self._lease_request_id


    async def report_error(self, code: str, message: str) -> dict:
        """Mid-lease: tell the pool about a backend error (429, quota,
        auth rejection) so it cools the account and routes parallel
        leases elsewhere. The lease itself stays alive — caller decides
        whether to keep using it or tear down and re-lease.
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
            log.warning("codex report_error HTTP failed: %s", e)
            return {}
        if r.status_code >= 400:
            log.warning("codex report_error %s: %s", r.status_code, r.text[:200])
            return {}
        return r.json()

    async def _current_lease_id(self) -> str | None:
        """The lease id to act on now — prefers shared meta over our cached
        self._lease_id so a non-leader holder acts on the leader's swapped lease
        rather than a released old one."""
        if self._dir is not None:
            with contextlib.suppress(Exception):
                async with locked_meta(self._dir) as meta:
                    if meta.lease_id is not None:
                        return meta.lease_id
        return self._lease_id

    async def exec(
        self,
        prompt: str,
        *extra_args: str,
        cwd: str | Path | None = None,
        model: str | None = None,
        json_output: bool = False,
    ) -> asyncio.subprocess.Process:
        """Spawn `codex exec` with this lease's CODEX_HOME.

        The returned process is already started; callers decide how to read
        stdout/stderr and wait.
        """
        cmd = [self._codex_bin, "exec"]
        if model:
            cmd += ["--model", model]
        if cwd is not None:
            cmd += ["--cd", str(cwd)]
        if json_output:
            cmd.append("--json")
        cmd.extend(extra_args)
        cmd.append(prompt)
        return await asyncio.create_subprocess_exec(
            *cmd,
            env=self.env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    async def _join_or_acquire_lease(self) -> None:
        assert self._dir is not None
        my_pid = os.getpid()
        async with locked_meta(self._dir) as meta:
            if meta.lease_id and meta.holders:
                # Active lease already present — reuse it. Read identity +
                # expiry from meta (shared source of truth): a prior health
                # swap may have advanced them, and we may be promoted later.
                self._lease_id = meta.lease_id
                self._lease_account = meta.account
                self._lease_account_id = meta.account_id
                self._token_expires_at = meta.token_expires_at
                meta.holders.append(my_pid)
                # locked_meta cleared dead-pid leaders, so poll_leader is
                # either a live pid or None. Claim leadership right away
                # if vacant; otherwise the heartbeat task will promote
                # us if the current leader exits.
                if meta.poll_leader is None:
                    meta.poll_leader = my_pid
                    self._is_poll_leader = True
                return

            lease = await self._post_lease()
            creds = lease.get("credentials") or {}
            auth_json = creds.get("auth_json")
            if not isinstance(auth_json, dict):
                raise PoolUpstreamError(
                    "MissingBundle",
                    "lease response did not include a Codex auth_json bundle",
                )
            write_codex_auth(self._dir, auth_json)
            self._lease_id = lease["lease_id"]
            self._lease_account = lease.get("account")
            self._lease_account_id = lease.get("pool_account_id")
            self._lease_request_id = lease.get("request_id")
            self._token_expires_at = float(lease.get("token_expires_at") or 0.0)
            meta.lease_id = self._lease_id
            meta.account = self._lease_account
            meta.account_id = self._lease_account_id
            meta.token_expires_at = self._token_expires_at
            meta.holders = [my_pid]
            meta.poll_leader = my_pid
            self._is_poll_leader = True

    async def _leave_lease(self, exc: BaseException | None) -> None:
        if self._dir is None or self._lease_id is None:
            return
        my_pid = os.getpid()
        last_out = False
        lease_id_to_release: str | None = None
        async with locked_meta(self._dir) as meta:
            if meta.holders:
                try:
                    meta.holders.remove(my_pid)
                except ValueError:
                    pass
            # Guard on self._is_poll_leader so a departing NON-leader in the
            # same process (identical pid) can't clear the live leader's seat
            # and trigger a spurious double-leader.
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

    async def _heartbeat_loop(self, interval_s: float = 30.0) -> None:
        """Every tick a non-leader holder (a) re-syncs its cached lease state
        from meta — so its `.account` / `report_error` / a future promotion
        track a leader's health swap instead of a released old lease — and
        (b) promotes itself if the poll leader is gone."""
        try:
            while True:
                await asyncio.sleep(interval_s)
                if self._is_poll_leader or self._dir is None:
                    continue
                my_pid = os.getpid()
                promoted = False
                async with locked_meta(self._dir) as meta:
                    if meta.lease_id is not None:
                        self._lease_id = meta.lease_id
                        self._lease_account = meta.account
                        self._lease_account_id = meta.account_id
                        self._token_expires_at = meta.token_expires_at
                    if meta.poll_leader is None and my_pid in (meta.holders or []):
                        meta.poll_leader = my_pid
                        promoted = True
                if promoted:
                    self._is_poll_leader = True
                    self._start_leader_tasks()
                    log.info("codex heartbeat: promoted self to poll leader")
                    return
        except asyncio.CancelledError:
            return

    async def _poll_loop(self) -> None:
        """Periodically ask the pool for a refreshed sanitized auth.json
        and rewrite the shared one. Codex CLI re-reads auth.json on its
        next call, so the rotation is transparent.
        """
        try:
            while True:
                self._poll_wake.clear()
                remaining = self._token_expires_at - time.time()
                # Wake up 10 min before expiry, minimum 60s sleep.
                sleep_s = max(60.0, remaining - 600.0)
                # Interruptible: a health swap sets _poll_wake so we reschedule
                # against the NEW account's token expiry instead of oversleeping
                # on the old one (Codex tokens can be days out).
                woke_early = True
                try:
                    await asyncio.wait_for(self._poll_wake.wait(), timeout=sleep_s)
                except asyncio.TimeoutError:
                    woke_early = False
                if woke_early:
                    continue

                if self._dir is None or self._lease_id is None:
                    return

                lease_id_before = self._lease_id
                try:
                    new = await self._post_poll_token()
                except Exception as e:  # noqa: BLE001
                    # If a swap rotated us mid-poll, this is just the OLD lease's
                    # 410 — don't clobber the new expiry the swap already set.
                    if self._lease_id != lease_id_before:
                        continue
                    log.warning("codex lease token poll failed: %s", e)
                    self._token_expires_at = time.time() + 60.0
                    continue

                # A health-driven swap may have rotated us onto a different
                # lease/account while this poll was in flight — the fetched
                # auth.json belongs to the OLD lease; writing it onto the NEW
                # account's shared file would clobber the swap.
                if self._lease_id != lease_id_before:
                    continue

                creds = new.get("credentials") or {}
                new_auth = creds.get("auth_json")
                new_exp = float(new.get("token_expires_at") or 0.0)
                if not isinstance(new_auth, dict) or not new_exp:
                    log.warning(
                        "codex poll returned empty bundle: %s", new,
                    )
                    continue

                try:
                    write_codex_auth(self._dir, new_auth)
                except OSError as e:
                    log.warning("failed to update codex auth.json: %s", e)
                    continue

                self._token_expires_at = new_exp
                log.debug(
                    "rotated codex access_token for lease %s (exp=%s)",
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
        account when the pool marks the current one unhealthy — quota COOLING
        or INVALID after a revoked / auth-expired credential. Reuses the same
        `HealthWatcher` the `sp-codex` CLI uses; the running `codex` subprocess
        re-reads `auth.json` on its next call, so the swap is transparent.
        Without it a PooledCodexClient handed a dead account would 401 for the
        rest of the session with no way to recover.
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
        """Lease a replacement Codex account and rewrite the shared auth.json
        in place so the running CLI picks it up on its next call. Only the poll
        leader runs this. Best-effort: no other healthy account (503) → keep the
        current lease and let the next health tick retry.
        """
        if self._dir is None or self._lease_id is None:
            return
        old_lease_id = self._lease_id
        old_account = self._lease_account
        old_account_id = self._lease_account_id
        try:
            # Do NOT reuse the caller's request_id: the pool dedupes by it and
            # would hand back the current (unhealthy) lease, making the swap a
            # silent no-op forever.
            lease = await self._post_lease(include_request_id=False)
        except PoolError as e:
            log.info("codex health swap deferred: new lease failed: %s", e)
            return

        creds = lease.get("credentials") or {}
        auth_json = creds.get("auth_json")
        new_lease_id = lease.get("lease_id")
        new_account = lease.get("account")
        new_account_id = lease.get("pool_account_id")
        if not isinstance(auth_json, dict) or not new_lease_id:
            log.warning("codex health swap: lease response missing auth_json; keeping current")
            await self._release_swap_dup(new_lease_id, old_lease_id)
            return
        # Same-account guard keys on the immutable server account_id, NOT the
        # name (a delete+recreate of the same name is a DIFFERENT account and
        # must swap in). For a cooled swap the same account means "nothing to
        # swap to" — keep the current lease. But when the lease is GONE
        # (lease_gone, 410/404), the same account on a NEW lease_id IS the
        # recovery; only a byte-identical lease_id is truly nothing gained.
        same_lease = new_lease_id == old_lease_id
        same_account = (
            same_lease
            or (new_account_id is not None and old_account_id is not None
                and new_account_id == old_account_id)
        )
        if same_lease or (same_account and not lease_gone):
            await self._release_swap_dup(new_lease_id, old_lease_id)
            log.info("codex health swap: no other account available (still on %s)",
                     old_account)
            return

        new_exp = float(lease.get("token_expires_at") or 0.0)
        # Commit under the meta lock so the poll loop's post-await lease_id
        # re-check sees the rotation and drops any stale token write. No await
        # between the disk write and the meta / self advance.
        #
        # `committed` marks ownership of the new lease — set BEFORE the block
        # exit persists meta.json. If that persist fails (ENOSPC), committed is
        # already True so the finally does NOT delete the new lease: self._
        # lease_id / auth.json already use it and deleting it would strand us on
        # a dead lease (health 410 → no self-heal). meta.json stays stale and
        # self-corrects next write; the old lease is reaped by TTL.
        committed = False
        try:
            async with locked_meta(self._dir) as meta:
                write_codex_auth(self._dir, auth_json)
                meta.lease_id = new_lease_id
                meta.account = new_account
                meta.account_id = new_account_id
                meta.token_expires_at = new_exp
                self._lease_id = new_lease_id
                self._lease_account = new_account
                self._lease_account_id = new_account_id
                self._lease_request_id = lease.get("request_id")
                self._token_expires_at = new_exp
                self._poll_wake.set()
                committed = True
        finally:
            if not committed:
                with contextlib.suppress(Exception):
                    await asyncio.shield(self._delete_lease(new_lease_id, None))

        # Release the cooled/dead old lease (skipped if the meta persist above
        # threw — it then falls to the reaper).
        with contextlib.suppress(Exception):
            await asyncio.shield(self._delete_lease(old_lease_id, None))
        log.info("codex health-swapped account %s → %s (lease %s → %s; reason: %s)",
                 old_account, new_account, old_lease_id, new_lease_id, reason[:80])

    async def _release_swap_dup(self, new_lease_id, old_lease_id) -> None:
        """Release a just-acquired lease we decided not to keep (missing
        auth_json or same-account) so a deferred swap never leaks it. Shielded
        so an __aexit__ cancel on the DELETE can't skip the cleanup."""
        if new_lease_id and new_lease_id != old_lease_id:
            with contextlib.suppress(Exception):
                await asyncio.shield(self._delete_lease(new_lease_id, None))

    async def _post_lease(self, *, include_request_id: bool = True) -> dict:
        body: dict[str, Any] = {"provider": "codex"}
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
        # Best-effort, like claude's _delete_lease: releasing a lease must never
        # raise into a swap / release path (the reaper is the backstop).
        try:
            await self._http.request(
                "DELETE",
                f"{self._pool_url}/credentials/lease/{lease_id}",
                headers={"authorization": f"Bearer {self._api_key}"},
                json=body,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("codex DELETE lease %s failed (best-effort): %s", lease_id, e)


def _detail_or_text(r: httpx.Response) -> dict[str, Any]:
    try:
        d = r.json()
    except ValueError:
        return {"message": r.text}
    if isinstance(d, dict) and isinstance(d.get("detail"), dict):
        return d["detail"]
    if isinstance(d, dict):
        return d
    return {"message": str(d)}
