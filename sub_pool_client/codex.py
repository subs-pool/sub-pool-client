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
import logging
import os
import time
from pathlib import Path
from typing import Any

import httpx

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
        self._owned_http = http_client is None
        self._http = http_client or httpx.AsyncClient(timeout=10.0)

        self._dir: Path | None = None
        self._lease_id: str | None = None
        self._lease_account: str | None = None
        self._lease_request_id: str | None = None
        self._token_expires_at: float = 0.0

        self._poll_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._is_poll_leader: bool = False

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
            self._poll_task = asyncio.create_task(self._poll_loop())
        # Every holder runs the heartbeat — cheap, and takes over
        # leadership if the current leader exits.
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        for attr in ("_poll_task", "_heartbeat_task"):
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
        if self._lease_id is None:
            return {}
        try:
            r = await self._http.post(
                f"{self._pool_url}/credentials/lease/{self._lease_id}/report-error",
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
                # Active lease already present — reuse it.
                self._lease_id = meta.lease_id
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
            self._lease_request_id = lease.get("request_id")
            self._token_expires_at = float(lease.get("token_expires_at") or 0.0)
            meta.lease_id = self._lease_id
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
            if meta.poll_leader == my_pid:
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
        """If the original poll leader exits, the next-in-line holder
        promotes itself and takes over the rotation."""
        try:
            while True:
                await asyncio.sleep(interval_s)
                if self._is_poll_leader or self._dir is None:
                    continue
                my_pid = os.getpid()
                promoted = False
                async with locked_meta(self._dir) as meta:
                    if meta.poll_leader is None and my_pid in (meta.holders or []):
                        meta.poll_leader = my_pid
                        promoted = True
                if promoted:
                    self._is_poll_leader = True
                    self._poll_task = asyncio.create_task(self._poll_loop())
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
                remaining = self._token_expires_at - time.time()
                # Wake up 10 min before expiry, minimum 60s sleep.
                sleep_s = max(60.0, remaining - 600.0)
                await asyncio.sleep(sleep_s)

                if self._dir is None or self._lease_id is None:
                    return

                try:
                    new = await self._post_poll_token()
                except Exception as e:  # noqa: BLE001
                    log.warning("codex lease token poll failed: %s", e)
                    self._token_expires_at = time.time() + 60.0
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

    async def _post_lease(self) -> dict:
        body: dict[str, Any] = {"provider": "codex"}
        if self._user_id:
            body["user_id"] = self._user_id
        if self._request_id_req:
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
        await self._http.request(
            "DELETE",
            f"{self._pool_url}/credentials/lease/{lease_id}",
            headers={"authorization": f"Bearer {self._api_key}"},
            json=body,
        )


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
