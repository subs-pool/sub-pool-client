"""Provider-agnostic engine behind `sp-claude` / `sp-codex`.

What lives here is the lease lifecycle: lease → write per-session
credentials → spawn the upstream CLI → background refresh + health
swap → release. The thin click entry-points in `cli_claude.py` and
`cli_codex.py` instantiate a `CliProvider` and hand it to
`run_session`; nothing in this module switches on provider name.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import signal
import sys
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

import click
import httpx

from sub_pool_client._cli_provider import CliProvider
from sub_pool_client._health_watcher import HealthWatcher
from sub_pool_client._shared import next_refresh_sleep
from sub_pool_client._swap import NoSwapAvailable, swap_credentials


log = logging.getLogger("sp")

# `sp` config + persistent home dirs, sibling to the user's `~/.claude/`
# (or `~/.codex/`). One root per host shared across providers; per-provider
# state lives in subdirs named by `CliProvider.home_subdir`.
SP_ROOT = Path.home() / ".sub-pool"
CONFIG_PATH = SP_ROOT / "cli.toml"


# ============================================================ config


class Config:
    """Loaded `~/.sub-pool/cli.toml`.

    Shape:

        [pool]
        url = "https://pool.example.com"
        api_key = "cp-..."

        [claude]
        bin = "claude"            # optional, defaults to PATH lookup
        default_account = ""      # optional, "" = strategy-picked

        [codex]
        bin = "codex"
        default_account = ""

    Per-provider sections are optional; absent values fall back to
    the provider's `default_bin` and an empty default account.
    """

    def __init__(self, pool_url: str, api_key: str,
                 per_provider: dict[str, dict[str, str]] | None = None):
        self.pool_url = pool_url.rstrip("/")
        self.api_key = api_key
        self._per_provider = per_provider or {}

    def bin_for(self, provider: CliProvider) -> str:
        return (self._per_provider.get(provider.name, {}).get("bin")
                or provider.default_bin)

    def default_account_for(self, provider: CliProvider) -> str | None:
        return (self._per_provider.get(provider.name, {}).get("default_account")
                or None)

    def merged_provider_section(
        self, provider: CliProvider, *, bin_override: str | None = None,
        account_override: str | None = None,
    ) -> dict[str, str]:
        """Return what would be written under `[<provider.name>]`, given
        the existing config and any in-flight overrides from `--setup`."""
        existing = dict(self._per_provider.get(provider.name, {}))
        if bin_override is not None:
            existing["bin"] = bin_override
        if account_override is not None:
            existing["default_account"] = account_override
        return existing

    @classmethod
    def load(cls) -> "Config | None":
        """Resolve pool_url + api_key from env vars first, falling back
        to `~/.sub-pool/cli.toml`. Per-provider sections (bin paths /
        default accounts) come from the TOML only — env vars cover the
        pool credentials, which is what CI / Docker / ephemeral runs
        actually need.

        Returns None only when neither source supplies both pool_url
        AND api_key — that's the "go run --setup" signal in main().
        """
        env_url = (os.environ.get("SUB_POOL_URL") or "").strip()
        env_key = (os.environ.get("SUB_POOL_KEY") or "").strip()

        data: dict = {}
        if CONFIG_PATH.exists():
            import tomllib
            data = tomllib.loads(CONFIG_PATH.read_text())

        pool = data.get("pool") or {}
        pool_url = env_url or (pool.get("url") or "")
        api_key = env_key or (pool.get("api_key") or "")
        if not pool_url or not api_key:
            return None

        per_provider: dict[str, dict[str, str]] = {}
        for name in ("claude", "codex"):
            section = data.get(name)
            if isinstance(section, dict):
                per_provider[name] = {
                    k: str(v) for k, v in section.items()
                    if isinstance(v, (str, int, float))
                }
        return cls(pool_url=pool_url, api_key=api_key,
                   per_provider=per_provider)

    def write(self) -> None:
        SP_ROOT.mkdir(parents=True, exist_ok=True)
        body = (
            "[pool]\n"
            f"url = {_toml_str(self.pool_url)}\n"
            f"api_key = {_toml_str(self.api_key)}\n"
        )
        for name in ("claude", "codex"):
            section = self._per_provider.get(name)
            if not section:
                continue
            body += f"\n[{name}]\n"
            for key in ("bin", "default_account"):
                if section.get(key) is not None:
                    body += f"{key} = {_toml_str(str(section[key]))}\n"
        # Create at 0o600 atomically — never a readable window for the
        # api_key — and fchmod after for the existing-file case.
        fd = os.open(str(CONFIG_PATH),
                     os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(body)


def _toml_str(s: str) -> str:
    """Render `s` as a TOML literal string (no escape interpretation).
    Falls back to a basic string when `s` contains a literal `'`."""
    if "'" not in s:
        return f"'{s}'"
    escaped = s.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def run_setup(provider: CliProvider) -> int:
    click.echo(f"sub-pool CLI setup ({provider.name})\n")
    existing = Config.load()
    pool_default = existing.pool_url if existing else ""
    while True:
        pool_url = click.prompt(
            "Pool URL", default=pool_default or "https://",
            show_default=bool(pool_default),
        ).strip()
        if pool_url.startswith(("http://", "https://")) and len(pool_url) > 8:
            break
        click.echo("  → must start with http:// or https://", err=True)
    api_key = click.prompt("API key (cp-...)", hide_input=True).strip()
    default_account = click.prompt(
        f"Default {provider.name} account (empty = strategy-based)",
        default=(existing.default_account_for(provider)
                 if existing else "") or "",
        show_default=False,
    ).strip()

    # Preserve other providers' sections — setup is per-provider.
    per_provider = (dict(existing._per_provider) if existing else {})
    per_provider[provider.name] = {
        "bin": (existing.bin_for(provider) if existing else provider.default_bin),
        "default_account": default_account,
    }
    cfg = Config(pool_url=pool_url, api_key=api_key, per_provider=per_provider)
    cfg.write()
    click.echo(f"\nWrote {CONFIG_PATH}")
    return 0


def run_status(provider: CliProvider) -> int:
    cfg = Config.load()
    if cfg is None:
        click.echo(f"No config — run `sp-{provider.name} --setup`", err=True)
        return 1
    click.echo(f"pool_url      {cfg.pool_url}")
    click.echo(f"api_key       {cfg.api_key[:12]}…")
    click.echo(f"provider      {provider.name}")
    click.echo(f"binary        {cfg.bin_for(provider)}")
    click.echo(f"default_acct  {cfg.default_account_for(provider) or '(strategy)'}")
    click.echo(f"config_path   {CONFIG_PATH}")
    return 0


# ============================================================ session dir


def _atomic_write_json(path: Path, payload: Any) -> None:
    """Mode 0o600 from the start (no readable window) + atomic rename."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(json.dumps(payload, indent=2))
    os.replace(tmp, path)


def _is_per_session(name: str, prefixes: tuple[str, ...]) -> bool:
    return any(name.startswith(p) for p in prefixes)


def _merge_into(src: Path, dst: Path) -> None:
    """Recursively copy entries from `src` into `dst`. Existing entries
    in `dst` always win — a concurrent session may have already
    committed its own copy.
    """
    dst.mkdir(parents=True, exist_ok=True)
    for entry in src.iterdir():
        d = dst / entry.name
        if entry.is_dir() and not entry.is_symlink():
            _merge_into(entry, d)
            continue
        if d.exists() or d.is_symlink():
            continue
        try:
            shutil.copy2(entry, d, follow_symlinks=True)
        except Exception:  # noqa: BLE001
            pass


def commit_session_to_home(
    session_dir: Path, home: Path, provider: CliProvider,
) -> None:
    """Persist anything the spawned CLI wrote into `session_dir` back
    into the persistent `home`.

    Entries that are symlinks pointed at `home` already routed writes
    there; nothing to do. Entries matching `cred_skip_prefixes` are
    per-session and must never leak (would persist a stale token).

    Best-effort — IO failures suppressed; shutdown must remain
    unblockable.
    """
    if not session_dir.exists():
        return
    home.mkdir(parents=True, exist_ok=True)
    for entry in session_dir.iterdir():
        if _is_per_session(entry.name, provider.cred_skip_prefixes):
            continue
        if entry.is_symlink():
            continue
        dst = home / entry.name
        try:
            if entry.is_dir():
                _merge_into(entry, dst)
            elif not dst.exists():
                shutil.copy2(entry, dst, follow_symlinks=True)
        except Exception:  # noqa: BLE001
            pass


def setup_session_dir(
    home: Path, session_dir: Path, provider: CliProvider,
) -> None:
    """Wire `session_dir` so the spawned CLI sees it as its config dir.

    Every top-level entry in `home` (skipping `cred_skip_prefixes`)
    gets symlinked into `session_dir`. The credentials file is NOT
    symlinked — we write a per-session copy so two concurrent sessions
    can bind to different leases without clobbering each other.

    Re-runnable: rebuilds the symlink layout from `home`'s current
    contents on each call, so files persisted by a previous session
    show up automatically.
    """
    home.mkdir(parents=True, exist_ok=True)
    os.chmod(home, 0o700)
    # Seed any first-run state the provider needs in its persistent
    # home (claude wants a `.claude.json` so Code's onboarding wizard
    # is skipped on the leased REPL). Runs before the symlink loop so
    # the seeded file shows up in the session dir too.
    if provider.init_home is not None:
        provider.init_home(home)
    for entry in home.iterdir():
        if _is_per_session(entry.name, provider.cred_skip_prefixes):
            continue
        target = session_dir / entry.name
        if target.is_symlink() or target.exists():
            if target.is_dir() and not target.is_symlink():
                shutil.rmtree(target)
            else:
                target.unlink()
        os.symlink(entry.resolve(), target)


# ============================================================ pool I/O


class PoolHTTP:
    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._client = httpx.AsyncClient(
            base_url=cfg.pool_url,
            headers={"Authorization": f"Bearer {cfg.api_key}"},
            timeout=httpx.Timeout(30.0, connect=10.0),
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def lease(self, provider: CliProvider, *,
                    account: str | None = None,
                    user_id: str | None = None) -> dict:
        body: dict[str, Any] = dict(provider.lease_body_extras)
        if account:
            body["account"] = account
        if user_id:
            body["user_id"] = user_id
        r = await self._client.post("/credentials/lease", json=body)
        r.raise_for_status()
        return r.json()

    async def refresh_token(self, lease_id: str) -> dict:
        r = await self._client.post(f"/credentials/lease/{lease_id}/token")
        r.raise_for_status()
        return r.json()

    async def release(self, lease_id: str) -> None:
        with suppress(Exception):
            await self._client.request(
                "DELETE", f"/credentials/lease/{lease_id}", json={},
            )

    async def health(self, lease_id: str) -> dict:
        r = await self._client.get(f"/credentials/lease/{lease_id}/health")
        r.raise_for_status()
        return r.json()


# ============================================================ session main


async def run_session(
    provider: CliProvider,
    prompt_args: tuple[str, ...],
    account: str | None,
    bin_override: str | None,
    verbose: bool,
) -> int:
    cfg = Config.load()
    if cfg is None:
        click.echo(
            f"No pool config — run `sp-{provider.name} --setup` first.",
            err=True,
        )
        return 2

    if verbose:
        logging.basicConfig(
            level=logging.INFO, format=f"[sp-{provider.name}] %(message)s",
            stream=sys.stderr,
        )
    bin_path = bin_override or cfg.bin_for(provider)
    chosen_account = account or cfg.default_account_for(provider)
    home_dir = SP_ROOT / provider.home_subdir
    user_id = os.environ.get("USER") or os.environ.get("USERNAME")

    pool = PoolHTTP(cfg)
    try:
        try:
            lease = await pool.lease(
                provider, account=chosen_account, user_id=user_id,
            )
        except httpx.HTTPStatusError as e:
            click.echo(_describe_http_error(e, "lease"), err=True)
            return 3
        except httpx.HTTPError as e:
            click.echo(f"[sp-{provider.name}] pool unreachable: {e}", err=True)
            return 3
        if verbose:
            log.info("leased %s (lease_id=%s)",
                     lease.get("account"), lease.get("lease_id"))

        # Per-session temp config dir. Holds ONLY the per-session
        # credentials; everything else lives in `home_dir` via symlinks
        # set up by `setup_session_dir`. The session_dir is rmtree'd on
        # exit; symlink targets in `home_dir` persist.
        session_dir = Path(tempfile.mkdtemp(prefix=f"sp-{provider.name}-"))
        try:
            setup_session_dir(home_dir, session_dir, provider)
            provider.write_credentials(
                session_dir / provider.cred_filename, lease,
            )

            env = os.environ.copy()
            env[provider.config_dir_env] = str(session_dir)
            for k in provider.bypass_env_vars:
                env.pop(k, None)

            proc = await asyncio.create_subprocess_exec(
                bin_path, *prompt_args, env=env,
            )

            # Forward common signals so Ctrl-C reaches the child. Record
            # which handlers we installed so the finally block can
            # uninstall — leaving them set could fire against an
            # already-exited proc, or (in tests that reuse a loop)
            # bleed across sessions.
            loop = asyncio.get_running_loop()
            installed_signals: list[int] = []
            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
                try:
                    loop.add_signal_handler(sig, lambda s=sig: _forward(proc, s))
                except (NotImplementedError, RuntimeError):
                    continue
                installed_signals.append(sig)

            # Background tasks: token refresh + health-driven account swap.
            current = {"lease": lease}
            # Set by a successful swap so the expiry-anchored refresh loop
            # re-anchors to the NEW lease's token instead of sleeping out a
            # now-stale deadline — a swapped-in token may have far less life
            # left than the one it replaced.
            refresh_wake = asyncio.Event()

            async def on_swap_needed(health: dict) -> None:
                # Read the from-account BEFORE swap_credentials runs —
                # the call mutates `current["lease"]` in place (that's
                # what closes the refresh/swap race; see _swap.py).
                old_account = current["lease"].get("account")
                try:
                    new = await swap_credentials(
                        pool, current, session_dir, provider,
                        account=chosen_account,
                    )
                    # swap_credentials already advanced current["lease"]
                    # (incl. token_expires_at) in its sync block; wake the
                    # refresh loop so it re-anchors to the new expiry.
                    refresh_wake.set()
                    if verbose:
                        log.info(
                            "swapped %s → %s (reason: %s)",
                            old_account, new.get("account"),
                            (health.get("reason") or "unhealthy")[:80],
                        )
                except NoSwapAvailable as e:
                    if verbose:
                        log.info("swap deferred: %s", e)

            refresher = asyncio.create_task(
                _token_refresh_loop(
                    pool, current, session_dir, provider, verbose,
                    wake=refresh_wake,
                ),
                name=f"sp-{provider.name}-refresh",
            )
            watcher = asyncio.create_task(
                HealthWatcher(
                    fetch=lambda: pool.health(current["lease"]["lease_id"]),
                    on_unhealthy=on_swap_needed,
                ).run(),
                name=f"sp-{provider.name}-health",
            )

            try:
                exit_code = await proc.wait()
            finally:
                for sig in installed_signals:
                    with suppress(NotImplementedError, RuntimeError):
                        loop.remove_signal_handler(sig)
                refresher.cancel()
                watcher.cancel()
                for t in (refresher, watcher):
                    with suppress(asyncio.CancelledError, Exception):
                        await t
                await pool.release(current["lease"]["lease_id"])
        finally:
            with suppress(Exception):
                commit_session_to_home(session_dir, home_dir, provider)
            shutil.rmtree(session_dir, ignore_errors=True)
        return exit_code
    finally:
        await pool.aclose()


def _forward(proc: asyncio.subprocess.Process, sig: int) -> None:
    with suppress(ProcessLookupError, OSError):
        proc.send_signal(sig)


async def _refresh_once(
    pool: PoolHTTP, current: dict, session_dir: Path,
    provider: CliProvider, verbose: bool,
) -> bool:
    """Refresh access_token once. Returns True iff the new token was
    persisted; False if the call failed OR a concurrent swap rotated
    `current["lease"]` while our POST /token was in flight — in that
    case our refresh is stale and writing it would clobber the swap's
    credentials file with the (now-cooled) prior account's token.
    """
    old_lease_id = current["lease"]["lease_id"]
    try:
        new = await pool.refresh_token(old_lease_id)
    except Exception as e:  # noqa: BLE001
        if verbose:
            log.info("token refresh failed: %s", e)
        return False
    if current["lease"]["lease_id"] != old_lease_id:
        if verbose:
            log.info("token refresh dropped (lease swapped during refresh)")
        return False
    merged = {**current["lease"], **new}
    current["lease"] = merged
    provider.write_credentials(session_dir / provider.cred_filename, merged)
    if verbose:
        log.info("token refreshed (expires_at=%s)",
                 merged.get("token_expires_at"))
    return True


async def _token_refresh_loop(
    pool: PoolHTTP, current: dict, session_dir: Path,
    provider: CliProvider, verbose: bool,
    *, wake: "asyncio.Event | None" = None,
    slack_s: float = 600.0, min_sleep_s: float = 60.0,
    fallback_s: float = 50 * 60,
) -> None:
    """Rotate access_token via the pool BEFORE it expires.

    Anthropic access tokens live only ~1h and the leased bundle is
    sanitized (refreshToken=""), so the spawned CLI can't self-refresh —
    the pool is the sole refresher, and the running `claude` re-reads the
    rewritten .credentials.json across requests (see _swap.py).

    The catch that broke this: a leased token is NOT freshly minted. The
    pool only re-mints at seal time when under `slack_s` of life remains,
    so a lease can hand out a token with as little as ~10min left. The old
    fixed 50-min cadence therefore fired long AFTER such a token had
    already died mid-session → an unrecoverable 401 (the exact failure
    users hit). So anchor every sleep to the token's ACTUAL expiry
    (mirrors PooledClient._poll_loop), and let a swap wake us to re-anchor
    against the replacement token — which may itself be short-lived.

    See `_refresh_once` for the concurrent-swap race-safety guard.
    """
    if wake is None:
        wake = asyncio.Event()  # never set → wait_for always times out
    while True:
        exp = float(current["lease"].get("token_expires_at") or 0.0)
        # Shared with both SDK poll loops (see _shared.next_refresh_sleep) so the
        # three schedulers can't drift again. A refresh that FAILS leaves exp
        # near-now, so the min_sleep_s floor makes us retry every min_sleep_s
        # until it lands — or the token dies, at which point a genuinely-dead
        # refresh_token has already flipped the account INVALID and the health
        # watcher swaps us off it.
        sleep_s = next_refresh_sleep(
            exp, time.time(),
            slack_s=slack_s, min_sleep_s=min_sleep_s, fallback_s=fallback_s,
        )
        try:
            await asyncio.wait_for(wake.wait(), timeout=sleep_s)
        except asyncio.TimeoutError:
            # A swap can land in the same tick the deadline fires (set the
            # wake just after wait_for times out); honor it so we re-anchor
            # to the swapped-in token rather than refreshing a lease the swap
            # already wrote fresh credentials for.
            if wake.is_set():
                wake.clear()
                continue
        except asyncio.CancelledError:
            return
        else:
            # Woken by a swap: current["lease"] already points at the new
            # token (written by swap_credentials); re-anchor without
            # refreshing, since the swap installed fresh credentials.
            wake.clear()
            continue
        await _refresh_once(pool, current, session_dir, provider, verbose)


def _describe_http_error(e: httpx.HTTPStatusError, action: str) -> str:
    try:
        detail = e.response.json()
        if isinstance(detail, dict):
            inner = detail.get("detail")
            if isinstance(inner, dict):
                return (f"[sp] {action} failed ({e.response.status_code}): "
                        f"{inner.get('code')}: {inner.get('message')}")
            if isinstance(inner, str):
                return (f"[sp] {action} failed ({e.response.status_code}): "
                        f"{inner}")
    except Exception:
        pass
    return (f"[sp] {action} failed ({e.response.status_code}): "
            f"{e.response.text[:200]}")
