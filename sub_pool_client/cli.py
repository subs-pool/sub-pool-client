"""cpool — run `claude` CLI under a credential leased from a sub-pool server.

Default invocation:
    cpool                       # interactive REPL, equivalent to bare `claude`
    cpool "prompt"              # one-shot (passes prompt to claude)
    cpool -- /resume            # arguments after `--` go straight to claude

Setup / introspection (flags only, no subcommands — the tool's whole job
is "run claude", everything else is incidental):
    cpool --setup               # one-time wizard, writes ~/.config/sub-pool/cli.toml
    cpool --status              # print current pool URL + last-used account
    cpool --account NAME        # bind to a specific pool account (skips
                                #   round-robin order; still subject to the
                                #   api key's strategy eligibility — e.g. a
                                #   pinned key can only choose from its
                                #   allowlist)
    cpool --claude-bin /path    # override `claude` binary location
    cpool --verbose             # print swap / refresh events to stderr

The credential rotation that happens during a session is intentionally
invisible: cpool runs the `claude` binary as a child process, hands it
stdin/stdout/stderr directly, and merely arranges for the
.credentials.json in CLAUDE_CONFIG_DIR to always carry a live token.

cpool keeps its own `~/.sub-pool/` as a sibling to `~/.claude/`:

    ~/.sub-pool/
    ├── cli.toml          # pool URL + api_key (this tool's config)
    └── home/             # claude CLI's CLAUDE_CONFIG_DIR for cpool sessions
        ├── settings.json
        ├── projects/     # conversation history, accumulates across sessions
        └── ...

claude's writes during a cpool session land in `~/.sub-pool/home/`,
never in `~/.claude/`. The two are fully independent — cpool starts
with an empty home and grows its own state. If you want cpool to
inherit anything from your real claude config, copy it over manually.
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
from contextlib import suppress
from pathlib import Path
from typing import Any

import click
import httpx

from sub_pool_client._health_watcher import HealthWatcher
from sub_pool_client._swap import NoSwapAvailable, swap_credentials


log = logging.getLogger("cpool")

# cpool's own root, sibling to the user's `~/.claude/`. Holds both this
# tool's config (cli.toml) and the persistent CLAUDE_CONFIG_DIR for
# cpool sessions (home/). The user's `~/.claude/` is never read or
# written by cpool — explicit isolation.
CPOOL_ROOT = Path.home() / ".sub-pool"
CONFIG_PATH = CPOOL_ROOT / "cli.toml"
CPOOL_HOME_DIR = CPOOL_ROOT / "home"

CREDENTIALS_FILE = ".credentials.json"
# Everything starting with ".credentials" is treated as per-session
# OAuth state we don't want bleeding into the persistent home. Covers:
#   - .credentials.json itself (cpool writes per-session, claude rereads)
#   - .credentials.json.tmp left by a partial write_credentials_file
#   - any future .credentials-* sidecar files claude might emit
# Both setup_session_dir (skip in home → no symlink) and
# commit_session_to_home (skip in session_dir → no copy back) use this.
_CRED_PREFIX = ".credentials"


# ============================================================ config

class Config:
    """Loaded ~/.config/sub-pool/cli.toml."""

    def __init__(self, pool_url: str, api_key: str,
                 default_account: str | None = None,
                 claude_bin: str = "claude"):
        self.pool_url = pool_url.rstrip("/")
        self.api_key = api_key
        self.default_account = default_account
        self.claude_bin = claude_bin

    @classmethod
    def load(cls) -> "Config | None":
        if not CONFIG_PATH.exists():
            return None
        import tomllib
        data = tomllib.loads(CONFIG_PATH.read_text())
        pool = data.get("pool") or {}
        defaults = data.get("defaults") or {}
        if not pool.get("url") or not pool.get("api_key"):
            return None
        return cls(
            pool_url=pool["url"],
            api_key=pool["api_key"],
            default_account=defaults.get("account") or None,
            claude_bin=defaults.get("claude_bin") or "claude",
        )

    def write(self) -> None:
        CPOOL_ROOT.mkdir(parents=True, exist_ok=True)
        # Hand-written TOML so we don't need a writer dep (tomllib reads only).
        # Use literal strings (single-quoted) — they don't interpret escapes,
        # so we don't have to worry about \\ / " / unicode in values.
        body = (
            "[pool]\n"
            f"url = {_toml_str(self.pool_url)}\n"
            f"api_key = {_toml_str(self.api_key)}\n"
            "\n"
            "[defaults]\n"
            f"account = {_toml_str(self.default_account or '')}\n"
            f"claude_bin = {_toml_str(self.claude_bin)}\n"
        )
        # Open with explicit mode 0o600 AND fchmod after, so:
        #  (a) New files are created at 0o600 from the start — no race
        #      window where the API key is readable to other local users.
        #  (b) Existing files (a prior cli.toml whose mode was loosened
        #      by mistake) get their mode corrected before we write the
        #      new key.
        fd = os.open(str(CONFIG_PATH),
                     os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(body)


def _toml_str(s: str) -> str:
    """Render `s` as a TOML string. Prefers a literal string (`'…'`,
    no escape interpretation). Falls back to a basic string (`"…"`)
    with the standard escapes only when the value contains a `'` —
    extremely rare for the fields we write (URL / api_key / account /
    binary path), but worth handling cleanly.
    """
    if "'" not in s:
        return f"'{s}'"
    escaped = s.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def run_setup() -> int:
    click.echo("sub-pool CLI setup\n")
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
        "Default account (empty = strategy-based)",
        default=existing.default_account if existing else "",
        show_default=False,
    ).strip()
    cfg = Config(pool_url=pool_url, api_key=api_key,
                 default_account=default_account or None)
    cfg.write()
    click.echo(f"\nWrote {CONFIG_PATH}")
    return 0


def run_status() -> int:
    cfg = Config.load()
    if cfg is None:
        click.echo("No config — run `cpool --setup`", err=True)
        return 1
    click.echo(f"pool_url      {cfg.pool_url}")
    click.echo(f"api_key       {cfg.api_key[:12]}…")
    click.echo(f"default_acct  {cfg.default_account or '(strategy)'}")
    click.echo(f"claude_bin    {cfg.claude_bin}")
    click.echo(f"config_path   {CONFIG_PATH}")
    return 0


# ============================================================ credentials

def write_credentials_file(config_dir: Path, lease: dict) -> None:
    """Write/replace `.credentials.json` atomically.

    Shape matches what `claude` CLI reads: top-level `claudeAiOauth`
    with accessToken + refreshToken + expiresAt + scopes +
    subscriptionType. We deliberately leave `refreshToken` blank — the
    pool owns the refresh chain; the client polls `/token` to rotate
    accessToken (`HealthWatcher` / `_swap.swap_credentials`).

    `scope` arrives from the pool as a space-separated string (Anthropic
    OAuth convention); we split it into the array shape claude expects.
    `subscription_type` falls back to "claude_max" if absent (older
    pool servers don't include it).
    """
    path = config_dir / CREDENTIALS_FILE
    scope_raw = (lease.get("scope") or "").strip()
    scopes = scope_raw.split() if scope_raw else []
    payload = {
        "claudeAiOauth": {
            "accessToken": lease["access_token"],
            "refreshToken": "",                  # pool-only
            "expiresAt": int(float(lease["token_expires_at"]) * 1000),
            "scopes": scopes,
            "subscriptionType": (lease.get("subscription_type") or "claude_max"),
        },
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    # Create + chmod via the fd so the file is never visible at any
    # mode wider than 0o600 — defense-in-depth even though session_dir
    # is 0o700.
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(json.dumps(payload, indent=2))
    os.replace(tmp, path)


def _merge_into(src: Path, dst: Path) -> None:
    """Recursively copy entries from `src` into `dst`. For directories
    that exist on both sides we recurse. For individual files that
    already exist in `dst` we skip — `home`'s version always wins on
    conflict (a concurrent session may have just committed its own
    copy). Used by `commit_session_to_home`.
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
            pass  # best-effort; never block session shutdown


def commit_session_to_home(session_dir: Path, home: Path) -> None:
    """Persist anything claude wrote into `session_dir` (top-level real
    files / dirs) back into the persistent `home`. Entries that are
    symlinks were writes already routed through to `home`, so we skip
    those. `.credentials.json` is per-session and explicitly excluded.

    Only matters for the very first cpool session (when `home` is empty
    and `setup_session_dir` couldn't symlink any of claude's known
    write dirs). Once `home` has `projects/`, `sessions/`, etc., later
    sessions get symlinks and claude's writes flow into `home`
    directly — making this function a no-op for entries that already
    exist as symlinks.

    Best-effort: any IO failure here is suppressed; cpool's shutdown
    must remain unblockable.
    """
    if not session_dir.exists():
        return
    home.mkdir(parents=True, exist_ok=True)
    for entry in session_dir.iterdir():
        # Skip the OAuth credential family — including stray
        # .credentials.json.tmp left by a failed write_credentials_file
        # write. Copying that .tmp into home would leak a partial
        # token onto disk as a permanent (mode 0o600 but still stray)
        # sidecar file.
        if entry.name.startswith(_CRED_PREFIX):
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


def setup_session_dir(home: Path, session_dir: Path) -> None:
    """Wire `session_dir` so it can serve as CLAUDE_CONFIG_DIR:
      - `home` (cpool's persistent claude home) is created if missing
        at 0o700, and otherwise left alone. cpool starts cold — no
        seeding from `~/.claude/` or anywhere else; claude CLI is
        fine starting from an empty config dir.
      - Every entry in `home` (except `.credentials.json`) is symlinked
        into `session_dir`. claude's writes through those symlinks
        persist in `home` across sessions.
      - `.credentials.json` is intentionally NOT symlinked — cpool
        writes a per-session credential into `session_dir` directly,
        which is what lets two concurrent cpool sessions bind to
        different leases without clobbering each other.

    `session_dir` is expected to already exist (typically a fresh
    tempfile.mkdtemp at 0o700).

    Re-runnable: each call rebuilds the symlink layout from `home`'s
    current contents, so files added to `home` between sessions
    (including ones committed by an earlier session — see
    `commit_session_to_home`) show up on the next call automatically.
    """
    home.mkdir(parents=True, exist_ok=True)
    os.chmod(home, 0o700)
    for entry in home.iterdir():
        # Same skip rule as commit_session_to_home — if a stale
        # .credentials.json or .credentials.json.tmp lives in home
        # (from a crash), don't symlink it into the session; cpool
        # writes a fresh per-session .credentials.json directly.
        if entry.name.startswith(_CRED_PREFIX):
            continue
        target = session_dir / entry.name
        # session_dir is normally fresh, but in tests / reuse scenarios
        # we may need to evict stale entries cleanly.
        if target.is_symlink() or target.exists():
            if target.is_dir() and not target.is_symlink():
                shutil.rmtree(target)
            else:
                target.unlink()
        # Symlink at the top level points into `home` — writes through
        # it (e.g. `projects/<sid>/<file>.jsonl` appends from claude)
        # persist to `home`, never escaping cpool's own world.
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

    async def lease(self, account: str | None = None,
                    user_id: str | None = None) -> dict:
        body: dict[str, Any] = {}
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
    prompt_args: tuple[str, ...],
    account: str | None,
    claude_bin: str | None,
    verbose: bool,
) -> int:
    cfg = Config.load()
    if cfg is None:
        click.echo("No pool config — run `cpool --setup` first.", err=True)
        return 2

    if verbose:
        logging.basicConfig(level=logging.INFO, format="[cpool] %(message)s",
                            stream=sys.stderr)
    bin_path = claude_bin or cfg.claude_bin
    chosen_account = account or cfg.default_account
    user_id = os.environ.get("USER") or os.environ.get("USERNAME")

    pool = PoolHTTP(cfg)
    try:
        # Lease the initial credential.
        try:
            lease = await pool.lease(account=chosen_account, user_id=user_id)
        except httpx.HTTPStatusError as e:
            click.echo(_describe_http_error(e, "lease"), err=True)
            return 3
        except httpx.HTTPError as e:
            click.echo(f"[cpool] pool unreachable: {e}", err=True)
            return 3
        if verbose:
            log.info("leased %s (lease_id=%s)",
                     lease.get("account"), lease.get("lease_id"))

        # Per-session temp config dir. Holds ONLY the per-session
        # `.credentials.json`; everything else (settings, history,
        # projects, ...) lives in CPOOL_HOME_DIR via symlinks set up
        # by setup_session_dir. The session_dir itself is rmtree'd on
        # exit; the symlink TARGETS in CPOOL_HOME_DIR persist.
        session_dir = Path(tempfile.mkdtemp(prefix="cpool-"))
        try:
            setup_session_dir(CPOOL_HOME_DIR, session_dir)
            write_credentials_file(session_dir, lease)

            env = os.environ.copy()
            env["CLAUDE_CONFIG_DIR"] = str(session_dir)
            # Strip env vars that would shortcut the cred file we just wrote.
            for k in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN",
                      "CLAUDE_CODE_OAUTH_TOKEN"):
                env.pop(k, None)

            proc = await asyncio.create_subprocess_exec(
                bin_path, *prompt_args, env=env,
            )

            # Forward common signals to the child so Ctrl-C reaches claude.
            # Record which handlers we installed so the finally-block can
            # uninstall them — leaving them set would let a later signal
            # call _forward() against an already-exited proc, or (in test
            # contexts that reuse a loop) bleed across sessions.
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

            async def on_swap_needed(health: dict) -> None:
                # Read the from-account BEFORE swap_credentials runs —
                # the call mutates `current["lease"]` in place (that's
                # what closes the refresh/swap race; see _swap.py).
                old_account = current["lease"].get("account")
                try:
                    new = await swap_credentials(
                        pool, current, session_dir,
                        account=chosen_account,
                    )
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
                _token_refresh_loop(pool, current, session_dir, verbose),
                name="cpool-refresh",
            )
            watcher = asyncio.create_task(
                HealthWatcher(
                    fetch=lambda: pool.health(current["lease"]["lease_id"]),
                    on_unhealthy=on_swap_needed,
                ).run(),
                name="cpool-health",
            )

            try:
                exit_code = await proc.wait()
            finally:
                # Uninstall signal handlers first — they target `proc`
                # via closure and we're about to let `proc` get GC'd.
                for sig in installed_signals:
                    with suppress(NotImplementedError, RuntimeError):
                        loop.remove_signal_handler(sig)
                refresher.cancel()
                watcher.cancel()
                for t in (refresher, watcher):
                    with suppress(asyncio.CancelledError, Exception):
                        await t
                # Release the lease we currently hold. Other historical
                # leases (from swaps) were released inside `_swap`.
                await pool.release(current["lease"]["lease_id"])
        finally:
            # Sweep any new top-level entries claude wrote into the
            # session_dir back to CPOOL_HOME_DIR before we rmtree —
            # matters on first-ever cpool run when home was empty and
            # the symlink layout was therefore minimal.
            with suppress(Exception):
                commit_session_to_home(session_dir, CPOOL_HOME_DIR)
            shutil.rmtree(session_dir, ignore_errors=True)
        return exit_code
    finally:
        await pool.aclose()


def _forward(proc: asyncio.subprocess.Process, sig: int) -> None:
    """Best-effort signal forwarding. claude handles SIGINT cleanly;
    cpool waits for the child to exit before doing its own cleanup."""
    with suppress(ProcessLookupError, OSError):
        proc.send_signal(sig)


async def _refresh_once(
    pool: PoolHTTP, current: dict, session_dir: Path, verbose: bool,
) -> bool:
    """Refresh access_token once. Returns True iff the new token was
    persisted; False if the call failed OR a concurrent swap rotated
    `current["lease"]` while our POST /token was in flight — in that
    case our refresh is stale and writing it would clobber the swap's
    `.credentials.json` with the (now-cooled) prior account's token.

    The race is real: refresh runs on a 50-min cadence and the health
    watcher's swap can fire at any 30-s tick in between, so the window
    spans the entire `await pool.refresh_token(...)` HTTP roundtrip.
    """
    old_lease_id = current["lease"]["lease_id"]
    try:
        new = await pool.refresh_token(old_lease_id)
    except Exception as e:  # noqa: BLE001
        if verbose:
            log.info("token refresh failed: %s", e)
        return False
    if current["lease"]["lease_id"] != old_lease_id:
        # Swap raced us — drop this refresh on the floor. The new
        # account already has a fresh token written by `_swap`.
        if verbose:
            log.info("token refresh dropped (lease swapped during refresh)")
        return False
    merged = {**current["lease"], **new}
    current["lease"] = merged
    write_credentials_file(session_dir, merged)
    if verbose:
        log.info("token refreshed (expires_at=%s)",
                 merged.get("token_expires_at"))
    return True


async def _token_refresh_loop(
    pool: PoolHTTP, current: dict, session_dir: Path, verbose: bool,
    *, interval_s: float = 50 * 60,
) -> None:
    """Periodically rotate access_token via pool. Same cadence as
    PooledClient. See `_refresh_once` for the race-safety guard."""
    while True:
        try:
            await asyncio.sleep(interval_s)
        except asyncio.CancelledError:
            return
        await _refresh_once(pool, current, session_dir, verbose)


def _describe_http_error(e: httpx.HTTPStatusError, action: str) -> str:
    try:
        detail = e.response.json()
        if isinstance(detail, dict):
            inner = detail.get("detail")
            if isinstance(inner, dict):
                return f"[cpool] {action} failed ({e.response.status_code}): " \
                       f"{inner.get('code')}: {inner.get('message')}"
            if isinstance(inner, str):
                return f"[cpool] {action} failed ({e.response.status_code}): {inner}"
    except Exception:
        pass
    return f"[cpool] {action} failed ({e.response.status_code}): {e.response.text[:200]}"


# ============================================================ click entry

@click.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    help="Run `claude` CLI with credentials leased from a sub-pool server.",
)
@click.argument("claude_args", nargs=-1, type=click.UNPROCESSED)
@click.option("--setup", is_flag=True, help="One-time pool config wizard.")
@click.option("--status", is_flag=True, help="Show current config + exit.")
@click.option("--account", default=None,
              help="Bind to specific account, skipping round-robin order. "
                   "Subject to the api key's strategy eligibility.")
@click.option("--claude-bin", default=None, help="Override path to `claude` binary.")
@click.option("--verbose", is_flag=True, help="Print swap / refresh events to stderr.")
def main(claude_args, setup, status, account, claude_bin, verbose) -> None:
    if setup:
        sys.exit(run_setup())
    if status:
        sys.exit(run_status())
    code = asyncio.run(run_session(claude_args, account, claude_bin, verbose))
    sys.exit(code)


if __name__ == "__main__":
    main()
