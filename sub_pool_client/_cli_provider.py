"""Per-provider knobs for the sp-claude / sp-codex / sp-grok / sp-muse CLIs.

The lease loop in `_cli_core` is provider-agnostic — same lease /
refresh / release flow regardless of which upstream CLI gets spawned.
Everything that DOES differ between Claude and Codex is collapsed into
this `CliProvider` value: file names, env var names, default binary,
credential payload shape.

Add a third provider by appending an instance below — no other module
needs to change.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from sub_pool_client import _usage


def _write_claude_credentials(path: Path, lease: dict) -> None:
    """Write `.credentials.json` in the shape the `claude` CLI reads.

    `refreshToken` is intentionally blanked — the pool owns the refresh
    chain; consumers poll `/credentials/lease/{id}/token` for rotations
    via `_token_refresh_loop`. `scope` arrives as a space-separated
    string (Anthropic OAuth convention) and gets split into the array
    shape claude expects.
    """
    scope_raw = (lease.get("scope") or "").strip()
    scopes = scope_raw.split() if scope_raw else []
    payload = {
        "claudeAiOauth": {
            "accessToken": lease["access_token"],
            "refreshToken": "",
            "expiresAt": int(float(lease["token_expires_at"]) * 1000),
            "scopes": scopes,
            "subscriptionType": (lease.get("subscription_type") or "claude_max"),
        },
    }
    _atomic_write_json(path, payload)


def _write_grok_auth(path: Path, lease: dict) -> None:
    """Write the leased `auth.json` for the `grok` CLI.

    Same deal as codex — the pool already stripped `refresh_token`, so this
    just persists the sanitized bundle to `$GROK_HOME/auth.json`. Note the
    payload keys are grok-build's own: the access token is `key`, and expiry
    is `expires_at` as an RFC3339 string. Neither is rewritten here; the pool
    is the only thing that mints them.
    """
    creds = lease.get("credentials") or {}
    auth = creds.get("auth_json")
    if not isinstance(auth, dict):
        raise RuntimeError(
            "lease response did not include a Grok auth_json bundle"
        )
    _atomic_write_json(path, auth)


def _write_codex_auth(path: Path, lease: dict) -> None:
    """Write the leased `auth.json` exactly as the pool returned it.

    The pool already strips `refresh_token` server-side — we just persist
    the sanitized bundle so the local `codex` CLI can read it from
    `$CODEX_HOME/auth.json`.
    """
    creds = lease.get("credentials") or {}
    auth = creds.get("auth_json")
    if not isinstance(auth, dict):
        raise RuntimeError(
            "lease response did not include a Codex auth_json bundle"
        )
    _atomic_write_json(path, auth)


def _write_nothing(path: Path, lease: dict) -> None:
    """muse reads its credential from the environment, not a file — see
    `_muse_env`. Writing the api_key to disk anyway would only add a copy to
    clean up."""
    del path, lease


def _muse_auth(lease: dict) -> dict:
    creds = lease.get("credentials") or {}
    auth = creds.get("auth_json") if isinstance(creds, dict) else None
    return auth if isinstance(auth, dict) else {}


def _muse_key(lease: dict) -> str:
    key = _muse_auth(lease).get("api_key")
    if not isinstance(key, str) or not key:
        raise RuntimeError("lease response did not include a Muse api_key")
    return key


def _muse_env(lease: dict) -> dict[str, str]:
    """`META_API_KEY` for the spawned `muse` — the no-proxy fallback.

    The pool hands out the api_key (never Meta's access_token — that stays
    server-side), and muse documents `META_API_KEY` as winning over any
    logged-in session, so a local `muse login` can't shadow the lease.
    """
    return {"META_API_KEY": _muse_key(lease)}


_MUSE_DEFAULT_BASE = "https://api.meta.ai/v1"


def _muse_origin(lease: dict) -> str:
    from sub_pool_client._api_proxy import origin_of_base_url
    return origin_of_base_url(
        _muse_auth(lease).get("base_url") or lease.get("base_url"),
        _MUSE_DEFAULT_BASE)


def _muse_user_base_url(args: tuple[str, ...]) -> bool:
    """Did the user point muse somewhere themselves? Then we don't proxy —
    theirs wins, and the session falls back to no mid-session swap."""
    return any(a == "--base-url" or a.startswith("--base-url=") for a in args)


def _muse_quota_body(status: int, body: bytes) -> bool:
    """Is this upstream 429 a spent SUBSCRIPTION (swap-worthy) rather than a
    short rate limit (muse's own backoff is right for that)?"""
    return status == 429 and b"quota exhausted" in body.lower()


def _muse_quota_exhausted(line: str) -> bool:
    """Does this stderr line say muse's subscription quota ran out?

    Verified shape (`muse exec`, 1.4.13):
        API error 429 [request_id=…]: Subscription quota exhausted. Your usage
        window resets at 2026-11-02T00:00:00Z. (rate_limit_error)
    Matched on the two transport-level fragments, never on prose alone — the
    pool parses the reset time out of the same line.
    """
    low = line.lower()
    return "api error 429" in low and (
        "rate_limit_error" in low or "quota exhausted" in low)


def _seed_claude_home(home: Path) -> None:
    """Pre-write a minimal `.claude.json` so Claude Code's first-run
    onboarding wizard does not appear inside the leased REPL.

    Claude Code gates its onboarding wizard on the presence of
    `hasCompletedOnboarding` in `$CLAUDE_CONFIG_DIR/.claude.json` —
    not on whether `.credentials.json` already holds a valid OAuth
    token. Worse, the wizard's "Sign in with Claude account" path
    starts a *new* browser OAuth flow rather than reusing the leased
    `.credentials.json`, so falling into the wizard wipes out the
    pool's credential entirely.

    Idempotent: only writes when no `.claude.json` exists yet. If a
    future Claude Code release changes the wizard contract, deleting
    `~/.sub-pool/claude-home/.claude.json` and re-running sp-claude
    is enough to inspect the new schema by hand.
    """
    claude_json = home / ".claude.json"
    if claude_json.exists():
        return
    _atomic_write_json(claude_json, {
        "hasCompletedOnboarding": True,
        "lastOnboardingVersion": "2.1.144",
    })


def _atomic_write_json(path: Path, payload: Any) -> None:
    """Create at 0o600 from the start (no readable window) and atomic-rename."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)
    try:
        with os.fdopen(fd, "w") as f:
            f.write(json.dumps(payload, indent=2))
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    os.replace(tmp, path)


@dataclass(frozen=True)
class ApiProxySpec:
    """How to put a `_api_proxy.KeySwappingProxy` in front of a CLI whose
    credential is fixed in its environment, so it can still swap accounts."""

    # proxy origin (`http://127.0.0.1:N`) -> args prepended to the CLI's own
    args_for: Callable[[str], tuple[str, ...]]
    # the user's args already redirect the CLI → don't proxy
    opted_out: Callable[[tuple[str, ...]], bool]
    # per-session local secret -> env for the child
    env_for: Callable[[str], dict[str, str]]
    # lease -> the real key / upstream origin the proxy forwards with
    key_of: Callable[[dict], str]
    origin_of: Callable[[dict], str]
    # (status, body) -> is this a spent-quota answer worth a swap
    quota_exhausted: Callable[[int, bytes], bool]


@dataclass(frozen=True)
class CliProvider:
    name: str
    # Default binary name resolved from PATH; overridable via CLI flag /
    # cli.toml.
    default_bin: str
    # Environment variable the spawned binary uses to locate its config
    # dir. The CLI sets this to the per-session temp dir. None: the binary
    # isn't redirected (its credential arrives via `credential_env`).
    config_dir_env: str | None
    # Name of the credentials file written into the per-session dir.
    cred_filename: str
    # File-name prefixes that are per-session and must NOT be symlinked
    # from the persistent home, nor copied back. Lets a stray
    # `auth.json.tmp` or `.credentials.json.bak` from a crashed run never
    # leak into the persistent home.
    cred_skip_prefixes: tuple[str, ...]
    # Subdir under `~/.sub-pool/` for this provider's persistent
    # CLAUDE_CONFIG_DIR / CODEX_HOME equivalent.
    home_subdir: str
    # Ambient env vars that would let the spawned binary skip the leased
    # credential file. Stripped from the child env before spawn.
    bypass_env_vars: tuple[str, ...]
    # Extra body fields included in POST /credentials/lease so the pool
    # routes us to a matching account. Empty for claude (the default).
    lease_body_extras: dict
    # Pluggable credential writer — kept on the provider so the engine
    # never has to switch on `name`.
    write_credentials: Callable[[Path, dict], None]
    # How long before expiry the pool re-mints this provider's token. The
    # default suits a CLI that keeps working on a nearly-expired token; raise
    # it for one that stops early. grok refuses to run once its token is
    # within GROK_AUTH_EARLY_INVALIDATION_SECS (300s) of expiry — and since
    # the leased copy has no refresh_token, it can't self-heal, it just
    # reports "not authenticated". Verified against grok 1.0.5.
    refresh_slack_s: float = 600.0
    # Optional first-run hook called on the persistent home dir before
    # symlinks get laid down. Used by Claude to seed a no-onboarding
    # `.claude.json`; Codex doesn't need it.
    init_home: Callable[[Path], None] | None = None
    # Builds the per-session usage collector from the passthrough args (see
    # `_usage.SessionUsage`); its tally rides on the session's final release.
    # None: the provider reports no usage.
    usage: Callable[[tuple[str, ...]], "_usage.SessionUsage"] | None = None
    # Extra env for the child, derived from the lease at spawn. For a CLI
    # that takes its credential from the environment (muse) rather than a
    # file. Env is fixed for the child's lifetime, so such a provider can't
    # pick up a swapped or rotated credential mid-session — see `live_swap`.
    credential_env: Callable[[dict], dict[str, str]] | None = None
    # Can a mid-session swap actually reach the running child? True wherever
    # the credential is a file the CLI re-reads. False for an env-borne one:
    # swapping the lease underneath would move the pool's bookkeeping to a
    # new account while the child keeps spending the old one.
    live_swap: bool = True
    # stderr line -> "upstream says this account's quota is spent". When it
    # matches, the line is reported to the pool (once per lease) so the
    # account cools until the reset time the line names. None: not watched.
    # Only used when `api_proxy` isn't active (the proxy sees the 429 first).
    quota_exhausted: Callable[[str], bool] | None = None
    # Route the CLI's API traffic through a local key-swapping proxy. With
    # it active the session CAN swap (the proxy reads the current lease per
    # request), overriding `live_swap`.
    api_proxy: ApiProxySpec | None = None


CLAUDE = CliProvider(
    name="claude",
    default_bin="claude",
    config_dir_env="CLAUDE_CONFIG_DIR",
    cred_filename=".credentials.json",
    cred_skip_prefixes=(".credentials",),
    home_subdir="claude-home",
    bypass_env_vars=(
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "CLAUDE_CODE_OAUTH_TOKEN",
    ),
    lease_body_extras={},
    write_credentials=_write_claude_credentials,
    init_home=_seed_claude_home,
    usage=_usage.ClaudeCliUsage,
)


CODEX = CliProvider(
    name="codex",
    default_bin="codex",
    config_dir_env="CODEX_HOME",
    cred_filename="auth.json",
    cred_skip_prefixes=("auth.json",),
    home_subdir="codex-home",
    bypass_env_vars=("OPENAI_API_KEY",),
    lease_body_extras={"provider": "codex"},
    write_credentials=_write_codex_auth,
    usage=_usage.CodexCliUsage,
)


GROK = CliProvider(
    name="grok",
    default_bin="grok",
    config_dir_env="GROK_HOME",
    cred_filename="auth.json",
    cred_skip_prefixes=("auth.json",),
    home_subdir="grok-home",
    # An ambient XAI_API_KEY makes grok skip the leased auth.json entirely and
    # bill the pay-per-token API instead of the subscription we leased.
    bypass_env_vars=("XAI_API_KEY",),
    lease_body_extras={"provider": "grok"},
    write_credentials=_write_grok_auth,
    # 30 min, six times grok's own 300s cut-off. Its tokens live 6h, so this
    # still means one rotation per session at most — cheap insurance against
    # a late or briefly-failing refresh dropping the CLI into a login prompt
    # it cannot complete.
    refresh_slack_s=1800.0,
)


MUSE = CliProvider(
    name="muse",
    default_bin="muse",
    # A per-session dir is still laid out (the engine is uniform), but muse
    # isn't pointed at it: its credential comes from META_API_KEY, and its
    # own config/history stay where the user's muse keeps them.
    config_dir_env=None,
    cred_filename="lease.json",
    cred_skip_prefixes=("lease.json",),
    home_subdir="muse-home",
    # Set by `_muse_env` from the lease; any ambient value is dropped first so
    # the user's own key can never be what the child bills.
    bypass_env_vars=("META_API_KEY",),
    lease_body_extras={"provider": "muse"},
    write_credentials=_write_nothing,
    # Fallback (user passed their own --base-url): the real key by env, no
    # swap, 429s scraped off stderr.
    credential_env=_muse_env,
    live_swap=False,
    quota_exhausted=_muse_quota_exhausted,
    # Normal path: `--base-url` at the loopback proxy (verified on muse
    # 1.4.13 for both the TUI and `exec`; plain http to loopback is
    # accepted), with a per-session secret as META_API_KEY. Telemetry still
    # goes straight to api.meta.ai carrying that secret — it's meaningless
    # off this machine, and the 401 it earns there is harmless.
    api_proxy=ApiProxySpec(
        args_for=lambda origin: ("--base-url", f"{origin}/v1"),
        opted_out=_muse_user_base_url,
        env_for=lambda secret: {"META_API_KEY": secret},
        key_of=_muse_key,
        origin_of=_muse_origin,
        quota_exhausted=_muse_quota_body,
    ),
)


_PROVIDERS = {p.name: p for p in (CLAUDE, CODEX, GROK, MUSE)}


def get_provider(name: str) -> CliProvider:
    try:
        return _PROVIDERS[name]
    except KeyError:
        raise ValueError(
            f"unknown provider {name!r} (known: {', '.join(sorted(_PROVIDERS))})"
        ) from None
