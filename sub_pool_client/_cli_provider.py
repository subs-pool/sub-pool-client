"""Per-provider knobs for the sp-claude / sp-codex CLIs.

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
class CliProvider:
    name: str
    # Default binary name resolved from PATH; overridable via CLI flag /
    # cli.toml.
    default_bin: str
    # Environment variable the spawned binary uses to locate its config
    # dir. The CLI sets this to the per-session temp dir.
    config_dir_env: str
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
    # Optional first-run hook called on the persistent home dir before
    # symlinks get laid down. Used by Claude to seed a no-onboarding
    # `.claude.json`; Codex doesn't need it.
    init_home: Callable[[Path], None] | None = None


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
)


def get_provider(name: str) -> CliProvider:
    if name == "claude":
        return CLAUDE
    if name == "codex":
        return CODEX
    raise ValueError(f"unknown provider {name!r}")
