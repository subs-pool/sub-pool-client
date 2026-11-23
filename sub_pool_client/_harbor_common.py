"""Harbor-free helpers behind `sub_pool_client.harbor` (kept apart so they can
be tested, and imported, without the `harbor` package)."""
from __future__ import annotations

import json
import os
import re
import shlex
from pathlib import Path

VENV = "/opt/subpool-client-venv"
URL_ENV, KEY_ENV = "SUB_POOL_URL", "SUB_POOL_KEY"

_RELEASE = re.compile(r"\A\d+\.\d+\.\d+\Z")


def client_requirement(version: str | None) -> str:
    """What the sandbox pip-installs. Default: this very client version, so a
    trial runs the client the host was tested with; a dev / local build (no
    such release on PyPI) or "latest" takes the newest release instead."""
    if version in (None, ""):
        from sub_pool_client._client_id import client_version
        version = client_version()
    if version == "latest" or not _RELEASE.match(version):
        return "sub-pool-client"
    return f"sub-pool-client=={version}"


def install_command(requirement: str) -> str:
    """Root shell that puts sub-pool-client in VENV, whatever Python the task
    image has (or lacks). Ported from harbor-hub's audit runner."""
    req = shlex.quote(requirement)
    return (
        "set -euo pipefail; "
        f'venv="{VENV}"; '
        'bootstrap_venv="/opt/subpool-bootstrap-venv"; '
        'python_bin="$(command -v python3 || command -v python || true)"; '
        'if [ -z "$python_bin" ]; then '
        "if command -v apt-get >/dev/null 2>&1; then "
        "apt-get update; DEBIAN_FRONTEND=noninteractive apt-get install -y python3 python3-venv; "
        "elif command -v apk >/dev/null 2>&1; then apk add --no-cache python3 py3-pip; "
        'else echo "Python 3 is required for sub-pool-client" >&2; exit 127; fi; '
        'python_bin="$(command -v python3 || command -v python || true)"; '
        "fi; "
        'if [ ! -x "$bootstrap_venv/bin/python" ]; then '
        'if ! "$python_bin" -m venv "$bootstrap_venv"; then '
        'rm -rf "$bootstrap_venv"; '
        "if command -v apt-get >/dev/null 2>&1; then "
        "apt-get update; DEBIAN_FRONTEND=noninteractive apt-get install -y python3-venv; "
        "elif command -v apk >/dev/null 2>&1; then apk add --no-cache py3-pip; "
        'else echo "The Python venv module is required for sub-pool-client" >&2; exit 127; fi; '
        '"$python_bin" -m venv "$bootstrap_venv"; '
        "fi; fi; "
        # Task images may export their own site-packages via PYTHONPATH:
        # isolated mode + a forced reinstall keeps uv inside this venv.
        '"$bootstrap_venv/bin/python" -I -m pip install '
        "--upgrade --force-reinstall --no-cache-dir uv; "
        'test -x "$bootstrap_venv/bin/uv"; '
        'UV_PYTHON_INSTALL_DIR="/opt/subpool-python" '
        '"$bootstrap_venv/bin/uv" venv --allow-existing --python 3.12 "$venv"; '
        f'"$bootstrap_venv/bin/uv" pip install --python "$venv/bin/python" --no-cache {req}; '
        'chmod -R a+rX "$venv"'
    )


def pool_env(extra_env: dict[str, str] | None, key_name: str | None) -> dict[str, str]:
    """SUB_POOL_URL / SUB_POOL_KEY for the sandbox, in order: the agent's own
    extra_env (`--ae`), the named key from the host's cli.toml
    (`--ak subpool_key=NAME`), the host environment, the host's default
    cli.toml key. Raises ValueError when none supplies both — better at
    construction than as a failed lease inside every trial."""
    extra = dict(extra_env or {})
    url, key = extra.get(URL_ENV), extra.get(KEY_ENV)
    if not (url and key):
        from sub_pool_client._cli_core import Config, KeyChoiceError
        try:
            cfg = Config.load(key_name) if key_name else None
        except KeyChoiceError as e:
            raise ValueError(str(e)) from None
        if cfg is not None:
            url, key = url or cfg.pool_url, key or cfg.api_key
        url = url or os.environ.get(URL_ENV)
        key = key or os.environ.get(KEY_ENV)
        if not (url and key):
            cfg = Config.load()
            if cfg is not None:
                url, key = url or cfg.pool_url, key or cfg.api_key
    if not (url and key):
        raise ValueError(
            "sub-pool agent needs SUB_POOL_URL and SUB_POOL_KEY: pass them with "
            "`--ae SUB_POOL_URL=… --ae SUB_POOL_KEY=…`, pick a saved key with "
            "`--ak subpool_key=NAME`, export them, or run `sp-claude --setup`")
    return {**extra, URL_ENV: url, KEY_ENV: key}


CLAUDE_WRAPPER = r"""#!/bin/sh
set -eu

direct="$HOME/.local/bin/claude-harbor-direct"
sp_claude="/opt/subpool-client-venv/bin/sp-claude"
harbor_sessions="${CLAUDE_CONFIG_DIR:-/logs/agent/sessions}"
pool_home="$HOME/.sub-pool/claude-home"

# Keep Harbor's session/trajectory locations visible through sp-claude's
# temporary CLAUDE_CONFIG_DIR. The OAuth credential itself stays private to
# sp-claude and is never copied into the job output.
mkdir -p "$pool_home"
for name in debug projects shell-snapshots statsig todos skills; do
    mkdir -p "$harbor_sessions/$name"
    if [ ! -e "$pool_home/$name" ] && [ ! -L "$pool_home/$name" ]; then
        ln -s "$harbor_sessions/$name" "$pool_home/$name"
    fi
done

# Harbor registers the task's MCP servers in its own .claude.json; sp-claude
# runs on the pool home's copy, so carry them over (Harbor's entries win).
if [ -f "$harbor_sessions/.claude.json" ]; then
    /opt/subpool-client-venv/bin/python -m sub_pool_client._harbor_common \
        merge-claude-mcp "$harbor_sessions/.claude.json" "$pool_home" || true
fi

exec "$sp_claude" --claude-bin "$direct" -- "$@"
"""

# `__DIRECT__` is replaced with the preserved codex binary at install time.
CODEX_WRAPPER = r"""#!/bin/sh
set -eu

direct="__DIRECT__"
sp_codex="/opt/subpool-client-venv/bin/sp-codex"
harbor_home="${CODEX_HOME:-}"
pool_home="$HOME/.sub-pool/codex-home"

# sp-codex points codex at its own temporary CODEX_HOME. Link in what Harbor
# put in its CODEX_HOME — the config (MCP servers, base URL), the skills — and
# the sessions dir Harbor copies the trajectory from when the run ends. The
# leased auth.json stays private to sp-codex.
if [ -n "$harbor_home" ]; then
    mkdir -p "$pool_home" "$harbor_home/sessions"
    for name in config.toml skills sessions; do
        if [ -e "$harbor_home/$name" ] && [ ! -e "$pool_home/$name" ] && [ ! -L "$pool_home/$name" ]; then
            ln -s "$harbor_home/$name" "$pool_home/$name"
        fi
    done
fi

exec "$sp_codex" --codex-bin "$direct" -- "$@"
"""


def merge_claude_mcp(src: Path, home: Path) -> None:
    """Merge `mcpServers` from Harbor's .claude.json into the pool home's,
    seeding the pool home first so onboarding state is still in place."""
    from sub_pool_client._cli_provider import CLAUDE
    home.mkdir(parents=True, exist_ok=True)
    if CLAUDE.init_home is not None:
        CLAUDE.init_home(home)
    try:
        theirs = json.loads(src.read_text()).get("mcpServers") or {}
    except (OSError, ValueError, AttributeError):
        return
    if not isinstance(theirs, dict) or not theirs:
        return
    dst = home / ".claude.json"
    try:
        ours = json.loads(dst.read_text()) if dst.exists() else {}
    except ValueError:
        ours = {}
    if not isinstance(ours, dict):
        ours = {}
    servers = ours.get("mcpServers") if isinstance(ours.get("mcpServers"), dict) else {}
    ours["mcpServers"] = {**servers, **theirs}
    tmp = dst.with_name(dst.name + ".tmp")
    tmp.write_text(json.dumps(ours, indent=2))
    os.replace(tmp, dst)


if __name__ == "__main__":  # pragma: no cover - exercised inside the sandbox
    import sys
    if len(sys.argv) == 4 and sys.argv[1] == "merge-claude-mcp":
        merge_claude_mcp(Path(sys.argv[2]), Path(sys.argv[3]))
    else:
        sys.exit("usage: python -m sub_pool_client._harbor_common "
                 "merge-claude-mcp SRC HOME")
