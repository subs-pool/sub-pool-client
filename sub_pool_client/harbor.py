"""Harbor agents that run Claude Code / Codex on a live sub-pool lease.

    harbor run -d <dataset> \\
        --agent-import-path sub_pool_client.harbor:SubpoolClaudeCode \\
        --ak subpool_key=home            # optional: a named key from cli.toml

Harbor's own Claude Code / Codex agents take a static token; sub-pool tokens
are short-lived. These install `sub-pool-client` inside the trial sandbox and
put a wrapper in front of the native binary, so `sp-claude` / `sp-codex` own
the lease, token refreshes and account swaps for the agent's whole run.

The pool URL and key reach the sandbox through Harbor's agent env (`--ae`),
which Harbor redacts in its logs; see `_harbor_common.pool_env` for where they
come from when not passed. Requires the `harbor` package (you run Harbor
anyway); `import sub_pool_client` itself never needs it.

Agent kwargs (`--ak name=value`):
    subpool_key             a key name from the host's ~/.sub-pool/cli.toml
    subpool_client_version  sub-pool-client to install in the sandbox:
                            default = this client's version, or "latest"
"""
from __future__ import annotations

import shlex
from typing import Any

try:
    from harbor.agents.installed.claude_code import ClaudeCode
    from harbor.agents.installed.codex import Codex
    from harbor.environments.base import BaseEnvironment
except ImportError as e:  # pragma: no cover - exercised only without harbor
    raise ImportError(
        "sub_pool_client.harbor needs the `harbor` package: pip install harbor"
    ) from e

from sub_pool_client._harbor_common import (
    CLAUDE_WRAPPER,
    VENV,
    client_requirement,
    install_command,
    pool_env,
)

__all__ = ["SubpoolClaudeCode", "SubpoolCodex"]


def _pool_kwargs(kwargs: dict[str, Any]) -> tuple[dict[str, str], str]:
    """Pop our own kwargs; return (extra_env with the pool credentials, the
    sub-pool-client requirement for the sandbox)."""
    key_name = kwargs.pop("subpool_key", None)
    version = kwargs.pop("subpool_client_version", None)
    env = pool_env(kwargs.pop("extra_env", None), key_name)
    return env, client_requirement(version)


_DIRECT_CLAUDE = "$HOME/.local/bin/claude-harbor-direct"
_WRAPPED_CLAUDE = "$HOME/.local/bin/claude"


class SubpoolClaudeCode(ClaudeCode):
    """Claude Code backed by a live sub-pool subscription-account lease."""

    _INSTALL_VERSION_COMMAND = (
        'export PATH="$HOME/.local/bin:$PATH"; '
        f"if test -x {_DIRECT_CLAUDE}; then {_DIRECT_CLAUDE} --version; "
        "else claude --version; fi"
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        env, self._subpool_requirement = _pool_kwargs(kwargs)
        super().__init__(*args, extra_env=env, **kwargs)

    @staticmethod
    def name() -> str:
        return "subpool-claude-code"

    def get_version_command(self) -> str | None:
        return self._INSTALL_VERSION_COMMAND

    async def install(self, environment: BaseEnvironment) -> None:
        # Claude Code itself is installed exactly as Harbor's own agent does.
        await super().install(environment)
        await self.exec_as_root(environment, command=install_command(self._subpool_requirement))
        # Keep the native binary once, then put the lease wrapper at the path
        # ClaudeCode.run() invokes.
        await self.exec_as_agent(environment, command=(
            "set -euo pipefail; "
            'mkdir -p "$HOME/.local/bin"; '
            'export PATH="$HOME/.local/bin:$PATH"; '
            # Harbor's install is a no-op when the image already ships Claude
            # Code, so resolve the binary actually on PATH. This runs before
            # the wrapper exists, so `command -v claude` can't be the wrapper.
            f"if [ ! -e {_DIRECT_CLAUDE} ]; then "
            'real="$(command -v claude || true)"; '
            'if [ -z "$real" ]; then echo "claude binary not found on PATH" >&2; exit 127; fi; '
            f'if [ "$real" = "{_WRAPPED_CLAUDE}" ]; then mv {_WRAPPED_CLAUDE} {_DIRECT_CLAUDE}; '
            # Image-provided binary: link, so absolute-path callers still work.
            f'else ln -s "$real" {_DIRECT_CLAUDE}; fi; '
            "fi; "
            f'"{VENV}/bin/python" -c \'import pathlib, sys; '
            'p = pathlib.Path.home() / ".local/bin/claude"; '
            "p.write_text(sys.argv[1]); p.chmod(0o755)' "
            f"{shlex.quote(CLAUDE_WRAPPER)}; "
            f"{_DIRECT_CLAUDE} --version; "
            f'"{VENV}/bin/sp-claude" --help >/dev/null'
        ))


# The preserved codex stays next to the original: it is a Node ESM launcher
# that finds its native payload relative to its own directory.
_DIRECT_CODEX = "codex-harbor-direct"

# Resolve codex the way Codex.run() does (through nvm), rename it in place
# once, then put the wrapper at every path that could win: the nvm-resolved one
# and the /usr/local/bin symlink Harbor creates.
_CODEX_SWAP = (
    "set -euo pipefail; "
    "if [ -s ~/.nvm/nvm.sh ]; then . ~/.nvm/nvm.sh; fi; "
    'real="$(command -v codex || true)"; '
    'if [ -z "$real" ]; then echo "codex binary not found on PATH" >&2; exit 127; fi; '
    'resolved="$(readlink -f "$real")"; '
    'realdir="$(dirname "$resolved")"; '
    f'direct="$realdir/{_DIRECT_CODEX}"; '
    'if [ ! -e "$direct" ]; then mv "$resolved" "$direct"; chmod 755 "$direct"; fi; '
    'for target in "$real" /usr/local/bin/codex; do '
    '[ -n "$target" ] || continue; '
    # Not `[ … ] && continue`: under set -e that AND-list fails the script.
    'if [ "$target" = "$direct" ]; then continue; fi; '
    'rm -f "$target"; '
    f"printf '#!/bin/sh\\nset -eu\\nexec {VENV}/bin/sp-codex "
    '--codex-bin %s -- "$@"\\n\' "$direct" > "$target"; '
    'chmod 755 "$target"; '
    "done; "
    '"$direct" --version; '
    f'"{VENV}/bin/sp-codex" --help >/dev/null'
)


class SubpoolCodex(Codex):
    """Codex backed by a live sub-pool subscription-account lease."""

    _INSTALL_VERSION_COMMAND = (
        "if [ -s ~/.nvm/nvm.sh ]; then . ~/.nvm/nvm.sh; fi; codex --version"
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        env, self._subpool_requirement = _pool_kwargs(kwargs)
        super().__init__(*args, extra_env=env, **kwargs)

    @staticmethod
    def name() -> str:
        return "subpool-codex"

    def get_version_command(self) -> str | None:
        return self._INSTALL_VERSION_COMMAND

    async def install(self, environment: BaseEnvironment) -> None:
        await super().install(environment)
        await self.exec_as_root(environment, command=install_command(self._subpool_requirement))
        await self.exec_as_root(environment, command=_CODEX_SWAP)
