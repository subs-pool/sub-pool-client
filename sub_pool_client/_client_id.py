"""Who is calling: every request to the pool carries `X-Sub-Pool-Client:
<kind>/<version>` (e.g. `sp-claude/0.12.0`, `sdk-codex/0.12.0`). The pool
records it per lease and can refuse new leases from clients older than its
minimum (server: sub_pool/client_policy.py). Clients before 0.12.0 send none.
"""
from __future__ import annotations

HEADER = "X-Sub-Pool-Client"


def client_version() -> str:
    try:
        from sub_pool_client._version import __version__
        return __version__
    except ImportError:   # not built by hatch-vcs (e.g. a bare checkout)
        pass
    try:
        from importlib.metadata import version
        return version("sub-pool-client")
    except Exception:
        return "0.0.0"


def client_headers(kind: str) -> dict[str, str]:
    return {HEADER: f"{kind}/{client_version()}"}
