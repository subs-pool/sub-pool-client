"""Exception hierarchy for PooledClient.

Rooted at `ClaudeSDKError` so that production code using
`except ClaudeSDKError:` catches pool-side failures the same way it catches
native SDK failures.
"""
from __future__ import annotations

try:
    from claude_agent_sdk import ClaudeSDKError  # type: ignore
except ImportError:  # pragma: no cover
    class ClaudeSDKError(Exception):  # type: ignore
        pass


class PoolError(ClaudeSDKError):
    """Base class for all pool-side failures surfaced to the client."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class PoolConnectionError(PoolError):
    """Network-level failure talking to the pool server."""


class PoolAuthError(PoolError):
    """401/403 from the pool server."""


class PoolAcquireTimeoutError(PoolError):
    """Pool could not find a suitable account within the acquire timeout."""


class PoolUpstreamError(PoolError):
    """A session-level error reported by the pool (usually from the SDK on the worker).

    Typically these originate from Anthropic — rate limits, quota, OAuth
    expired, etc. `code` is the worker's exception class name (e.g.
    ClaudeSDKError, ProcessError) and `message` is its str().
    """


class PoolProtocolError(PoolError):
    """Pool returned an event we could not interpret."""
