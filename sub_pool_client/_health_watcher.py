"""Background health watcher for an active lease.

Polls `/credentials/lease/{id}/health` on a slow cadence and invokes
`on_unhealthy` when the lease's underlying account stops being healthy
(typically: usage_refresh_loop just marked it COOLING because the plan
window hit ≥95%). The handler is expected to perform an account swap
(see `_swap.swap_credentials`).

Transient failures (network blips, 5xx) are swallowed and retried on the
next tick — the watcher's job is to be quietly reliable, not noisy.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

import httpx

log = logging.getLogger(__name__)


class HealthWatcher:
    def __init__(
        self,
        *,
        fetch: Callable[[], Awaitable[dict[str, Any]]],
        on_unhealthy: Callable[[dict[str, Any]], Awaitable[None]],
        interval_s: float = 30.0,
    ):
        # `fetch` is a no-argument coroutine that returns the health dict
        # — passed as a closure so it captures the *current* lease id
        # even after a swap rotates it (cpool updates the captured lease
        # in place via a small mutable container).
        self._fetch = fetch
        self._on_unhealthy = on_unhealthy
        self._interval_s = interval_s

    async def run(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._interval_s)
            except asyncio.CancelledError:
                return
            try:
                data = await self._fetch()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 410:
                    # Lease ended out from under us (someone called
                    # DELETE, or a prior swap obsoleted us). Stop.
                    return
                if e.response.status_code == 404:
                    # Wrong key / unknown lease — same disposition.
                    return
                # Other 4xx/5xx — log and keep going.
                log.debug("health probe http error: %s", e)
                continue
            except (httpx.HTTPError, asyncio.TimeoutError) as e:
                log.debug("health probe network error: %s", e)
                continue
            except Exception as e:  # noqa: BLE001
                log.debug("health probe unexpected: %s", e)
                continue

            if data.get("healthy"):
                continue

            try:
                await self._on_unhealthy(data)
            except asyncio.CancelledError:
                return
            except Exception as e:  # noqa: BLE001
                log.warning("on_unhealthy handler raised: %s", e)
