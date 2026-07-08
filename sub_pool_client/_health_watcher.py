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
        # even after a swap rotates it (the CLI updates the captured lease
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
                if e.response.status_code in (404, 410):
                    # Our CURRENT lease died server-side while the session is
                    # still live: reaped after a health-poll gap (e.g. the host
                    # slept past LEASE_TTL_S), externally released, or the
                    # account deleted. Giving up here is what left a long
                    # session 401ing forever with no recovery. Instead recover
                    # through the same swap handler — it re-leases a fresh
                    # account (releasing the dead one is best-effort) — then
                    # keep watching the new lease. `lease_gone` tells the
                    # handler this is a re-lease, so a same-account replacement
                    # (new lease_id) is still adopted rather than discarded as
                    # "nothing to swap to".
                    #
                    # Operator note: because of this, a bare DELETE of the lease
                    # no longer evicts a live client — it re-leases within one
                    # tick (possibly onto the same account). To take an account
                    # out from under a running session, DISABLE the account
                    # (re-lease then 503s and the client quietly waits it out) or
                    # revoke its api-key.
                    try:
                        await self._on_unhealthy({
                            "healthy": False,
                            "state": "gone",
                            "lease_gone": True,
                            "reason": f"lease gone ({e.response.status_code}); "
                                      f"re-leasing",
                        })
                    except asyncio.CancelledError:
                        return
                    except Exception as ex:  # noqa: BLE001
                        log.warning("re-lease after %s raised: %s",
                                    e.response.status_code, ex)
                    continue
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
