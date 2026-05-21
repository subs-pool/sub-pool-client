"""Mid-session account swap.

When the health watcher reports the current lease's account has cooled,
this module rotates the on-disk credential to a different account:

  1. POST /credentials/lease (pool strategy picks an AVAILABLE account,
     not the one we're trying to swap away from — because the strategy
     already skips COOLING accounts).
  2. Atomically update the caller's `current` container AND
     .credentials.json (sync block, no await between).
  3. Best-effort DELETE the old lease.

The order in step 2 is load-bearing — see the race-safety contract in
`swap_credentials` below.

The `claude` CLI re-reads .credentials.json across requests (empirically
verified: a /login in one window flips the running `claude` in another
window to the newly-written identity, observable via `/cost`), so the
swap is transparent to the user — next prompt round-trips on the new
account.

If every account in the pool is currently cooling, the strategy returns
503. We raise `NoSwapAvailable`; the watcher swallows it and retries on
the next tick.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from sub_pool_client._cli_core import PoolHTTP
    from sub_pool_client._cli_provider import CliProvider

log = logging.getLogger(__name__)


class NoSwapAvailable(Exception):
    """Pool has no other healthy account to switch to right now."""


async def swap_credentials(
    pool: "PoolHTTP", current: dict, session_dir: Path,
    provider: "CliProvider",
    *, account: str | None = None,
) -> dict:
    """Replace the lease backing `session_dir` with a fresh one.

    `current` is the same mutable container the refresh / health tasks
    read out of (`{"lease": <lease dict>}`); we mutate it in place so
    those tasks pick up the new lease_id without any restart plumbing.

    Race-safety contract — this is why `current` is a parameter rather
    than something the caller assigns after we return:

      The token-refresh task captures `current["lease"]["lease_id"]`
      pre-call, awaits `pool.refresh_token`, then re-checks the lease_id
      to detect a concurrent swap (`_refresh_once`). For that guard to
      catch us, `current["lease"]` must be advanced to the new lease
      BEFORE any further `await` in this function — otherwise refresh
      can wake up while we're suspended on `pool.release`, still see the
      old lease_id, and clobber `.credentials.json` with a stale token.

      So: update `current` + disk inside a sync block, THEN await release.

    Returns the new lease dict (also stored at `current["lease"]`).
    """
    old_lease_id = current["lease"]["lease_id"]
    try:
        new_lease = await pool.lease(provider, account=account)
    except httpx.HTTPStatusError as e:
        # 503 = strategy says no candidate (all cooling / invalid).
        # Anything else: surface as no-swap-available so the watcher
        # backs off; we don't want to nuke the existing lease over a
        # transient pool blip.
        raise NoSwapAvailable(
            f"pool returned {e.response.status_code}: "
            f"{e.response.text[:120]}"
        ) from e
    except httpx.HTTPError as e:
        raise NoSwapAvailable(f"pool unreachable: {e}") from e

    if new_lease.get("lease_id") == old_lease_id:
        # Strategy handed back the same lease — happens if the CLI passes
        # `account=` and that account is still the only candidate. Not a
        # real swap; treat as nothing-to-do so we don't churn.
        raise NoSwapAvailable("strategy returned the same lease_id")

    # === Sync block — no await between disk write and current update.
    # Write disk first so a `write_credentials` exception leaves the
    # caller's view (`current`) consistent with what's actually on disk.
    provider.write_credentials(session_dir / provider.cred_filename, new_lease)
    current["lease"] = new_lease
    # === End sync block.

    # Best-effort cleanup of the now-cooled lease. Even if release fails
    # the swap has already taken effect.
    await pool.release(old_lease_id)
    log.info("swapped lease %s → %s (account %s)",
             old_lease_id, new_lease["lease_id"], new_lease.get("account"))
    return new_lease
