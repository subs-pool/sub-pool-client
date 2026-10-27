"""sp-muse — run Meta's `muse` CLI under a credential leased from sub-pool.

    sp-muse                      # interactive TUI, equivalent to bare `muse`
    sp-muse exec "fix the bug"   # arguments pass straight through to muse
    sp-muse -- --help            # arguments after `--` go straight to muse

Setup / introspection:

    sp-muse --setup              # one-time wizard, writes ~/.sub-pool/cli.toml
    sp-muse --status             # print current pool URL + last-used account
    sp-muse --account NAME       # bind to a specific Muse account
    sp-muse --muse-bin /path     # override `muse` binary location
    sp-muse --verbose            # print lease / quota events to stderr

muse takes its key from `META_API_KEY`, fixed for the child's lifetime, so
the account swap the other wrappers do by rewriting a credential file is
done with a loopback proxy instead (`_api_proxy`): muse runs with
`--base-url http://127.0.0.1:<port>/v1` and a random per-session secret as
its key, and the proxy forwards each request with the CURRENT lease's real
api_key. When upstream answers 429 "Subscription quota exhausted", the proxy
reports it (the pool cools the account until the reset time), swaps to
another account and replays the request — muse never sees the 429 unless no
account is left. The health watcher's swaps take effect the same way.

Pass your own `--base-url` and none of that happens: the real key goes in
`META_API_KEY`, the session stays on one account, and quota 429s are only
picked up from stderr on non-interactive runs. Meta's login token never
leaves the pool either way.
"""
from __future__ import annotations

import asyncio
import sys

import click

from sub_pool_client._cli_core import run_session, run_setup, run_status
from sub_pool_client._cli_provider import MUSE


@click.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    help="Run `muse` CLI with credentials leased from a sub-pool server.",
)
@click.argument("muse_args", nargs=-1, type=click.UNPROCESSED)
@click.option("--setup", is_flag=True, help="One-time pool config wizard.")
@click.option("--status", is_flag=True, help="Show current config + exit.")
@click.option("--account", default=None,
              help="Bind to a specific Muse account, skipping round-robin "
                   "order. Subject to the api key's strategy eligibility.")
@click.option("--muse-bin", default=None,
              help="Override path to `muse` binary.")
@click.option("--verbose", is_flag=True,
              help="Print lease / quota events to stderr.")
def main(muse_args, setup, status, account, muse_bin, verbose) -> None:
    if setup:
        sys.exit(run_setup(MUSE))
    if status:
        sys.exit(run_status(MUSE))
    code = asyncio.run(run_session(
        MUSE, muse_args, account, muse_bin, verbose,
    ))
    sys.exit(code)


if __name__ == "__main__":
    main()
