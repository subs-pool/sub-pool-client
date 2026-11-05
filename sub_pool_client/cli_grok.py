"""sp-grok — run the `grok` CLI under a credential leased from sub-pool.

    sp-grok                      # interactive TUI, equivalent to bare `grok`
    sp-grok -- --help            # arguments after `--` go straight to grok

Setup / introspection:

    sp-grok --setup              # one-time wizard, writes ~/.sub-pool/cli.toml
    sp-grok --status             # print current pool URL + last-used account
    sp-grok --account NAME       # bind to a specific Grok account
    sp-grok --grok-bin /path     # override `grok` binary location
    sp-grok --verbose            # print swap / refresh events to stderr

While the session is alive `$GROK_HOME/auth.json` is kept current — token
rotations and mid-session account swaps happen transparently in the
background, so the leased account stays in rotation across long runs.

Grok Build refreshes proactively ~5 min before expiry
(`GROK_AUTH_EARLY_INVALIDATION_SECS`), and the copy it holds carries no
refresh_token — the pool owns that chain. The client's refresh loop is
anchored to expiry and rewrites auth.json well before that window, so grok
never reaches the point of asking for an interactive login.
"""
from __future__ import annotations

import asyncio
import sys

import click

from sub_pool_client._cli_core import (
    run_add_key, run_session, run_setup, run_status, run_whoami,
)
from sub_pool_client._cli_provider import GROK


@click.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    help="Run `grok` CLI with credentials leased from a sub-pool server.",
)
@click.argument("grok_args", nargs=-1, type=click.UNPROCESSED)
@click.option("--setup", is_flag=True, help="One-time pool config wizard.")
@click.option("--status", is_flag=True, help="Show current config + exit.")
@click.option("--key", "key_name", default=None, metavar="NAME",
              help="Use the API key saved under NAME in cli.toml (see --add-key).")
@click.option("--whoami", is_flag=True,
              help="Show each configured key's sub-pool and whether it can lease from here.")
@click.option("--add-key", default=None, metavar="NAME",
              help="Save another API key under NAME (e.g. one per sub-pool).")
@click.option("--account", default=None,
              help="Bind to a specific Grok account, skipping round-robin "
                   "order. Subject to the api key's strategy eligibility.")
@click.option("--grok-bin", default=None,
              help="Override path to `grok` binary.")
@click.option("--verbose", is_flag=True,
              help="Print swap / refresh events to stderr.")
def main(grok_args, setup, status, key_name, whoami, add_key, account, grok_bin, verbose) -> None:
    if setup:
        sys.exit(run_setup(GROK))
    if status:
        sys.exit(run_status(GROK, key_name))
    if whoami:
        sys.exit(run_whoami(GROK, key_name))
    if add_key:
        sys.exit(run_add_key(GROK, add_key))
    code = asyncio.run(run_session(
        GROK, grok_args, account, grok_bin, verbose, key_name,
    ))
    sys.exit(code)


if __name__ == "__main__":
    main()
