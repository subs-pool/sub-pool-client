"""sp-codex — run the `codex` CLI under a credential leased from sub-pool.

    sp-codex                     # interactive REPL, equivalent to bare `codex`
    sp-codex exec "prompt"       # one-shot; args after the binary are
                                 # passed through unchanged
    sp-codex -- /help            # arguments after `--` go straight to codex

Setup / introspection:

    sp-codex --setup             # one-time wizard, writes ~/.sub-pool/cli.toml
    sp-codex --status            # print current pool URL + last-used account
    sp-codex --account NAME      # bind to a specific Codex account
    sp-codex --codex-bin /path   # override `codex` binary location
    sp-codex --verbose           # print swap / refresh events to stderr

While the session is alive `$CODEX_HOME/auth.json` is kept current —
token rotations and mid-session account swaps happen transparently in
the background, so the leased account stays in rotation across long
runs.
"""
from __future__ import annotations

import asyncio
import sys

import click

from sub_pool_client._cli_core import run_session, run_setup, run_status
from sub_pool_client._cli_provider import CODEX


@click.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    help="Run `codex` CLI with credentials leased from a sub-pool server.",
)
@click.argument("codex_args", nargs=-1, type=click.UNPROCESSED)
@click.option("--setup", is_flag=True, help="One-time pool config wizard.")
@click.option("--status", is_flag=True, help="Show current config + exit.")
@click.option("--account", default=None,
              help="Bind to a specific Codex account, skipping round-robin "
                   "order. Subject to the api key's strategy eligibility.")
@click.option("--codex-bin", default=None,
              help="Override path to `codex` binary.")
@click.option("--verbose", is_flag=True,
              help="Print swap / refresh events to stderr.")
def main(codex_args, setup, status, account, codex_bin, verbose) -> None:
    if setup:
        sys.exit(run_setup(CODEX))
    if status:
        sys.exit(run_status(CODEX))
    code = asyncio.run(run_session(
        CODEX, codex_args, account, codex_bin, verbose,
    ))
    sys.exit(code)


if __name__ == "__main__":
    main()
