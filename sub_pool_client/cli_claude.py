"""sp-claude — run the `claude` CLI under a credential leased from sub-pool.

    sp-claude                    # interactive REPL, equivalent to bare `claude`
    sp-claude "prompt"           # one-shot
    sp-claude -- /resume         # arguments after `--` go straight to claude

Setup / introspection:

    sp-claude --setup            # one-time wizard, writes ~/.sub-pool/cli.toml
    sp-claude --status           # print current pool URL + last-used account
    sp-claude --account NAME     # bind to a specific pool account
    sp-claude --claude-bin /path # override `claude` binary location
    sp-claude --verbose          # print swap / refresh events to stderr

While the session is alive the credentials file in `CLAUDE_CONFIG_DIR`
is kept current — token rotations and mid-session account swaps
happen transparently in the background.
"""
from __future__ import annotations

import asyncio
import sys

import click

from sub_pool_client._cli_core import run_session, run_setup, run_status
from sub_pool_client._cli_provider import CLAUDE


@click.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    help="Run `claude` CLI with credentials leased from a sub-pool server.",
)
@click.argument("claude_args", nargs=-1, type=click.UNPROCESSED)
@click.option("--setup", is_flag=True, help="One-time pool config wizard.")
@click.option("--status", is_flag=True, help="Show current config + exit.")
@click.option("--account", default=None,
              help="Bind to a specific account, skipping round-robin order. "
                   "Subject to the api key's strategy eligibility.")
@click.option("--claude-bin", default=None,
              help="Override path to `claude` binary.")
@click.option("--verbose", is_flag=True,
              help="Print swap / refresh events to stderr.")
def main(claude_args, setup, status, account, claude_bin, verbose) -> None:
    if setup:
        sys.exit(run_setup(CLAUDE))
    if status:
        sys.exit(run_status(CLAUDE))
    code = asyncio.run(run_session(
        CLAUDE, claude_args, account, claude_bin, verbose,
    ))
    sys.exit(code)


if __name__ == "__main__":
    main()
