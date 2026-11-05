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

Unrecognised arguments pass straight through to `claude`, so anything
the upstream CLI accepts works here unchanged. One of them is also
*read* on the way past: `--model` is forwarded to the pool as the
lease's `required_model`, so accounts whose weekly sub-cap for that
model is already spent get skipped at pick time instead of handing you
an immediate 429. The flag still reaches `claude` untouched.

That read is conservative, not a second argument parser: where the
model can't be determined without knowing claude's option table — after
a `--`, or when `--model` might be the value of the flag before it —
nothing is declared and the lease goes unfiltered, which is simply the
behaviour from before this existed.

While the session is alive the credentials file in `CLAUDE_CONFIG_DIR`
is kept current — token rotations and mid-session account swaps
happen transparently in the background.

When it ends, the lease release reports what the session consumed:
from claude's result lines when stdout is `--output-format json` /
`stream-json`, otherwise from the session's own transcripts (see
`_usage`).
"""
from __future__ import annotations

import asyncio
import sys

import click

from sub_pool_client._cli_core import (
    run_add_key, run_session, run_setup, run_status, run_whoami,
)
from sub_pool_client._cli_provider import CLAUDE


@click.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    help="Run `claude` CLI with credentials leased from a sub-pool server.",
)
@click.argument("claude_args", nargs=-1, type=click.UNPROCESSED)
@click.option("--setup", is_flag=True, help="One-time pool config wizard.")
@click.option("--status", is_flag=True, help="Show current config + exit.")
@click.option("--key", "key_name", default=None, metavar="NAME",
              help="Use the API key saved under NAME in cli.toml (see --add-key).")
@click.option("--whoami", is_flag=True,
              help="Show each configured key's sub-pool and whether it can lease from here.")
@click.option("--add-key", default=None, metavar="NAME",
              help="Save another API key under NAME (e.g. one per sub-pool).")
@click.option("--account", default=None,
              help="Bind to a specific account, skipping round-robin order. "
                   "Subject to the api key's strategy eligibility.")
@click.option("--claude-bin", default=None,
              help="Override path to `claude` binary.")
@click.option("--verbose", is_flag=True,
              help="Print swap / refresh events to stderr.")
def main(claude_args, setup, status, key_name, whoami, add_key, account, claude_bin, verbose) -> None:
    if setup:
        sys.exit(run_setup(CLAUDE))
    if status:
        sys.exit(run_status(CLAUDE, key_name))
    if whoami:
        sys.exit(run_whoami(CLAUDE, key_name))
    if add_key:
        sys.exit(run_add_key(CLAUDE, add_key))
    code = asyncio.run(run_session(
        CLAUDE, claude_args, account, claude_bin, verbose, key_name,
    ))
    sys.exit(code)


if __name__ == "__main__":
    main()
