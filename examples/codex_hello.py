"""Minimal codex usage through the pool.

Before running:
  1. Install:    uv pip install -e .
  2. Have a running sub-pool server with at least one codex account
     connected via the admin UI (provider: codex).
  3. Create an API key on the same server.
  4. Have the official `codex` CLI on PATH.
  5. Run:        SUB_POOL_URL=http://localhost:8787 \
                 SUB_POOL_KEY=cp-... \
                 python examples/codex_hello.py

The pool leases an auth.json onto your machine, the local codex CLI
runs against it, and the pool rotates the access_token transparently
through a background poll. The refresh_token never leaves the pool.
"""
import asyncio

from sub_pool_client import PooledCodexClient


async def main() -> None:
    async with PooledCodexClient() as codex:
        print(f"leased account={codex.account} lease_id={codex.lease_id}")
        print(f"CODEX_HOME={codex.env['CODEX_HOME']}")

        proc = await codex.exec(
            "Print the string 'hello, codex' and nothing else.",
            "--cd", ".",
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            err = stderr.decode(errors="replace")
            print(f"codex exit={proc.returncode}: {err[:400]}")
            # Tell the pool if it looks like an upstream rate-limit so
            # the account gets cooled and the next lease skips it.
            if "rate" in err.lower() or "429" in err:
                await codex.report_error("RateLimit", err[:500])
            return

        print(stdout.decode())


if __name__ == "__main__":
    asyncio.run(main())
