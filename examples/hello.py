"""Minimal end-to-end usage (credential-broker mode).

Before running:
  1. Install:    uv pip install -e . -e ./client
  2. Run pool:   docker compose up -d (from the sub-pool deployment dir)
  3. Log in an account via the admin UI + create an API key
  4. Run:        SUB_POOL_URL=http://localhost:8787 \
                 SUB_POOL_KEY=cp-... \
                 python examples/hello.py

The pool is a *credential broker*: it leases an OAuth access token to this
process, and the official `claude` CLI spawns locally. Tools run on this
box, files land in this directory, hooks fire here. Pool only sees the
lease request/release + any usage you choose to report back.

## Handling rate limits

If Anthropic returns 429 mid-agent-run, catch it and call
`client.report_error(code, message)` so the pool marks the account
COOLING. A follow-up `async with PooledClient(...)` picks another
account (if you have more than one) or sleeps until the cooldown
expires. See `_with_retry()` below for the pattern.
"""
import asyncio

from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKError
from sub_pool_client import PooledClient


async def _run_once(prompt: str) -> bool:
    """Returns True on success, False if the account is rate-limited
    (caller should retry)."""
    options = ClaudeAgentOptions(
        system_prompt="You are terse. Answer in one short sentence.",
    )
    async with PooledClient(options=options) as client:
        print(f"leased account={client.account} lease_id={client.lease_id}")
        try:
            await client.query(prompt)
            async for msg in client.receive_response():
                content = getattr(msg, "content", None)
                if content:
                    for block in content:
                        text = getattr(block, "text", None)
                        if text:
                            print(f"<< {text}")
        except ClaudeSDKError as e:
            msg_text = str(e).lower()
            if any(k in msg_text for k in ("429", "rate", "quota")):
                # Tell the pool so this account is cooled immediately —
                # then re-enter PooledClient on a different account.
                await client.report_error("RateLimit", str(e)[:500])
                return False
            raise
    return True


async def main() -> None:
    prompt = "List three prime numbers."
    for attempt in range(3):
        ok = await _run_once(prompt)
        if ok:
            return
        print(f"attempt {attempt + 1} rate-limited; retrying with next account")
        await asyncio.sleep(1)
    print("all attempts rate-limited — try again later or add more accounts")


if __name__ == "__main__":
    asyncio.run(main())
