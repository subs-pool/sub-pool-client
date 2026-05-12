# sub-pool-client

Python client for a sub-pool server — a credential broker for N Claude
subscription accounts. This package is a drop-in subclass of
`ClaudeSDKClient` from the official `claude-agent-sdk` that leases
short-lived OAuth access tokens from a pool server, writes them into a
`CLAUDE_CONFIG_DIR`, and lets the local `claude` CLI run normally.

The agent loop — tool execution, filesystem I/O, hooks, MCP servers — all
stays on the consumer's machine. The pool only sees lease lifecycles.

## Install

```bash
pip install git+https://github.com/subs-pool/sub-pool-client
# or
uv pip install git+https://github.com/subs-pool/sub-pool-client
```

## Usage

```python
import asyncio
from claude_agent_sdk import ClaudeAgentOptions
from sub_pool_client import PooledClient

async def main():
    options = ClaudeAgentOptions(
        system_prompt="You are a terse assistant.",
    )
    async with PooledClient(options=options) as client:
        print(f"leased account={client.account} lease_id={client.lease_id}")
        await client.query("List three prime numbers.")
        async for msg in client.receive_response():
            usage = getattr(msg, "usage", None)
            if isinstance(usage, dict):
                client.report_usage(
                    input_tokens=int(usage.get("input_tokens") or 0),
                    output_tokens=int(usage.get("output_tokens") or 0),
                )
            print(type(msg).__name__, "->", msg)

asyncio.run(main())
```

```bash
SUB_POOL_URL=http://localhost:8787 \
SUB_POOL_KEY=cp-...from-admin-ui... \
python examples/hello.py
```

`PooledClient` is a subclass of `ClaudeSDKClient`. Every option the SDK
accepts (hooks, MCP servers, `cwd`, `allowed_tools`, `permission_mode`)
works unchanged — the pool never sees them.

### Multi-turn sessions

`PooledClient.__aenter__` leases once and a background task polls the pool
for a fresh access_token ~10 min before expiry, atomic-rewriting
`.credentials.json`. Long sessions work without code changes:

```python
async with PooledClient(options=options) as client:
    for prompt in ["first", "second", "third"]:
        await client.query(prompt)
        async for msg in client.receive_response():
            ...
```

Prompt-cache hits are up to the official SDK — it's running locally so
Anthropic sees the same `session_id` across turns.

### Handling rate limits

If Anthropic returns 429 mid-run, catch it and tell the pool so the
account gets cooled — the next lease skips it:

```python
from claude_agent_sdk import ClaudeSDKError

try:
    async with PooledClient(options=options) as client:
        await client.query("...")
        async for msg in client.receive_response():
            ...
except ClaudeSDKError as e:
    if "429" in str(e) or "rate" in str(e).lower():
        # For pro-active mid-run reporting, call client.report_error()
        # BEFORE the exception unwinds the context manager.
        pass
```

See `examples/hello.py` for a retry loop that routes to a different
account on rate limit.

### Concurrent usage

Multiple `async with PooledClient()` in the same process with identical
routing (same `user_id` / `required_model`) share a single lease. The
CLI subprocesses all read the same `.credentials.json` and coordinate
refresh via the pool, so N parallel agents on one account "just work"
— up to whatever Anthropic's concurrency limit is on that account.

Want true parallelism across accounts? Use different `user_id`s (with
the `sticky_user` strategy) or different API keys.

## Configuration

| Env var | Required | Notes |
|---|---|---|
| `SUB_POOL_URL` | ✅* | Pool base URL (`http://host:8787`) |
| `SUB_POOL_KEY` | ✅* | API key bearer |
| `SUB_POOL_CLIENT_DIR` | — | Override `~/.sub-pool-client/` shared-dir root |

`*` can also be passed as kwargs: `PooledClient(pool_url=..., api_key=...)`.

## Requires

- Python ≥ 3.12
- `claude-agent-sdk` ≥ 0.1.63
- A running sub-pool server

## License

Not yet specified.
