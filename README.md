# sub-pool-client

Python client for a [sub-pool](https://github.com/the-upstream-org/sub-pool)
server — a credential broker for N Claude **and** OpenAI Codex
subscription accounts. This package gives you three things:

1. **`PooledClient`** — a drop-in subclass of `ClaudeSDKClient` from the
   official `claude-agent-sdk`. Leases short-lived OAuth access tokens
   from the pool, writes them into a `CLAUDE_CONFIG_DIR`, and lets the
   local `claude` CLI run normally.
2. **`PooledCodexClient`** — equivalent helper for OpenAI Codex. Leases
   a Codex `auth.json`, writes it into an isolated `CODEX_HOME`, and
   lets you spawn `codex exec` locally. Backed by the same pool, with
   the pool owning the OAuth refresh chain so concurrent leases on one
   ChatGPT account don't race-rotate each other.
3. **`cpool` CLI** — a `claude`-binary wrapper: `cpool` runs the real
   `claude` CLI with credentials transparently leased + auto-refreshed
   from the pool. Use it as a drop-in `claude` replacement in shells
   and scripts.

The agent loop — tool execution, filesystem I/O, hooks, MCP servers,
the codex subprocess — all stays on the consumer's machine. The pool
only sees lease lifecycles.

## Install

```bash
pip install git+https://github.com/subs-pool/sub-pool-client
# or
uv pip install git+https://github.com/subs-pool/sub-pool-client
```

## Environment

Both flows need:

```bash
export SUB_POOL_URL=http://your-pool-host:8787
export SUB_POOL_KEY=cp-...   # admin gives you an API key from the UI
```

`SUB_POOL_KEY`'s strategy decides which accounts you can reach.

## 1. Claude

### `cpool` CLI (drop-in `claude` wrapper)

```bash
cpool --setup            # one-time interactive config wizard
cpool                    # start the claude REPL on a leased account
cpool -p "explain this"  # single-prompt mode
cpool --account claude-pro   # pin to a specific account (subject to strategy)
cpool --status           # show effective config and exit
```

`cpool` foregrounds the real `claude` binary so SDK options
(`--mcp-config`, `--allowed-tools`, `--cwd`, etc.) pass straight through.

### `PooledClient` (Python SDK)

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
python examples/hello.py
```

`PooledClient` is a true subclass of `ClaudeSDKClient`. Every option the
SDK accepts (hooks, MCP servers, `cwd`, `allowed_tools`,
`permission_mode`) works unchanged — the pool never sees them.

### Multi-turn sessions

`__aenter__` leases once and a background task polls the pool for a
fresh access_token ~10 min before expiry, atomic-rewriting
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

### Reporting rate limits / errors

If Anthropic returns 429 mid-run, tell the pool so the account gets
cooled and the next lease skips it:

```python
async with PooledClient(options=options) as client:
    try:
        await client.query("...")
        async for msg in client.receive_response():
            ...
    except Exception as e:
        if "429" in str(e) or "rate" in str(e).lower():
            await client.report_error("RateLimit", str(e)[:500])
        raise
```

See `examples/hello.py` for a retry loop that routes to a different
account on rate limit.

## 2. Codex

```python
import asyncio
from sub_pool_client import PooledCodexClient

async def main():
    async with PooledCodexClient(
        user_id="alice",   # optional
    ) as codex:
        print(f"leased {codex.account} lease_id={codex.lease_id}")

        # codex.exec spawns `codex exec <prompt>` with CODEX_HOME pointing
        # at the leased auth.json. Returns an asyncio subprocess; caller
        # waits / streams as needed.
        proc = await codex.exec(
            "summarize this repository",
            "--cd", ".",
            model="gpt-5.5",
            json_output=True,
        )
        stdout, stderr = await proc.communicate()
        print(stdout.decode())

        if proc.returncode != 0 and b"rate" in stderr.lower():
            await codex.report_error("RateLimit", stderr.decode()[:500])

asyncio.run(main())
```

You can also run the `codex` CLI manually using the prepared environment:

```python
import subprocess
async with PooledCodexClient() as codex:
    subprocess.run(
        ["codex", "exec", "hello"],
        env=codex.env,   # has CODEX_HOME pointing at the leased auth.json
    )
```

**Pool owns the refresh chain**: the leased `auth.json` carries a fresh
access_token but a blanked `refresh_token`. A background poll task asks
the pool for a refreshed access_token before expiry and rewrites the
local `auth.json` atomically — codex CLI picks it up on its next call.
Concurrent leases on the same Codex account are safe.

## Concurrent usage (same process)

Multiple `async with PooledClient()` (or `PooledCodexClient()`) in the
same process with identical routing (`pool_url`, `api_key`, `user_id`,
`required_model`) share a single lease. The local CLI subprocesses all
read the same on-disk credentials and coordinate refresh via the pool,
so N parallel agents on one account "just work" up to whatever the
upstream provider's concurrency limit is on that account.

Want true parallelism across accounts? Use different `user_id`s with
the `sticky_user` strategy or use different API keys.

## Configuration

| Env var | Required | Notes |
|---|---|---|
| `SUB_POOL_URL` | ✅* | Pool base URL (`http://host:8787`) |
| `SUB_POOL_KEY` | ✅* | API key bearer |
| `SUB_POOL_CLIENT_DIR` | — | Override `~/.sub-pool-client/` shared-dir root |

`*` can also be passed as kwargs: `PooledClient(pool_url=..., api_key=...)`.

`cpool --setup` writes these to `~/.config/sub-pool/cpool.toml` and
re-reads them on every invocation, so you don't need them in your
shell after first-time setup.

## Public surface

```python
from sub_pool_client import (
    PooledClient,          # claude SDK wrapper
    PooledCodexClient,     # codex helper
    PoolError,             # base exception
    PoolConnectionError,   # network / pool unreachable
    PoolAuthError,         # 401 / 403 from pool itself
    PoolUpstreamError,     # pool returned 4xx/5xx with a code
    PoolAcquireTimeoutError,
    PoolProtocolError,
)
```

Both `PooledClient` and `PooledCodexClient` expose:

- `.account`, `.lease_id`, `.request_id` — lease identity
- `.report_usage(input_tokens=, output_tokens=)` — bump pool's stats
- `await .report_error(code, message)` — cool the account upstream

## Requires

- Python ≥ 3.12
- `claude-agent-sdk` ≥ 0.1.63
- A running [sub-pool](https://github.com/the-upstream-org/sub-pool) server
- For Codex: the official `codex` CLI installed locally (the pool host
  no longer needs it — only consumers running codex do)

## License

Not yet specified.
