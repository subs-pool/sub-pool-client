# sub-pool-client

Python client + interactive CLIs for a
[sub-pool](https://github.com/the private upstream repo) server — a credential
broker for N Claude **and** OpenAI Codex subscription accounts.

You get three things in one package:

1. **`sp-claude`** — a `claude` CLI wrapper. Leases a Claude account
   from the pool, runs the real `claude` binary against a per-session
   `CLAUDE_CONFIG_DIR`, rotates the access_token in the background, and
   swaps accounts transparently if the leased one cools mid-run.
2. **`sp-codex`** — same flow for OpenAI Codex. Leases a Codex
   `auth.json`, drops it into an isolated `CODEX_HOME`, spawns the
   real `codex` binary, and rotates the token in the background.
3. **`PooledClient` / `PooledCodexClient`** — Python SDK classes for
   programs and services. `PooledClient` is a drop-in subclass of
   `ClaudeSDKClient` from the official `claude-agent-sdk`;
   `PooledCodexClient` is a thin async wrapper around `codex exec`.

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

Both the CLIs and the SDK read:

```bash
export SUB_POOL_URL=http://your-pool-host:8787
export SUB_POOL_KEY=cp-...   # admin issues this from the sub-pool UI
```

`SUB_POOL_KEY`'s strategy decides which accounts you can reach.

## 1. CLIs

### `sp-claude` — `claude` wrapper

```bash
sp-claude --setup            # one-time interactive config wizard
sp-claude                    # start the claude REPL on a leased account
sp-claude "explain this"     # single-prompt mode
sp-claude --account alice    # pin to a specific account
sp-claude --status           # show effective config and exit
```

`sp-claude --setup` writes pool URL + API key to `~/.sub-pool/cli.toml`
at `0o600`. Subsequent invocations exec `claude` against a per-session
config dir that holds the leased `.credentials.json`. The persistent
home (`~/.sub-pool/claude-home/`) keeps your conversation history
across sessions and is isolated from `~/.claude/` — `sp-claude` never
reads or writes your real claude config.

If the leased account starts cooling (Anthropic rate-limit or quota
window), a background watcher swaps to a different account
transparently. Your running session sees one continuous `claude`
process; only the access_token under the hood changes.

### `sp-codex` — `codex` wrapper

```bash
sp-codex --setup
sp-codex                     # interactive REPL on a leased Codex account
sp-codex exec "summarize ."  # one-shot
sp-codex --account codex1
```

Shares `~/.sub-pool/cli.toml` with `sp-claude`. Persistent
`CODEX_HOME` lives at `~/.sub-pool/codex-home/`.

## 2. Python SDK

### `PooledClient` (Claude)

```python
import asyncio
from claude_agent_sdk import ClaudeAgentOptions
from sub_pool_client import PooledClient

async def main():
    options = ClaudeAgentOptions(system_prompt="Be terse.")
    async with PooledClient(options=options) as client:
        print(f"leased {client.account} lease_id={client.lease_id}")
        await client.query("List three prime numbers.")
        async for msg in client.receive_response():
            print(type(msg).__name__, "->", msg)

asyncio.run(main())
```

`PooledClient` subclasses `ClaudeSDKClient`, so every option the SDK
accepts (hooks, MCP servers, `cwd`, `allowed_tools`,
`permission_mode`) works unchanged.

#### Rate-limit handling

```python
from claude_agent_sdk import ClaudeSDKError

async with PooledClient(options=options) as client:
    try:
        await client.query("...")
        async for msg in client.receive_response():
            ...
    except ClaudeSDKError as e:
        if "429" in str(e) or "rate" in str(e).lower():
            await client.report_error("RateLimit", str(e))
        raise
```

`report_error` posts to `/credentials/lease/{id}/report-error`; the
pool marks the account COOLING so the next lease skips it.

#### Concurrent agents

Multiple `async with PooledClient()` calls in the same process — or
across sibling processes — with identical routing inputs (`user_id`,
`required_model`) **share a single lease**. Coordination happens
through `~/.sub-pool/client/<hash>/` (flock + holder list). First
arriver leases; later arrivers refcount; last out releases. So N
parallel agents on one account work up to Anthropic's per-account
concurrency limit. Vary `user_id` (with the `sticky_user` strategy)
to force different accounts in parallel.

### `PooledCodexClient` (Codex)

```python
import asyncio
from sub_pool_client import PooledCodexClient

async def main():
    async with PooledCodexClient() as codex:
        proc = await codex.exec(
            "summarize this repository",
            cwd=".",
            model="gpt-5.5",
        )
        stdout, stderr = await proc.communicate()
        print(stdout.decode())

asyncio.run(main())
```

`PooledCodexClient` writes the leased (sanitized) `auth.json` into an
isolated `CODEX_HOME` and spawns `codex exec` against it. A
background poll task rotates the on-disk `auth.json` before the
access_token expires. On a backend error, call
`await codex.report_error("RateLimit", "...")` so the pool cools the
account.

## Pool invariants this client relies on

- The pool exclusively owns the `refresh_token` chain (both Anthropic
  and OpenAI). Leases hand out only the access_token; the
  `refreshToken` field in the leased credentials file is blank.
- Token rotation happens through `POST
  /credentials/lease/{lease_id}/token` — a consumer crash can never
  leak the pool's refresh state.
- `account_name`s come from the pool's admin config. The pool decides
  which account fulfills each lease based on the API key's strategy.

See [the private upstream repo](https://github.com/the private upstream repo) for the
pool server, admin UI, and strategy reference.

## Layout under `~/.sub-pool/`

```
~/.sub-pool/
├── cli.toml                 # sp-claude / sp-codex shared config (0o600)
├── claude-home/             # sp-claude persistent CLAUDE_CONFIG_DIR
├── codex-home/              # sp-codex persistent CODEX_HOME
└── client/<hash>/           # PooledClient / PooledCodexClient
                             #   cross-process lease coordination
```

Override the SDK shared-dir root with `SUB_POOL_CLIENT_DIR`.

## Examples

- `examples/hello.py` — `PooledClient` end-to-end with a retry loop on 429.
- `examples/codex_hello.py` — `PooledCodexClient` running `codex exec`.

## License

(TBD — defer to upstream)
