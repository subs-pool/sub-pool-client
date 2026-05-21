# sub-pool

This distribution exists to **reserve the `sub-pool` name on PyPI**. It
has no Python module of its own — it depends on
[`sub-pool-client`](https://pypi.org/project/sub-pool-client/), so
`pip install sub-pool` actually gets you the client.

If you came here looking for the `sub-pool` server, that lives upstream
and is not (currently) on PyPI.

## What you probably want

```bash
pip install sub-pool-client
```

Then either use the CLIs (`sp-claude`, `sp-codex`) or the Python SDK
(`from sub_pool_client import PooledClient, PooledCodexClient`).

See <https://github.com/subs-pool/sub-pool-client> for the full client
docs.
