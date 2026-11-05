from sub_pool_client.client import PooledClient
from sub_pool_client.codex import PooledCodexClient
from sub_pool_client.errors import (
    PoolAcquireTimeoutError,
    PoolAuthError,
    PoolConnectionError,
    PoolError,
    PoolProtocolError,
    PoolUpstreamError,
)

from sub_pool_client._client_id import client_version as _client_version

__version__ = _client_version()
__all__ = [
    "PooledClient",
    "PooledCodexClient",
    "PoolError",
    "PoolConnectionError",
    "PoolAuthError",
    "PoolAcquireTimeoutError",
    "PoolUpstreamError",
    "PoolProtocolError",
]
