"""Loopback reverse proxy that lets an env-keyed CLI swap accounts mid-session.

`muse` takes its credential from `$META_API_KEY`, which is fixed for the
child's lifetime — so the file-rewrite trick sp-claude/codex/grok use to swap
accounts can't reach it. What CAN move is where it sends traffic:
`muse --base-url http://127.0.0.1:<port>/v1` routes every model call
(`POST /v1/responses`, `GET /muse-code/models`) through us. The child is
given a random per-session secret as its key; this proxy

  * rejects any request not bearing that secret (loopback-only AND keyed, so
    another local process can't ride the lease),
  * swaps in the CURRENT lease's real api_key and forwards to that lease's
    upstream origin — read per request out of the same `current` container
    the swap machinery advances, so a swap takes effect on the next request
    with no restart,
  * streams the response (SSE) back without buffering.

And it intercepts the two upstream answers worth acting on before the child
sees them (muse would otherwise retry a spent account 10x with backoff):

  * 429 "Subscription quota exhausted" — report RateLimit (once per lease;
    the message names the reset time the pool cools until), swap, replay
    the buffered request once on the new key. No account to swap to → the
    original 429 goes through. A plain rate limit passes straight through.
  * 401 on the api_key — report it (the pool cools; a consumer report never
    sticky-parks a shared account), swap, replay once.

A request is replayed only if it is self-contained: a Responses-API body
carrying `previous_response_id` or `store: true` refers to server-side state
on the OLD account, so it is answered with the original error instead.

Nothing here logs a key, the secret, or a body.
"""
from __future__ import annotations

import asyncio
import hmac
import json
import logging
import secrets
from dataclasses import dataclass
from http import HTTPStatus
from typing import Awaitable, Callable
from urllib.parse import urlsplit

import httpx

log = logging.getLogger(__name__)

# Hop-by-hop headers (RFC 9110 §7.6.1) plus the ones we always recompute.
_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade", "host", "content-length",
})
# Request bodies are a full Responses-API conversation (~100KB); headers are
# small. Generous caps, so a garbage client can't make us buffer unboundedly.
MAX_HEADER_BYTES = 64 * 1024
MAX_BODY_BYTES = 64 * 1024 * 1024


@dataclass
class _Request:
    method: str
    target: str
    headers: list[tuple[str, str]]
    body: bytes
    keep_alive: bool

    def header(self, name: str) -> str | None:
        name = name.lower()
        for k, v in self.headers:
            if k.lower() == name:
                return v
        return None


class _BadRequest(Exception):
    pass


def new_secret() -> str:
    """The per-session key the child is given instead of the real one."""
    return "sp-local-" + secrets.token_urlsafe(32)


def replayable(body: bytes) -> bool:
    """Can this request be re-sent on a DIFFERENT account?

    Only when it carries its whole context. `previous_response_id` (or a
    `store: true` the next turn may chain off) points at state kept by the
    account that answered it, which a replacement account doesn't have.
    Bodies that aren't JSON (none today) are treated as self-contained.
    """
    if not body:
        return True
    try:
        data = json.loads(body)
    except ValueError:
        return True
    if not isinstance(data, dict):
        return True
    return not data.get("previous_response_id") and data.get("store") is not True


def error_message(body: bytes) -> str:
    """The human message out of an upstream error body, for the pool report."""
    text = body.decode("utf-8", "replace")
    try:
        data = json.loads(text)
    except ValueError:
        return text.strip()[:500]
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict) and isinstance(err.get("message"), str):
            return err["message"][:500]
        for k in ("message", "detail", "error"):
            if isinstance(data.get(k), str):
                return data[k][:500]
    return text.strip()[:500]


class KeySwappingProxy:
    """See the module docstring.

    `current` is the session's `{"lease": …}` container. `key_of(lease)` and
    `origin_of(lease)` read the real api_key and upstream origin off it.
    `swap_from(lease_id, reason)` swaps away from `lease_id` (a no-op if the
    session already moved off it); `report(lease_id, code, message)` posts a
    report-error. `quota_exhausted(status, body)` decides which 429s are
    worth a swap.
    """

    def __init__(
        self, current: dict, *,
        key_of: Callable[[dict], str],
        origin_of: Callable[[dict], str],
        swap_from: Callable[[str, str], Awaitable[None]],
        report: Callable[[str, str, str], Awaitable[None]],
        quota_exhausted: Callable[[int, bytes], bool],
        secret: str | None = None,
        upstream: httpx.AsyncClient | None = None,
    ):
        self._current = current
        self._key_of = key_of
        self._origin_of = origin_of
        self._swap_from = swap_from
        self._report = report
        self._quota_exhausted = quota_exhausted
        self.secret = secret or new_secret()
        self._upstream = upstream or httpx.AsyncClient(
            # Streams are long-lived: no overall/read cap, only on connect.
            timeout=httpx.Timeout(None, connect=30.0),
            follow_redirects=False,
        )
        self._owns_upstream = upstream is None
        self._server: asyncio.AbstractServer | None = None
        self._reported: set[str] = set()
        self.port: int = 0

    # ----------------------------------------------------------- lifecycle

    async def start(self) -> str:
        """Bind to a random loopback port; returns the base URL for the CLI."""
        self._server = await asyncio.start_server(
            self._handle_conn, host="127.0.0.1", port=0,
            limit=MAX_HEADER_BYTES,
        )
        self.port = self._server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{self.port}"

    async def aclose(self) -> None:
        if self._server is not None:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), timeout=2.0)
            except (asyncio.TimeoutError, Exception):  # noqa: BLE001
                pass
        if self._owns_upstream:
            await self._upstream.aclose()

    # ------------------------------------------------------------ plumbing

    async def _handle_conn(self, reader: asyncio.StreamReader,
                           writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                try:
                    req = await _read_request(reader, writer)
                except _BadRequest:
                    await _write_full(writer, 400, [], b"bad request\n", keep_alive=False)
                    break
                if req is None:
                    break
                keep = await self._serve(req, writer)
                if not keep:
                    break
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception:  # noqa: BLE001 — never let one connection kill the server
            log.debug("proxy connection failed", exc_info=True)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001
                pass

    def _authorized(self, req: _Request) -> bool:
        auth = req.header("authorization") or ""
        scheme, _, token = auth.partition(" ")
        return (scheme.lower() == "bearer"
                and hmac.compare_digest(token.strip().encode(), self.secret.encode()))

    def _build(self, req: _Request, lease: dict) -> httpx.Request:
        headers = [(k, v) for k, v in req.headers
                   if k.lower() not in _HOP and k.lower() != "authorization"]
        headers.append(("Authorization", f"Bearer {self._key_of(lease)}"))
        url = self._origin_of(lease).rstrip("/") + req.target
        return self._upstream.build_request(
            req.method, url, headers=headers, content=req.body,
        )

    async def _serve(self, req: _Request, writer: asyncio.StreamWriter) -> bool:
        if not req.target.startswith("/"):
            await _write_full(writer, 400, [], b"absolute-form not supported\n",
                              keep_alive=req.keep_alive)
            return req.keep_alive
        if not self._authorized(req):
            await _write_full(
                writer, 401, [("content-type", "application/json")],
                b'{"error":{"message":"sp-muse proxy: bad local key"}}',
                keep_alive=req.keep_alive)
            return req.keep_alive

        replayed = False
        while True:
            lease = self._current["lease"]
            lease_id = lease.get("lease_id") or ""
            try:
                resp = await self._upstream.send(self._build(req, lease), stream=True)
            except httpx.HTTPError as e:
                await _write_full(
                    writer, 502, [("content-type", "text/plain")],
                    f"sp-muse proxy: upstream unreachable ({type(e).__name__})\n".encode(),
                    keep_alive=req.keep_alive)
                return req.keep_alive

            if resp.status_code in (401, 429) and not replayed:
                body = await resp.aread()
                await resp.aclose()
                if await self._should_retry(resp.status_code, body, lease_id, req):
                    replayed = True
                    continue
                await _write_full(writer, resp.status_code,
                                  _resp_headers(resp, decoded=True), body,
                                  keep_alive=req.keep_alive)
                return req.keep_alive

            try:
                await _write_stream(writer, resp, head_only=req.method == "HEAD",
                                    keep_alive=req.keep_alive)
            finally:
                await resp.aclose()
            return req.keep_alive

    async def _should_retry(self, status: int, body: bytes, lease_id: str,
                            req: _Request) -> bool:
        """Act on a 401/quota-429 from `lease_id`; True if the request should
        be replayed on the lease the session is on now."""
        if status == 429:
            if not self._quota_exhausted(status, body):
                return False  # a plain rate limit: muse's own backoff is right
            code, message = "RateLimit", error_message(body)
        else:
            code = "401"
            message = "upstream rejected the leased api_key (401)"
        if lease_id and lease_id not in self._reported:
            self._reported.add(lease_id)
            try:
                await self._report(lease_id, code, message)
            except Exception as e:  # noqa: BLE001
                log.warning("proxy: report-error failed: %s", e)
        try:
            await self._swap_from(lease_id, f"upstream {status}: {message[:80]}")
        except Exception as e:  # noqa: BLE001
            log.warning("proxy: swap failed: %s", e)
            return False
        moved = (self._current["lease"].get("lease_id") or "") != lease_id
        if moved and not replayable(req.body):
            log.info("proxy: swapped, but the request chains server-side "
                     "state; passing the %s through", status)
            return False
        return moved


# ------------------------------------------------------------ HTTP framing

async def _read_request(reader: asyncio.StreamReader,
                        writer: asyncio.StreamWriter) -> _Request | None:
    try:
        head = await reader.readuntil(b"\r\n\r\n")
    except asyncio.IncompleteReadError as e:
        if not e.partial.strip():
            return None
        raise _BadRequest from None
    except asyncio.LimitOverrunError:
        raise _BadRequest from None
    lines = head.decode("latin-1").split("\r\n")
    try:
        method, target, version = lines[0].split(" ", 2)
    except ValueError:
        raise _BadRequest from None
    headers: list[tuple[str, str]] = []
    for line in lines[1:]:
        if not line:
            continue
        k, sep, v = line.partition(":")
        if not sep:
            raise _BadRequest
        headers.append((k.strip(), v.strip()))
    req = _Request(method.upper(), target, headers, b"", True)
    conn = (req.header("connection") or "").lower()
    req.keep_alive = (conn != "close") if version == "HTTP/1.1" else (conn == "keep-alive")
    if (req.header("expect") or "").lower() == "100-continue":
        writer.write(b"HTTP/1.1 100 Continue\r\n\r\n")
        await writer.drain()
    te = (req.header("transfer-encoding") or "").lower()
    if "chunked" in te:
        chunks: list[bytes] = []
        total = 0
        while True:
            size_line = await reader.readuntil(b"\r\n")
            try:
                size = int(size_line.split(b";", 1)[0].strip(), 16)
            except ValueError:
                raise _BadRequest from None
            if size == 0:
                # Trailers (ignored) up to the blank line.
                while (await reader.readuntil(b"\r\n")) != b"\r\n":
                    pass
                break
            total += size
            if total > MAX_BODY_BYTES:
                raise _BadRequest
            chunks.append(await reader.readexactly(size))
            await reader.readexactly(2)
        req.body = b"".join(chunks)
    else:
        try:
            n = int(req.header("content-length") or 0)
        except ValueError:
            raise _BadRequest from None
        if n < 0 or n > MAX_BODY_BYTES:
            raise _BadRequest
        req.body = await reader.readexactly(n) if n else b""
    return req


def _resp_headers(resp: httpx.Response, *, decoded: bool) -> list[tuple[str, str]]:
    out = []
    for k, v in resp.headers.multi_items():
        lk = k.lower()
        if lk in _HOP:
            continue
        # A body we already decoded (aread) must not claim its old encoding.
        if decoded and lk == "content-encoding":
            continue
        out.append((k, v))
    return out


def _status_line(status: int) -> bytes:
    try:
        phrase = HTTPStatus(status).phrase
    except ValueError:
        phrase = ""
    return f"HTTP/1.1 {status} {phrase}\r\n".encode("latin-1")


def _head(status: int, headers: list[tuple[str, str]], extra: list[tuple[str, str]]) -> bytes:
    lines = [_status_line(status)]
    for k, v in [*headers, *extra]:
        lines.append(f"{k}: {v}\r\n".encode("latin-1", "replace"))
    lines.append(b"\r\n")
    return b"".join(lines)


async def _write_full(writer: asyncio.StreamWriter, status: int,
                      headers: list[tuple[str, str]], body: bytes, *,
                      keep_alive: bool) -> None:
    extra = [("content-length", str(len(body)))]
    if not keep_alive:
        extra.append(("connection", "close"))
    writer.write(_head(status, headers, extra) + body)
    await writer.drain()


async def _write_stream(writer: asyncio.StreamWriter, resp: httpx.Response, *,
                        head_only: bool, keep_alive: bool) -> None:
    """Relay status + headers, then the body chunk by chunk as it arrives —
    raw (still content-encoded) and re-framed chunked, so SSE events reach
    the child the moment upstream emits them."""
    headers = _resp_headers(resp, decoded=False)
    no_body = head_only or resp.status_code in (204, 304) or resp.status_code < 200
    extra: list[tuple[str, str]] = []
    if not keep_alive:
        extra.append(("connection", "close"))
    if no_body:
        extra.append(("content-length", "0"))
        writer.write(_head(resp.status_code, headers, extra))
        await writer.drain()
        return
    extra.append(("transfer-encoding", "chunked"))
    writer.write(_head(resp.status_code, headers, extra))
    await writer.drain()
    async def _chunks():
        try:
            async for chunk in resp.aiter_raw():
                yield chunk
        except httpx.StreamConsumed:
            # A response constructed already-read (a mock transport hands
            # these out) has no raw stream left; its body is in memory.
            yield resp.content

    async for chunk in _chunks():
        if not chunk:
            continue
        writer.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
        await writer.drain()
    writer.write(b"0\r\n\r\n")
    await writer.drain()


def origin_of_base_url(base_url: str | None, default: str) -> str:
    """`https://api.meta.ai/v1` → `https://api.meta.ai`. The child's own
    paths already carry `/v1` (and `/muse-code/…` sits beside it)."""
    parts = urlsplit(base_url or default)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        parts = urlsplit(default)
    return f"{parts.scheme}://{parts.netloc}"


__all__ = [
    "KeySwappingProxy", "error_message", "new_secret", "origin_of_base_url",
    "replayable",
]
