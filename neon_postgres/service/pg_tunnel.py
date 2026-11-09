"""Postgres over WebSocket: a relay from the service's HTTP port to Postgres.

The platform ingress carries HTTP only, and the Postgres wire protocol is raw
TCP. A WebSocket is the one kind of long-lived byte stream that the ingress
passes through to a job, so this module adds a ``/pg`` WebSocket endpoint to
the status API. Every binary frame that arrives is written to the Postgres
socket, and every byte that Postgres sends goes back as a binary frame.

A client on a laptop turns a local TCP port into that WebSocket with
``websocat``; see "External connections" in the README. The platform
authenticates the WebSocket request before it reaches this process: the
user's login cookie must be valid and the user must hold the ``access``
permission on this job. Postgres then asks for its own password as usual.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

log = logging.getLogger("pg_tunnel")

PATH = "/pg"
CHUNK = 64 * 1024


def tunnel_enabled() -> bool:
    """Read ``NEON_PG_TUNNEL`` (default on; ``0``, ``false`` or ``no`` turns it off)."""
    value = os.environ.get("DATATAILR_JOB_ENV_NEON_PG_TUNNEL", os.environ.get("NEON_PG_TUNNEL", "1"))
    return value.strip().lower() not in ("0", "false", "no", "off", "")


def user_name(websocket: WebSocket) -> str:
    """The platform user who opened the connection, from the signed user header."""
    raw = websocket.headers.get("x-datatailr-user", "")
    try:
        name = json.loads(raw).get("name")
    except (ValueError, AttributeError):
        name = None
    return name or "unknown"


def add_pg_tunnel(app: FastAPI, host: str, port: int, path: str = PATH) -> None:
    """Register the WebSocket endpoint on *app* that relays bytes to ``host:port``."""

    @app.websocket(path)
    async def pg_tunnel(websocket: WebSocket) -> None:
        user = user_name(websocket)
        try:
            reader, writer = await asyncio.open_connection(host, port)
        except OSError as exc:
            log.warning("tunnel for %s refused: %s:%d is unreachable (%s)", user, host, port, exc)
            await websocket.close(code=1011, reason="postgres is not reachable")
            return
        await websocket.accept()
        sent = received = 0
        log.info("tunnel opened by %s", user)

        async def client_to_postgres() -> None:
            nonlocal received
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    return
                data = message.get("bytes")
                if data is None:
                    data = message.get("text", "").encode()
                received += len(data)
                writer.write(data)
                await writer.drain()

        async def postgres_to_client() -> None:
            nonlocal sent
            while chunk := await reader.read(CHUNK):
                sent += len(chunk)
                await websocket.send_bytes(chunk)

        tasks = [asyncio.ensure_future(client_to_postgres()), asyncio.ensure_future(postgres_to_client())]
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            for task in done:
                exc = task.exception()
                if exc and not isinstance(exc, (WebSocketDisconnect, ConnectionError)):
                    log.warning("tunnel for %s ended with an error: %s", user, exc)
        finally:
            writer.close()
            try:
                await websocket.close()
            except Exception:  # the client is already gone
                pass
            log.info("tunnel closed for %s: %d bytes to postgres, %d bytes to the client", user, received, sent)
