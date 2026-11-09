"""The Postgres-over-WebSocket relay, against a TCP echo server."""

from __future__ import annotations

import json
import socket
import threading

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from neon_postgres.service.pg_tunnel import add_pg_tunnel, tunnel_enabled


@pytest.fixture(scope="module")
def echo_port() -> int:
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen()
    server.settimeout(0.2)
    stop = threading.Event()

    def serve_one(conn: socket.socket) -> None:
        with conn:
            while chunk := conn.recv(65536):
                conn.sendall(chunk)

    def serve() -> None:
        while not stop.is_set():
            try:
                conn, _ = server.accept()
            except socket.timeout:
                continue
            threading.Thread(target=serve_one, args=(conn,), daemon=True).start()

    threading.Thread(target=serve, daemon=True).start()
    yield server.getsockname()[1]
    stop.set()
    server.close()


@pytest.fixture
def client(echo_port: int) -> TestClient:
    app = FastAPI()
    add_pg_tunnel(app, "127.0.0.1", echo_port)
    return TestClient(app)


def test_bytes_go_both_ways(client: TestClient) -> None:
    with client.websocket_connect("/pg", headers={"x-datatailr-user": json.dumps({"name": "alice"})}) as ws:
        ws.send_bytes(b"\x00\x00\x00\x08\x04\xd2\x16\x2f")  # a Postgres SSLRequest
        assert ws.receive_bytes() == b"\x00\x00\x00\x08\x04\xd2\x16\x2f"
        big = bytes(range(256)) * 1000
        ws.send_bytes(big)
        got = b""
        while len(got) < len(big):
            got += ws.receive_bytes()
        assert got == big


def test_text_frames_are_relayed_too(client: TestClient) -> None:
    with client.websocket_connect("/pg") as ws:
        ws.send_text("hello")
        assert ws.receive_bytes() == b"hello"


def test_unreachable_postgres_closes_the_socket() -> None:
    closed = socket.socket()
    closed.bind(("127.0.0.1", 0))
    port = closed.getsockname()[1]
    closed.close()
    app = FastAPI()
    add_pg_tunnel(app, "127.0.0.1", port)
    with pytest.raises(Exception):  # the server closes before the handshake completes
        with TestClient(app).websocket_connect("/pg"):
            pass


def test_tunnel_enabled_reads_both_spellings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NEON_PG_TUNNEL", raising=False)
    monkeypatch.delenv("DATATAILR_JOB_ENV_NEON_PG_TUNNEL", raising=False)
    assert tunnel_enabled()
    monkeypatch.setenv("NEON_PG_TUNNEL", "0")
    assert not tunnel_enabled()
    monkeypatch.setenv("DATATAILR_JOB_ENV_NEON_PG_TUNNEL", "false")
    assert not tunnel_enabled()
    monkeypatch.setenv("DATATAILR_JOB_ENV_NEON_PG_TUNNEL", "1")
    assert tunnel_enabled()
