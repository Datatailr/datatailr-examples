"""The aws-chunked body decoder, fed in awkward pieces, and an end-to-end chunked PUT."""

from __future__ import annotations

import httpx
import pytest

from neon_postgres.s3_gateway.app import AwsChunkedDecoder, create_app
from neon_postgres.s3_gateway.backends import MemoryBackend


def _chunked(payload: bytes, chunk_size: int, signed: bool = False) -> bytes:
    out = b""
    for i in range(0, len(payload), chunk_size):
        piece = payload[i : i + chunk_size]
        header = f"{len(piece):x}"
        if signed:
            header += ";chunk-signature=" + "ab" * 32
        out += header.encode() + b"\r\n" + piece + b"\r\n"
    out += b"0\r\n"
    if signed:
        out += b"x-amz-checksum-crc32:AAAAAA==\r\n"
    out += b"\r\n"
    return out


@pytest.mark.parametrize("feed_size", [1, 3, 7, 1000])
@pytest.mark.parametrize("signed", [False, True])
def test_decoder_handles_arbitrary_split_points(feed_size, signed):
    payload = bytes(range(256)) * 10
    encoded = _chunked(payload, chunk_size=300, signed=signed)
    decoder = AwsChunkedDecoder()
    out = b""
    for i in range(0, len(encoded), feed_size):
        out += decoder.feed(encoded[i : i + feed_size])
    assert out == payload
    assert decoder.done


def test_decoder_ignores_bytes_after_the_final_chunk():
    decoder = AwsChunkedDecoder()
    assert decoder.feed(_chunked(b"abc", 10) + b"garbage") == b"abc"
    assert decoder.feed(b"more") == b""


@pytest.mark.anyio
async def test_chunked_put_with_unsigned_trailer():
    backend = MemoryBackend()
    app = create_app(backend)
    payload = b"layer-file-bytes" * 5000
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as client:
        response = await client.put(
            "/neon/pageserver/tenants/t/timelines/l/000-100",
            content=_chunked(payload, 65536, signed=True),
            headers={
                "content-encoding": "aws-chunked",
                "x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                "x-amz-decoded-content-length": str(len(payload)),
                "x-amz-trailer": "x-amz-checksum-crc32",
            },
        )
        assert response.status_code == 200
        got = await client.get("/neon/pageserver/tenants/t/timelines/l/000-100")
        assert got.status_code == 200
        assert got.content == payload


@pytest.mark.anyio
async def test_validate_upcall_confirms_every_generation():
    app = create_app(MemoryBackend())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        response = await client.post(
            "/validate", json={"tenants": [{"id": "abc-0000", "gen": 3}, {"id": "def-0000", "gen": 7}]}
        )
        assert response.status_code == 200
        assert response.json() == {"tenants": [{"id": "abc-0000", "valid": True}, {"id": "def-0000", "valid": True}]}


@pytest.fixture
def anyio_backend():
    return "asyncio"
