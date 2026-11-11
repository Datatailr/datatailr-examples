"""Test doubles shared by the gateway tests."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from llm_service.gateway.engine import EngineError
from llm_service.gateway.storage import MemoryBlobStore


class FlakyBlobStore(MemoryBlobStore):
    """A memory store whose `put` fails while `fail_puts` is true."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_puts = False

    def put(self, path: str, data: bytes | str) -> None:
        if self.fail_puts:
            raise OSError("simulated storage failure")
        super().put(path, data)


class FakeEngine:
    """An engine double. It records calls and fails for chosen model names."""

    def __init__(self) -> None:
        self.base_url = "http://127.0.0.1:0"
        self.started: list[tuple[Path, str]] = []
        self.stopped = 0
        self.fail_on: set[str] = set()
        self.alive_flag = True

    async def start(self, model_dir: Path, served_name: str) -> None:
        if served_name in self.fail_on:
            raise EngineError(f"cannot start {served_name}")
        self.started.append((model_dir, served_name))
        self.alive_flag = True

    async def stop(self) -> None:
        self.stopped += 1

    def alive(self) -> bool:
        return self.alive_flag


FAKE_USAGE = {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}


def fake_vllm_app(status: int = 200) -> FastAPI:
    """Return a FastAPI app that behaves like vLLM for the proxy tests.

    The app records each request body in `app.state.requests`. When `status`
    is not 200, the three POST endpoints answer with that status.
    """
    app = FastAPI()
    app.state.requests = []

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    def sse(obj: dict) -> bytes:
        return b"data: " + json.dumps(obj).encode() + b"\n\n"

    async def answer(request: Request):
        body = await request.json()
        app.state.requests.append(body)
        if status != 200:
            return JSONResponse({"error": {"message": "bad request"}}, status_code=status)
        if not body.get("stream"):
            return {"id": "x", "choices": [], "usage": FAKE_USAGE}
        with_usage = bool((body.get("stream_options") or {}).get("include_usage"))

        async def events():
            for text in ("Hi", "!"):
                chunk = {
                    "id": "x",
                    "object": "chat.completion.chunk",
                    "choices": [{"index": 0, "delta": {"content": text}}],
                }
                if with_usage:
                    chunk["usage"] = None
                yield sse(chunk)
            if with_usage:
                yield sse({"id": "x", "object": "chat.completion.chunk", "choices": [], "usage": FAKE_USAGE})
            yield b"data: [DONE]\n\n"

        return StreamingResponse(events(), media_type="text/event-stream")

    app.post("/v1/chat/completions")(answer)
    app.post("/v1/completions")(answer)
    app.post("/v1/embeddings")(answer)
    return app
