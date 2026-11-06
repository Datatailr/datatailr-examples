"""Datatailr service entrypoint: Postgres (Neon) backed by Datatailr blob storage.

``main(port)`` starts, in this order:

1. the S3 gateway on localhost, translating S3 calls into ``dt blob`` operations;
2. the Neon stack (storage broker, safekeeper, pageserver, compute) pointed at
   that gateway;
3. a small HTTP status API on the platform-assigned ``port``.

Postgres itself listens on ``NEON_PG_PORT`` (5432) on all interfaces, so other
jobs in the same environment connect to it by the service's hostname.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
import time

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse

from neon_postgres.s3_gateway.app import create_app as create_gateway
from neon_postgres.s3_gateway.backends import backend_from_env
from neon_postgres.service.neon_stack import NeonConfig, NeonStack

log = logging.getLogger("neon_postgres")


def start_gateway(backend, port: int) -> uvicorn.Server:
    config = uvicorn.Config(create_gateway(backend), host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    threading.Thread(target=server.run, name="s3-gateway", daemon=True).start()
    deadline = time.time() + 30
    while not server.started:
        if time.time() > deadline:
            raise RuntimeError("the S3 gateway did not start")
        time.sleep(0.1)
    log.info("S3 gateway listening on 127.0.0.1:%d", port)
    return server


def status_app(stack: NeonStack) -> FastAPI:
    app = FastAPI(
        title="Neon Postgres",
        description="A Postgres database whose durable state lives in Datatailr blob storage.",
    )
    cfg = stack.cfg

    @app.get("/", response_class=JSONResponse, summary="Connection details and process status")
    def index():
        return {
            "status": "running" if not stack.dead_processes() else "degraded",
            "host": os.environ.get("HOSTNAME", "localhost"),
            "port": cfg.pg_port,
            "user": cfg.pg_user,
            "database": "postgres",
            "tenant_id": stack.state.tenant_id if stack.state else None,
            "timeline_id": stack.state.timeline_id if stack.state else None,
            "generation": stack.state.generation if stack.state else None,
            "bucket": cfg.bucket,
            "processes": {name: process.pid for name, process in stack.processes.items()},
        }

    @app.get("/health", response_class=PlainTextResponse, summary="Health check")
    def health():
        if stack.dead_processes():
            return PlainTextResponse(f"DEAD: {', '.join(stack.dead_processes())}\n", status_code=503)
        if not stack.postgres_ready():
            return PlainTextResponse("postgres not ready\n", status_code=503)
        return "OK\n"

    return app


def watchdog(stack: NeonStack, status_server: uvicorn.Server) -> None:
    """Exit the service when any Neon process dies; the platform restarts it."""
    while not stack.stopping:
        dead = stack.dead_processes()
        if dead:
            log.error("process(es) exited: %s; shutting down so the platform restarts the service", dead)
            stack.stop()
            status_server.should_exit = True
            time.sleep(2)
            os._exit(1)
        time.sleep(2)


def main(port):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    cfg = NeonConfig.from_env()
    backend = backend_from_env()

    start_gateway(backend, cfg.gateway_port)
    stack = NeonStack(cfg, backend)
    stack.start()
    log.info("postgres ready on port %d (tenant %s, generation %d)", cfg.pg_port, stack.state.tenant_id, stack.state.generation)

    status_server = uvicorn.Server(uvicorn.Config(status_app(stack), host="0.0.0.0", port=int(port), log_level="info"))

    def shutdown(signum, frame):
        log.info("signal %d received, stopping", signum)
        stack.stop()
        status_server.should_exit = True

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    threading.Thread(target=watchdog, args=(stack, status_server), name="watchdog", daemon=True).start()
    status_server.run()
    stack.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Neon Postgres service locally.")
    parser.add_argument("--port", type=int, default=8080, help="port for the status API")
    main(parser.parse_args().port)
