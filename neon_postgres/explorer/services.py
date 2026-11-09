"""Find Neon Postgres services on the platform.

Every Datatailr service answers on ``http://<name>/`` inside the environment.
A service built from this example returns JSON with a ``tenant_id`` there, which
is how database services are told apart from the others.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger("explorer.services")


def platform_services() -> list[dict]:
    """All services the platform lists, as ``{"name", "display_name", "state"}``; empty off-platform."""
    try:
        from datatailr.wrapper import dt__Job  # type: ignore

        listed = dt__Job().ls(filter="type = service")
    except Exception as exc:  # no CLI, not logged in, or running outside the platform
        log.info("service listing unavailable: %s", exc)
        return []
    if isinstance(listed, dict):
        listed = listed.get("jobs", [])
    services = []
    for job in listed or []:
        if isinstance(job, dict) and job.get("name"):
            services.append(
                {"name": job["name"], "display_name": job.get("display_name") or job["name"], "state": job.get("state")}
            )
    return services


def probe(name: str, timeout: float = 2.0) -> dict | None:
    """The status JSON of a Neon Postgres service, or None if ``name`` is not one."""
    try:
        with urllib.request.urlopen(f"http://{name}/", timeout=timeout) as response:
            data = json.loads(response.read())
    except (urllib.error.URLError, ValueError, OSError):
        return None
    if isinstance(data, dict) and "tenant_id" in data:
        return data
    return None


def database_services(timeout: float = 2.0) -> list[dict]:
    """Platform services that answer like a Neon Postgres service, with their port and user."""
    services = platform_services()
    if not services:
        return []
    with ThreadPoolExecutor(max_workers=16) as pool:
        probes = list(pool.map(lambda s: probe(s["name"], timeout), services))
    found = []
    for service, status in zip(services, probes):
        if status is not None:
            found.append(
                {
                    **service,
                    "host": service["name"],
                    "port": status.get("port", 5432),
                    "user": status.get("user", "cloud_admin"),
                    "database": status.get("database", "postgres"),
                    "bucket": status.get("bucket"),
                    "status": status.get("status"),
                    # Set when the service syncs platform users into Postgres roles,
                    # so the browser can offer "connect as me".
                    "role_sync": bool(status.get("role_sync")),
                }
            )
    return found
