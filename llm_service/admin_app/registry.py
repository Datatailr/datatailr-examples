"""The registry of LLM services that the admin app knows."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import subprocess
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Protocol

import httpx

log = logging.getLogger(__name__)

NAME_PATTERN = re.compile(r"[a-z0-9-]{1,63}")
API_NAME = "datatailr-llm-service"
API_VERSION = 1
MANUAL_BLOB_PATH = "llm-service-admin/services.json"
LISTING_TIMEOUT_SECONDS = 30.0
NOT_ADMIN = "The owner of the app is not an admin of this service."


def valid_name(name: object) -> bool:
    """Return True if `name` is a service name."""
    return isinstance(name, str) and NAME_PATTERN.fullmatch(name) is not None


def service_url(name: str) -> str:
    """Return the internal URL of the service `name`. Raise `ValueError` for a bad name."""
    if not valid_name(name):
        raise ValueError("The service name is not valid.")
    return "http://" + name


@dataclass
class Entry:
    """One service in the registry."""

    name: str
    source: str
    state: str
    detail: str | None
    model: str | None
    engine: str | None
    checked_at: float

    def to_dict(self) -> dict:
        return asdict(self)


class ManualStore(Protocol):
    """Keeps the names that the owner of the app adds."""

    def load(self) -> list[str]: ...

    def save(self, names: list[str]) -> None: ...


class BlobManualStore:
    """Manual names in a JSON list in blob storage."""

    def load(self) -> list[str]:
        """Return the saved names.

        A missing file and content that is not a JSON list give an empty list.
        Any other error propagates, so a failed read never looks like an empty list.
        """
        from datatailr import Blob

        blob = Blob()
        try:
            data = blob.get(MANUAL_BLOB_PATH)
        except Exception:
            if not blob.exists(MANUAL_BLOB_PATH):
                return []
            raise
        try:
            names = json.loads(data)
        except ValueError:
            return []
        if not isinstance(names, list):
            return []
        return [name for name in names if valid_name(name)]

    def save(self, names: list[str]) -> None:
        from datatailr import Blob

        Blob().put(MANUAL_BLOB_PATH, json.dumps(names))


def platform_listing(environment: str, run=subprocess.run) -> list[str]:
    """Return the names of the ready services in `environment`. Raise on failure.

    The field `name` of a listed job is already the host form, for example `llm-service`.
    The field `display_name` holds the human name, so this function does not read it.
    """
    result = run(
        ["dt", "job", "ls", "-j", "-f", "type = service"],
        capture_output=True,
        text=True,
        timeout=LISTING_TIMEOUT_SECONDS,
    )
    if result.returncode != 0:
        raise RuntimeError(f"dt job ls failed with code {result.returncode}: {result.stderr.strip()}")
    jobs = json.loads(result.stdout)
    if not isinstance(jobs, list):
        raise ValueError("dt job ls did not print a list")
    names = []
    for job in jobs:
        if not isinstance(job, dict):
            continue
        if job.get("state") == "ready" and job.get("environment") == environment and valid_name(job.get("name")):
            names.append(job["name"])
    return names


class Registry:
    """Finds the LLM services and keeps the result of the last probe of each."""

    def __init__(
        self,
        http: httpx.AsyncClient,
        configured: list[str],
        manual: ManualStore,
        listing: Callable[[], list[str]],
        clock: Callable[[], float] = time.time,
        probe_timeout: float = 3.0,
        retry_after: float = 600.0,
        concurrency: int = 10,
    ) -> None:
        self._http = http
        self._configured = [name for name in dict.fromkeys(configured) if valid_name(name)]
        self._manual_store = manual
        self._listing = listing
        self._clock = clock
        self._probe_timeout = probe_timeout
        self._retry_after = retry_after
        self._concurrency = concurrency
        self._entries: dict[str, Entry] = {}
        self._manual: list[str] = []
        self._paused: dict[str, float] = {}  # found names that failed, with the time of the probe
        self._ready = asyncio.Event()
        self._lock = asyncio.Lock()  # one of refresh, add_manual and remove_manual at a time
        self.listing_state = "ok"

    async def refresh(self) -> None:
        """Build the candidate names, probe them and replace the entries."""
        async with self._lock:
            await self._refresh()

    async def _refresh(self) -> None:
        """Probe the configured and manual names and publish them. Then add the names of the listing.

        The listing can take 30 seconds, so the first page load does not wait for it.
        """
        manual = await self._load_manual()
        sources: dict[str, str] = {}
        for source, names in (("manual", manual), ("configured", self._configured)):
            for name in names:
                sources[name] = source  # a later source has the higher priority
        own = await self._probe_all(sorted(sources), sources)
        # The found entries of the last refresh stay until the listing answers again.
        kept = {n: e for n, e in self._entries.items() if e.source == "found" and n not in sources}
        self._entries = {**kept, **{entry.name: entry for entry in own}}
        self._ready.set()

        found = [name for name in await self._find() if name not in sources]
        now = self._clock()
        self._paused = {n: t for n, t in self._paused.items() if n in found and now - t < self._retry_after}
        candidates = [n for n in sorted(set(found)) if n not in self._paused]
        entries = {entry.name: entry for entry in own}
        for entry in await self._probe_all(candidates, {name: "found" for name in candidates}):
            if entry.state in ("not_llm", "unreachable"):
                self._paused[entry.name] = entry.checked_at
            else:
                entries[entry.name] = entry
        self._entries = entries

    async def _probe_all(self, names: list[str], sources: dict[str, str]) -> list[Entry]:
        """Probe `names`, at most `concurrency` at a time."""
        gate = asyncio.Semaphore(self._concurrency)

        async def probe(name: str) -> Entry:
            async with gate:
                return await self._probe(name, sources[name])

        return await asyncio.gather(*(probe(name) for name in names))

    async def run(self, interval: float = 60.0) -> None:
        """Refresh every `interval` seconds. Log errors and keep going."""
        while True:
            try:
                await self.refresh()
            except Exception:
                log.exception("The refresh of the service registry failed.")
            self._ready.set()
            await asyncio.sleep(interval)

    async def wait_ready(self, timeout: float = 10.0) -> bool:
        """Wait for the first refresh. Return False if it takes longer than `timeout`."""
        try:
            await asyncio.wait_for(self._ready.wait(), timeout)
        except asyncio.TimeoutError:
            return False
        return True

    def entries(self) -> list[Entry]:
        return sorted(self._entries.values(), key=lambda entry: entry.name)

    def get(self, name: str) -> Entry | None:
        entry = self._entries.get(name)
        return entry if entry is not None and entry.state == "ok" else None

    def manual_names(self) -> list[str]:
        return list(self._manual)

    async def add_manual(self, name: str) -> Entry:
        """Save a manual name and probe it at once. Raise `ValueError` for a bad name."""
        if not valid_name(name):
            raise ValueError("The service name has 1 to 63 characters from a-z, 0-9 and -.")
        async with self._lock:
            names = [n for n in await asyncio.to_thread(self._manual_store.load) if valid_name(n)]
            if name not in names:
                names = [*names, name]
                await asyncio.to_thread(self._manual_store.save, names)
            self._manual = names
            entry = await self._probe(name, "configured" if name in self._configured else "manual")
            self._entries = {**self._entries, name: entry}
            self._paused.pop(name, None)
            return entry

    async def remove_manual(self, name: str) -> bool:
        """Remove a manual name. Return False if it was not in the list."""
        async with self._lock:
            names = [n for n in await asyncio.to_thread(self._manual_store.load) if valid_name(n)]
            if name not in names:
                self._manual = names
                return False
            names = [n for n in names if n != name]
            await asyncio.to_thread(self._manual_store.save, names)
            self._manual = names
            entry = self._entries.get(name)
            if entry is not None and entry.source == "manual":
                self._entries = {n: e for n, e in self._entries.items() if n != name}
            return True

    async def _load_manual(self) -> list[str]:
        try:
            names = await asyncio.to_thread(self._manual_store.load)
        except Exception:
            log.exception("The load of the manual service list failed.")
            return self._manual
        self._manual = [name for name in names if valid_name(name)]
        return self._manual

    async def _find(self) -> list[str]:
        try:
            names = await asyncio.to_thread(self._listing)
        except Exception as error:
            if self.listing_state != "failed":
                log.warning("The platform listing of services failed: %s", error)
            self.listing_state = "failed"
            return []
        self.listing_state = "ok"
        return [name for name in names if valid_name(name)]

    async def _probe(self, name: str, source: str) -> Entry:
        """Call `GET /v1/service` on `name` and classify the answer."""
        state, detail, model, engine = await self._classify(name)
        return Entry(name, source, state, detail, model, engine, self._clock())

    async def _classify(self, name: str) -> tuple[str, str | None, str | None, str | None]:
        try:
            response = await asyncio.wait_for(
                self._http.get(
                    service_url(name) + "/v1/service",
                    headers={"Connection": "close"},
                    timeout=self._probe_timeout,
                ),
                self._probe_timeout,
            )
        except Exception as error:  # one bad service must not stop the others
            return "unreachable", f"The service did not answer ({type(error).__name__}).", None, None
        if response.status_code in (401, 403):
            return "no_access", f"The service refused the owner of the app (status {response.status_code}).", None, None
        body = None
        if response.status_code == 200:
            try:
                body = response.json()
            except ValueError:
                body = None
        version = body.get("api_version") if isinstance(body, dict) else None
        if (
            not isinstance(body, dict)
            or body.get("api") != API_NAME
            or type(version) is not int
            or version != API_VERSION
        ):
            return "not_llm", "The job does not answer as an LLM service.", None, None
        if body.get("caller_admin") is not True:
            return "no_access", NOT_ADMIN, None, None
        model, engine = body.get("model"), body.get("state")
        return (
            "ok",
            None,
            model if isinstance(model, str) else None,
            engine if isinstance(engine, str) else None,
        )
