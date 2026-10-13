"""DuckDB over the Parquet request logs stored in blob storage.

The admin app mirrors new Parquet files from blob into a local cache directory
and queries them with DuckDB. Sync is incremental (by blob name) and rate
limited by a TTL so UI refreshes stay cheap.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any

import duckdb
from datatailr import Blob
from datatailr.logging import DatatailrLogger

from llm_platform.common.naming import LOGS_PREFIX

log = DatatailrLogger(__name__).get_logger()


class LogStore:
    def __init__(self, prefix: str = LOGS_PREFIX, cache_dir: str = "/tmp/llm_platform_logs", ttl: float = 30.0):
        self._blob = Blob()
        self._prefix = prefix.rstrip("/") + "/"
        self._cache_dir = cache_dir
        self._ttl = ttl
        self._last_sync = 0.0
        self._known: set[str] = set()
        self._lock = threading.Lock()
        os.makedirs(self._cache_dir, exist_ok=True)
        self._con = duckdb.connect(database=":memory:")
        self._con.execute("SET TimeZone='UTC'")

    # ---- sync ------------------------------------------------------------
    def sync(self, force: bool = False) -> int:
        with self._lock:
            if not force and time.time() - self._last_sync < self._ttl:
                return 0
            new_files = 0
            try:
                entries = self._blob.ls(self._prefix, recursive=True)
            except Exception as exc:  # noqa: BLE001
                log.warning("Listing request logs failed: %s", exc)
                self._last_sync = time.time()
                return 0
            for entry in entries or []:
                name = entry.get("name") if isinstance(entry, dict) else str(entry)
                if not name or not name.endswith(".parquet"):
                    continue
                if not entry.get("is_file", True):
                    continue
                rel = name.split(self._prefix, 1)[-1] if self._prefix in name else name.lstrip("/")
                if rel in self._known:
                    continue
                local = os.path.join(self._cache_dir, rel)
                os.makedirs(os.path.dirname(local), exist_ok=True)
                if not os.path.exists(local):
                    try:
                        self._blob.get_file(name, local)
                    except Exception as exc:  # noqa: BLE001
                        log.warning("Download of %s failed: %s", name, exc)
                        continue
                self._known.add(rel)
                new_files += 1
            self._last_sync = time.time()
            return new_files

    def has_data(self) -> bool:
        return bool(self._known)

    @property
    def _glob(self) -> str:
        return os.path.join(self._cache_dir, "*", "*.parquet")

    def _source(self) -> str:
        return f"read_parquet('{self._glob}', hive_partitioning=true, union_by_name=true)"

    def query(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        self.sync()
        if not self.has_data():
            return []
        sql = sql.replace("{logs}", self._source())
        with self._lock:
            cur = self._con.execute(sql, params or [])
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        out = []
        for row in rows:
            item = {}
            for key, val in zip(cols, row):
                if hasattr(val, "isoformat"):
                    val = val.isoformat()
                item[key] = val
            out.append(item)
        return out

    # ---- canned queries --------------------------------------------------
    def list_requests(
        self,
        *,
        user: str | None = None,
        model: str | None = None,
        status: str | None = None,
        since: float | None = None,
        until: float | None = None,
        search: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        clauses, params = [], []
        if user:
            clauses.append('"user" = ?')
            params.append(user)
        if model:
            clauses.append("(model = ? OR deployment = ?)")
            params.extend([model, model])
        if status == "ok":
            clauses.append("status BETWEEN 200 AND 299")
        elif status == "error":
            clauses.append("(status < 200 OR status >= 300)")
        if since:
            clauses.append("ts >= to_timestamp(?)")
            params.append(since)
        if until:
            clauses.append("ts <= to_timestamp(?)")
            params.append(until)
        if search:
            clauses.append("(request_json ILIKE ? OR response_text ILIKE ? OR error ILIKE ?)")
            like = f"%{search}%"
            params.extend([like, like, like])
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        total = self.query(f"SELECT count(*) AS n FROM {{logs}} {where}", params)
        rows = self.query(
            f"""
            SELECT request_id, ts, "user", model, deployment, endpoint, stream, status, latency_ms,
                   prompt_tokens, completion_tokens, error, client,
                   left(coalesce(response_text, ''), 160) AS response_preview
            FROM {{logs}} {where}
            ORDER BY ts DESC LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        )
        return {"total": total[0]["n"] if total else 0, "items": rows}

    def get_request(self, request_id: str) -> dict[str, Any] | None:
        rows = self.query("SELECT * FROM {logs} WHERE request_id = ? LIMIT 1", [request_id])
        return rows[0] if rows else None

    def stats(self, since: float) -> dict[str, Any]:
        base = "FROM {logs} WHERE ts >= to_timestamp(?)"
        totals = self.query(
            f"""
            SELECT count(*) AS requests,
                   count(DISTINCT "user") AS users,
                   coalesce(sum(prompt_tokens), 0) AS prompt_tokens,
                   coalesce(sum(completion_tokens), 0) AS completion_tokens,
                   coalesce(avg(latency_ms), 0) AS avg_latency_ms,
                   sum(CASE WHEN status BETWEEN 200 AND 299 THEN 0 ELSE 1 END) AS errors
            {base}
            """,
            [since],
        )
        by_hour = self.query(
            f"""
            SELECT date_trunc('hour', ts) AS bucket, count(*) AS requests,
                   coalesce(sum(prompt_tokens), 0) + coalesce(sum(completion_tokens), 0) AS tokens
            {base} GROUP BY 1 ORDER BY 1
            """,
            [since],
        )
        by_model = self.query(
            f"""
            SELECT coalesce(nullif(deployment, ''), nullif(model, ''), '(unknown model)') AS model, count(*) AS requests,
                   coalesce(sum(prompt_tokens), 0) + coalesce(sum(completion_tokens), 0) AS tokens,
                   coalesce(avg(latency_ms), 0) AS avg_latency_ms
            {base} GROUP BY 1 ORDER BY 2 DESC
            """,
            [since],
        )
        by_user = self.query(
            f"""
            SELECT "user", count(*) AS requests,
                   coalesce(sum(prompt_tokens), 0) + coalesce(sum(completion_tokens), 0) AS tokens,
                   max(ts) AS last_seen,
                   count(DISTINCT coalesce(deployment, model)) AS models
            {base} GROUP BY 1 ORDER BY 2 DESC
            """,
            [since],
        )
        return {
            "totals": totals[0] if totals else {},
            "by_hour": by_hour,
            "by_model": by_model,
            "by_user": by_user,
        }

    def users(self) -> list[dict[str, Any]]:
        return self.query(
            """
            SELECT "user", count(*) AS requests,
                   coalesce(sum(prompt_tokens), 0) AS prompt_tokens,
                   coalesce(sum(completion_tokens), 0) AS completion_tokens,
                   min(ts) AS first_seen, max(ts) AS last_seen,
                   count(DISTINCT coalesce(deployment, model)) AS models,
                   sum(CASE WHEN status BETWEEN 200 AND 299 THEN 0 ELSE 1 END) AS errors
            FROM {logs} GROUP BY 1 ORDER BY requests DESC
            """
        )
