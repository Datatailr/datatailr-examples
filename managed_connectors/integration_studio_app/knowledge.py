"""Durable knowledge store for shared Slack and HubSpot connector data.

SQLite is the source of truth for normalized documents, sync cursors, and
ingestion runs. FTS5 supports bounded lexical lookup for maintenance and tests;
the Connector Gateway serves capability-specific rows directly from SQLite.
Gmail, Outlook, and Zoom bypass this store entirely and are fetched from their
providers only for the duration of a signed-in user's request.

The boundary is intentionally small: a production installation can replace the
local store with a dedicated database without changing any connector.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable


INDEX_ROOT = Path(os.environ.get("INTEGRATION_STUDIO_INDEX", "/mnt/integration-studio/knowledge"))
SQLITE_PATH = INDEX_ROOT / "knowledge.sqlite3"
MAX_CONTEXT_CHARS = int(os.environ.get("INTEGRATION_STUDIO_CONTEXT_CHARS", "60000"))
MAX_PASSAGE_CHARS = int(os.environ.get("INTEGRATION_STUDIO_PASSAGE_CHARS", "8000"))
_WORD = re.compile(r"[A-Za-z0-9_@.#-]{2,}")
_RECENT = re.compile(r"\b(recent|recently|latest|today|yesterday|this week|last week|newest)\b", re.I)


@dataclass(slots=True)
class KnowledgeDocument:
    source: str
    scope: str
    external_id: str
    title: str
    content: str
    ref: str
    source_updated_at: str
    acl_users: list[str] = field(default_factory=list)
    acl_groups: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def id(self) -> str:
        value = f"{self.source}\0{self.scope}\0{self.external_id}".encode()
        return hashlib.sha256(value).hexdigest()

    @property
    def content_hash(self) -> str:
        payload = json.dumps(
            {
                "title": self.title,
                "content": self.content,
                "ref": self.ref,
                "updated": self.source_updated_at,
                "users": sorted(set(self.acl_users)),
                "groups": sorted(set(self.acl_groups)),
                "metadata": self.metadata,
            },
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode()).hexdigest()


@dataclass(slots=True)
class SearchHit:
    id: str
    source: str
    title: str
    content: str
    ref: str
    source_updated_at: str
    metadata: dict[str, Any]
    score: float


@dataclass(slots=True)
class SearchResult:
    hits: list[SearchHit]
    vector_used: bool
    note: str | None = None

    def context(self) -> str:
        parts: list[str] = []
        size = 0
        for index, hit in enumerate(self.hits, 1):
            passage = hit.content.strip()
            if len(passage) > MAX_PASSAGE_CHARS:
                passage = passage[:MAX_PASSAGE_CHARS].rstrip() + "\n[passage truncated]"
            block = (
                f"[S{index}] {hit.title}\n"
                f"Source: {hit.source} | Updated: {hit.source_updated_at} | Ref: {hit.ref}\n"
                f"{passage}"
            )
            if size + len(block) > MAX_CONTEXT_CHARS:
                remaining = MAX_CONTEXT_CHARS - size
                if remaining > 400:
                    parts.append(block[:remaining])
                break
            parts.append(block)
            size += len(block) + 2
        return "\n\n".join(parts)


class KnowledgeIndex:
    def __init__(self, sqlite_path: Path = SQLITE_PATH):
        self.sqlite_path = sqlite_path
        self._lock = threading.RLock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(self.sqlite_path.parent, 0o700)
        conn = sqlite3.connect(self.sqlite_path, timeout=30)
        os.chmod(self.sqlite_path, 0o600)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _harden_permissions(self) -> None:
        """The index contains raw connector text, so mount defaults are too broad."""
        if not INDEX_ROOT.exists() and self.sqlite_path.parent != INDEX_ROOT:
            root = self.sqlite_path.parent
        else:
            root = INDEX_ROOT
        if not root.exists():
            return
        for current, directories, files in os.walk(root):
            try:
                os.chmod(current, 0o700)
            except OSError:
                pass
            for name in directories:
                try:
                    os.chmod(Path(current) / name, 0o700)
                except OSError:
                    pass
            for name in files:
                try:
                    os.chmod(Path(current) / name, 0o600)
                except OSError:
                    pass

    @contextmanager
    def _db(self):
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        with self._lock, self._db() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    ref TEXT NOT NULL,
                    source_updated_at TEXT NOT NULL,
                    metadata TEXT NOT NULL DEFAULT '{}',
                    content_hash TEXT NOT NULL,
                    seen_run TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(source, scope, external_id)
                );
                CREATE INDEX IF NOT EXISTS documents_source_scope_idx ON documents(source, scope);
                CREATE INDEX IF NOT EXISTS documents_updated_idx ON documents(source_updated_at DESC);

                CREATE TABLE IF NOT EXISTS document_acl (
                    document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
                    principal TEXT NOT NULL,
                    PRIMARY KEY(document_id, principal)
                );
                CREATE INDEX IF NOT EXISTS document_acl_principal_idx ON document_acl(principal, document_id);

                CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
                    id UNINDEXED, title, content, tokenize='porter unicode61'
                );

                CREATE TABLE IF NOT EXISTS sync_cursors (
                    source TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    cursor_key TEXT NOT NULL,
                    cursor_value TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(source, scope, cursor_key)
                );

                CREATE TABLE IF NOT EXISTS ingestion_runs (
                    id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    fetched INTEGER NOT NULL DEFAULT 0,
                    changed INTEGER NOT NULL DEFAULT 0,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    detail TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS ingestion_runs_latest_idx
                    ON ingestion_runs(source, scope, started_at DESC);
                """
            )

    @staticmethod
    def principals(user: str, groups: Iterable[str]) -> list[str]:
        return [f"user:{user}", *(f"group:{g}" for g in sorted(set(groups)))]

    def cursor(self, source: str, scope: str, key: str) -> str | None:
        with self._db() as conn:
            row = conn.execute(
                "SELECT cursor_value FROM sync_cursors WHERE source=? AND scope=? AND cursor_key=?",
                (source, scope, key),
            ).fetchone()
            return str(row[0]) if row else None

    def set_cursor(self, source: str, scope: str, key: str, value: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock, self._db() as conn:
            conn.execute(
                """INSERT INTO sync_cursors(source, scope, cursor_key, cursor_value, updated_at)
                   VALUES(?,?,?,?,?) ON CONFLICT(source, scope, cursor_key) DO UPDATE SET
                   cursor_value=excluded.cursor_value, updated_at=excluded.updated_at""",
                (source, scope, key, value, now),
            )

    def start_run(self, source: str, scope: str, mode: str) -> str:
        run_id = hashlib.sha256(f"{source}:{scope}:{time.time_ns()}".encode()).hexdigest()[:24]
        with self._lock, self._db() as conn:
            conn.execute(
                """UPDATE ingestion_runs SET status='interrupted', finished_at=?,
                          detail='superseded by a retry after the previous worker stopped'
                   WHERE source=? AND scope=? AND status='running'""",
                (datetime.now(UTC).isoformat(), source, scope),
            )
            conn.execute(
                "INSERT INTO ingestion_runs(id,source,scope,mode,status,started_at) VALUES(?,?,?,?,?,?)",
                (run_id, source, scope, mode, "running", datetime.now(UTC).isoformat()),
            )
        return run_id

    def finish_run(
        self,
        run_id: str,
        status: str,
        *,
        fetched: int = 0,
        changed: int = 0,
        deleted: int = 0,
        detail: str = "",
    ) -> None:
        with self._lock, self._db() as conn:
            conn.execute(
                """UPDATE ingestion_runs SET status=?, finished_at=?, fetched=?, changed=?,
                   deleted=?, detail=? WHERE id=?""",
                (
                    status,
                    datetime.now(UTC).isoformat(),
                    fetched,
                    changed,
                    deleted,
                    detail[:1000],
                    run_id,
                ),
            )

    def upsert_documents(
        self,
        documents: list[KnowledgeDocument],
        settings: dict[str, Any],
        *,
        run_id: str,
        replace_scope: bool = False,
        source: str | None = None,
        scope: str | None = None,
    ) -> tuple[int, int, str]:
        """Upsert documents and optionally reconcile deletions for a full scan.

        Returns (changed, deleted, detail). Unchanged documents are stamped with
        the current run ID without rewriting their FTS rows.
        """
        now = datetime.now(UTC).isoformat()
        changed_docs: list[KnowledgeDocument] = []
        deleted_ids: list[str] = []
        with self._lock, self._db() as conn:
            for doc in documents:
                if not doc.content.strip():
                    continue
                existing = conn.execute(
                    "SELECT content_hash FROM documents WHERE id=?", (doc.id,)
                ).fetchone()
                changed = existing is None or existing[0] != doc.content_hash
                conn.execute(
                    """INSERT INTO documents(
                           id,source,scope,external_id,title,content,ref,source_updated_at,
                           metadata,content_hash,seen_run,created_at,updated_at
                       ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(id) DO UPDATE SET title=excluded.title, content=excluded.content,
                           ref=excluded.ref, source_updated_at=excluded.source_updated_at,
                           metadata=excluded.metadata, content_hash=excluded.content_hash,
                           seen_run=excluded.seen_run, updated_at=excluded.updated_at""",
                    (
                        doc.id,
                        doc.source,
                        doc.scope,
                        doc.external_id,
                        doc.title,
                        doc.content,
                        doc.ref,
                        doc.source_updated_at,
                        json.dumps(doc.metadata, ensure_ascii=False, separators=(",", ":")),
                        doc.content_hash,
                        run_id,
                        now,
                        now,
                    ),
                )
                conn.execute("DELETE FROM document_acl WHERE document_id=?", (doc.id,))
                principals = {
                    *(f"user:{value}" for value in doc.acl_users if value),
                    *(f"group:{value}" for value in doc.acl_groups if value),
                }
                conn.executemany(
                    "INSERT INTO document_acl(document_id, principal) VALUES(?,?)",
                    [(doc.id, principal) for principal in sorted(principals)],
                )
                if changed:
                    conn.execute("DELETE FROM documents_fts WHERE id=?", (doc.id,))
                    conn.execute(
                        "INSERT INTO documents_fts(id,title,content) VALUES(?,?,?)",
                        (doc.id, doc.title, doc.content),
                    )
                    changed_docs.append(doc)
            if replace_scope:
                resolved_source = source or (documents[0].source if documents else "")
                resolved_scope = scope or (documents[0].scope if documents else "")
                rows = conn.execute(
                    "SELECT id FROM documents WHERE source=? AND scope=? AND COALESCE(seen_run,'')<>?",
                    (resolved_source, resolved_scope, run_id),
                ).fetchall()
                deleted_ids = [str(row[0]) for row in rows]
                for doc_id in deleted_ids:
                    conn.execute("DELETE FROM documents_fts WHERE id=?", (doc_id,))
                conn.executemany("DELETE FROM documents WHERE id=?", [(doc_id,) for doc_id in deleted_ids])

        self._harden_permissions()
        return len(changed_docs), len(deleted_ids), ""

    def delete_external_ids(self, source: str, scope: str, external_ids: Iterable[str]) -> int:
        ids: list[str] = []
        with self._lock, self._db() as conn:
            for external_id in set(external_ids):
                rows = conn.execute(
                    "SELECT id FROM documents WHERE source=? AND scope=? AND external_id=?",
                    (source, scope, external_id),
                ).fetchall()
                for row in rows:
                    doc_id = str(row[0])
                    ids.append(doc_id)
                    conn.execute("DELETE FROM documents_fts WHERE id=?", (doc_id,))
                    conn.execute("DELETE FROM documents WHERE id=?", (doc_id,))
        return len(ids)

    def delete_scope(self, source: str, scope: str) -> int:
        """Remove every indexed document for a disconnected connector scope."""
        with self._lock, self._db() as conn:
            ids = [
                str(row[0])
                for row in conn.execute(
                    "SELECT id FROM documents WHERE source=? AND scope=?", (source, scope)
                ).fetchall()
            ]
            for doc_id in ids:
                conn.execute("DELETE FROM documents_fts WHERE id=?", (doc_id,))
            conn.execute("DELETE FROM documents WHERE source=? AND scope=?", (source, scope))
            conn.execute("DELETE FROM sync_cursors WHERE source=? AND scope=?", (source, scope))
        return len(ids)

    def purge_sources(self, sources: Iterable[str]) -> int:
        """Remove all legacy records for sources that have become live-only."""
        values = sorted({str(source) for source in sources if source})
        if not values:
            return 0
        marks = ",".join("?" for _ in values)
        with self._lock, self._db() as conn:
            conn.execute("PRAGMA secure_delete=ON")
            ids = [
                str(row[0])
                for row in conn.execute(
                    f"SELECT id FROM documents WHERE source IN ({marks})", values
                ).fetchall()
            ]
            conn.executemany("DELETE FROM documents_fts WHERE id=?", [(value,) for value in ids])
            conn.execute(f"DELETE FROM documents WHERE source IN ({marks})", values)
            conn.execute(f"DELETE FROM sync_cursors WHERE source IN ({marks})", values)
            conn.execute(f"DELETE FROM ingestion_runs WHERE source IN ({marks})", values)
        return len(ids)

    @staticmethod
    def _fts_query(query: str) -> str:
        terms = []
        for value in _WORD.findall(query):
            term = value.strip(".#-_")
            if term and term.casefold() not in {
                "the", "and", "for", "from", "with", "what", "in", "of", "to", "on",
                "is", "are", "be", "summarize", "summary",
                "recent", "recently", "latest", "discussion", "discussions", "slack",
                "gmail", "outlook", "zoom", "hubspot",
            }:
                terms.append(f'"{term.replace(chr(34), "")}"')
        return " OR ".join(dict.fromkeys(terms[:20]))

    def _authorized_ids(
        self, user: str, groups: Iterable[str], sources: list[str]
    ) -> list[str]:
        if not sources:
            return []
        shared = sorted(set(sources) & {"slack", "hubspot"})
        personal = sorted(set(sources) - {"slack", "hubspot"})
        clauses: list[str] = []
        parameters: list[str] = []
        if shared:
            marks = ",".join("?" for _ in shared)
            clauses.append(f"SELECT id FROM documents WHERE source IN ({marks})")
            parameters.extend(shared)
        principals = self.principals(user, groups)
        if personal and principals:
            source_marks = ",".join("?" for _ in personal)
            principal_marks = ",".join("?" for _ in principals)
            clauses.append(
                f"SELECT DISTINCT d.id FROM documents d JOIN document_acl a ON a.document_id=d.id "
                f"WHERE d.source IN ({source_marks}) AND a.principal IN ({principal_marks})"
            )
            parameters.extend([*personal, *principals])
        if not clauses:
            return []
        with self._db() as conn:
            return [str(row[0]) for row in conn.execute(" UNION ".join(clauses), parameters).fetchall()]

    def search(
        self,
        query: str,
        *,
        user: str,
        groups: Iterable[str],
        sources: list[str],
        settings: dict[str, Any],
        limit: int = 20,
        leg_limit: int = 60,
    ) -> SearchResult:
        authorized = self._authorized_ids(user, groups, sources)
        if not authorized:
            return SearchResult([], False, "No indexed documents are visible for the selected sources")
        ranks: list[tuple[list[str], float]] = []
        fts = self._fts_query(query)
        with self._db() as conn:
            if fts:
                marks = ",".join("?" for _ in authorized)
                rows = conn.execute(
                    f"""SELECT id FROM documents_fts WHERE documents_fts MATCH ?
                         AND id IN ({marks}) ORDER BY bm25(documents_fts) LIMIT ?""",
                    [fts, *authorized, leg_limit],
                ).fetchall()
                ranks.append(([str(row[0]) for row in rows], 1.0))
            if _RECENT.search(query):
                marks = ",".join("?" for _ in authorized)
                rows = conn.execute(
                    f"SELECT id FROM documents WHERE id IN ({marks}) ORDER BY source_updated_at DESC LIMIT ?",
                    [*authorized, leg_limit],
                ).fetchall()
                ranks.append(([str(row[0]) for row in rows], 3.0))

        if not ranks:
            # A natural-language query with no lexical hits still gets the latest
            # authorized material instead of an empty answer.
            with self._db() as conn:
                marks = ",".join("?" for _ in authorized)
                rows = conn.execute(
                    f"SELECT id FROM documents WHERE id IN ({marks}) ORDER BY source_updated_at DESC LIMIT ?",
                    [*authorized, leg_limit],
                ).fetchall()
                ranks.append(([str(row[0]) for row in rows], 1.0))

        scores: dict[str, float] = {}
        for ranking, weight in ranks:
            for position, doc_id in enumerate(ranking):
                scores[doc_id] = scores.get(doc_id, 0.0) + weight / (60 + position + 1)
        top_ids = [doc_id for doc_id, _ in sorted(scores.items(), key=lambda item: item[1], reverse=True)[:limit]]
        if not top_ids:
            return SearchResult([], False, "SQLite FTS/recency search")
        marks = ",".join("?" for _ in top_ids)
        with self._db() as conn:
            rows = conn.execute(
                f"""SELECT id,source,title,content,ref,source_updated_at,metadata
                     FROM documents WHERE id IN ({marks})""",
                top_ids,
            ).fetchall()
        by_id = {str(row["id"]): row for row in rows}
        hits = [
            SearchHit(
                id=doc_id,
                source=str(by_id[doc_id]["source"]),
                title=str(by_id[doc_id]["title"]),
                content=str(by_id[doc_id]["content"]),
                ref=str(by_id[doc_id]["ref"]),
                source_updated_at=str(by_id[doc_id]["source_updated_at"]),
                metadata=json.loads(str(by_id[doc_id]["metadata"] or "{}")),
                score=scores[doc_id],
            )
            for doc_id in top_ids
            if doc_id in by_id
        ]
        return SearchResult(hits, False, "SQLite FTS/recency search")

    def stats(self, user: str | None = None, groups: Iterable[str] = ()) -> dict[str, Any]:
        with self._db() as conn:
            sources = [dict(row) for row in conn.execute(
                """SELECT source,scope,count(*) documents,
                          max(source_updated_at) latest_document
                   FROM documents GROUP BY source,scope ORDER BY source,scope"""
            ).fetchall()]
            runs = [dict(row) for row in conn.execute(
                """SELECT source,scope,mode,status,started_at,finished_at,fetched,changed,deleted,detail
                   FROM ingestion_runs ORDER BY started_at DESC LIMIT 30"""
            ).fetchall()]
        if user is not None:
            # Shared connector operations are workspace-visible. Personal
            # mailbox scopes are usernames and must not leak—even as counts or
            # sync metadata—to another signed-in user or an administrator.
            sources = [
                row
                for row in sources
                if row["source"] in {"slack", "hubspot"} or row["scope"] == user
            ]
            runs = [
                row
                for row in runs
                if row["source"] in {"slack", "hubspot"} or row["scope"] == user
            ]
            visible: dict[str, int] = {}
            for source in {row["source"] for row in sources}:
                visible[source] = len(self._authorized_ids(user, groups, [source]))
        else:
            visible = {}
        return {"sources": sources, "runs": runs, "visible": visible}


_INDEX: KnowledgeIndex | None = None
_INDEX_LOCK = threading.Lock()


def get_index() -> KnowledgeIndex:
    global _INDEX
    with _INDEX_LOCK:
        if _INDEX is None:
            _INDEX = KnowledgeIndex()
        return _INDEX
