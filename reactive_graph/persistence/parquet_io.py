"""Parquet + Blob helpers shared by the persistence-sink, the dashboard
and the EOD / warm-up workflows.

Everything in the reactive-graph demo writes and reads through a tiny
abstraction over the platform's blob store:

* on Datatailr we use :class:`datatailr.Blob`;
* in local development (no ``datatailr`` package available) we fall back
  to a filesystem-backed mock rooted at ``REACTIVE_GRAPH_LOCAL_BLOB_DIR``
  (default ``/tmp/reactive_graph_blob``).

The on-disk layout under either backend is::

    reactive_graph/trades/dt=YYYY-MM-DD/HHMMSS-<seq>.parquet
    reactive_graph/positions/latest.parquet
    reactive_graph/positions/history/dt=YYYY-MM-DD/HHMMSS-<seq>.parquet

Fills are append-only, partitioned by date.  ``positions/latest.parquet``
is overwritten on every flush; ``positions/history/`` keeps the full
sequence of snapshots so the pre-market warm-up workflow can rebuild
yesterday's book.
"""

from __future__ import annotations

import glob
import io
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

log = logging.getLogger("reactive_graph.persistence")

DEFAULT_BLOB_ROOT = "reactive_graph"
TRADES_PREFIX = f"{DEFAULT_BLOB_ROOT}/trades"
POSITIONS_LATEST = f"{DEFAULT_BLOB_ROOT}/positions/latest.parquet"
POSITIONS_HISTORY_PREFIX = f"{DEFAULT_BLOB_ROOT}/positions/history"


# ---------------------------------------------------------------------------
# Blob backend
# ---------------------------------------------------------------------------


class _LocalFsBlob:
    """Filesystem-backed stand-in for :class:`datatailr.Blob`.

    Only implements the subset of the SDK the demo uses
    (``put``, ``get``, ``put_file``, ``get_file``, ``ls``, ``exists``,
    ``delete``).
    """

    def __init__(self, root: str) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _abs(self, path: str) -> Path:
        return self.root / path

    def put(self, path: str, data) -> None:
        if isinstance(data, str):
            data = data.encode("utf-8")
        p = self._abs(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def put_file(self, path: str, local: str) -> None:
        with open(local, "rb") as f:
            self.put(path, f.read())

    def get(self, path: str) -> bytes:
        return self._abs(path).read_bytes()

    def get_file(self, path: str, local: str) -> None:
        Path(local).parent.mkdir(parents=True, exist_ok=True)
        with open(local, "wb") as f:
            f.write(self.get(path))

    def ls(self, prefix: str) -> List[str]:
        base = self._abs(prefix)
        if not base.exists():
            return []
        if base.is_file():
            return [prefix]
        out: List[str] = []
        for p in base.rglob("*"):
            if p.is_file():
                out.append(str(p.relative_to(self.root)).replace(os.sep, "/"))
        return sorted(out)

    def exists(self, path: str) -> bool:
        return self._abs(path).exists()

    def delete(self, path: str) -> None:
        p = self._abs(path)
        if p.exists():
            p.unlink()


def _datatailr_cli_available() -> bool:
    """True iff the ``dt`` CLI is actually invocable on this machine.

    The Datatailr SDK is happy to import even when the CLI isn't present,
    but every Blob operation shells out to ``dt`` and would then raise at
    call time.  We probe up-front and gracefully fall back to the
    filesystem mock so local development stays usable.
    """
    try:
        from datatailr.wrapper import CLI_TOOL  # type: ignore[import-not-found]
        return CLI_TOOL is not None
    except Exception:  # noqa: BLE001
        return False


def get_blob():
    """Return the active blob backend (Datatailr or filesystem mock)."""
    if os.environ.get("REACTIVE_GRAPH_FORCE_LOCAL_BLOB", "").lower() in (
        "1", "true", "yes"
    ):
        root = os.environ.get(
            "REACTIVE_GRAPH_LOCAL_BLOB_DIR", "/tmp/reactive_graph_blob"
        )
        log.info("using local-fs blob at %s (forced by env)", root)
        return _LocalFsBlob(root)
    try:
        from datatailr import Blob  # type: ignore[import-not-found]
        if not _datatailr_cli_available():
            raise RuntimeError("datatailr CLI ('dt') not available")
        return Blob()
    except Exception as exc:  # noqa: BLE001
        root = os.environ.get(
            "REACTIVE_GRAPH_LOCAL_BLOB_DIR", "/tmp/reactive_graph_blob"
        )
        log.info("using local-fs blob at %s (datatailr unavailable: %s)", root, exc)
        return _LocalFsBlob(root)


# ---------------------------------------------------------------------------
# Parquet writers
# ---------------------------------------------------------------------------


def _pyarrow():
    import pyarrow as pa  # type: ignore[import-not-found]
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    return pa, pq


FILLS_SCHEMA_FIELDS = (
    ("order_id", "string"),
    ("symbol", "string"),
    ("side", "string"),
    ("qty", "int64"),
    ("price", "float64"),
    ("slippage", "float64"),
    ("strategy", "string"),
    ("at", "float64"),
    ("correlation_id", "string"),
)

POSITIONS_SCHEMA_FIELDS = (
    ("symbol", "string"),
    ("net_qty", "int64"),
    ("avg_price", "float64"),
    ("market_price", "float64"),
    ("realised_pnl", "float64"),
    ("unrealised_pnl", "float64"),
    ("at", "float64"),
)


def _build_schema(fields):
    pa, _pq = _pyarrow()
    type_map = {
        "string": pa.string(),
        "int64": pa.int64(),
        "float64": pa.float64(),
    }
    return pa.schema([(name, type_map[ty]) for name, ty in fields])


def fills_to_parquet_bytes(fills: List[Dict[str, Any]]) -> bytes:
    """Serialise a list of fill dicts into a Parquet payload."""
    pa, pq = _pyarrow()
    rows = [
        {
            "order_id": str(f.get("order_id") or ""),
            "symbol": str(f.get("symbol") or ""),
            "side": str(f.get("side") or ""),
            "qty": int(f.get("qty") or 0),
            "price": float(f.get("price") or 0.0),
            "slippage": float(f.get("slippage") or 0.0),
            "strategy": str(f.get("strategy") or ""),
            "at": float(f.get("at") or 0.0),
            "correlation_id": str(f.get("correlation_id") or ""),
        }
        for f in fills
    ]
    table = pa.Table.from_pylist(rows, schema=_build_schema(FILLS_SCHEMA_FIELDS))
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def positions_to_parquet_bytes(positions: Dict[str, Dict[str, Any]]) -> bytes:
    """Serialise a {symbol: stats} snapshot into a Parquet payload."""
    pa, pq = _pyarrow()
    rows: List[Dict[str, Any]] = []
    for sym, p in positions.items():
        rows.append({
            "symbol": str(sym),
            "net_qty": int(p.get("net_qty") or 0),
            "avg_price": float(p.get("avg_price") or 0.0),
            "market_price": float(p.get("market_price") or 0.0),
            "realised_pnl": float(p.get("realised_pnl") or 0.0),
            "unrealised_pnl": float(p.get("unrealised_pnl") or 0.0),
            "at": float(p.get("at") or 0.0),
        })
    table = pa.Table.from_pylist(
        rows, schema=_build_schema(POSITIONS_SCHEMA_FIELDS)
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Read helpers (DuckDB)
# ---------------------------------------------------------------------------


def _normalise_ls_entries(entries: Iterable[Any], prefix: str) -> List[str]:
    """Coerce a heterogeneous ``blob.ls()`` result into full path strings.

    The two backends we support disagree on the shape of ``ls()``:

    * :class:`_LocalFsBlob.ls` returns ``List[str]`` of full relative paths.
    * :class:`datatailr.Blob.ls` returns ``List[dict]`` with ``name``
      (relative to *prefix*), ``is_file``, ``last_modified`` and ``size``.

    This helper normalises both into a flat list of full paths (no
    ``blob://`` scheme), prepending *prefix* when the entry is a bare
    basename.
    """
    norm_prefix = prefix.rstrip("/") + "/"
    out: List[str] = []
    for entry in entries:
        if isinstance(entry, dict):
            if entry.get("is_file") is False:
                continue
            name = entry.get("name") or entry.get("path") or ""
        else:
            name = str(entry)
        if not name:
            continue
        if name.startswith("blob://"):
            name = name[len("blob://"):]
        if not name.startswith(norm_prefix):
            name = norm_prefix + name.lstrip("/")
        out.append(name)
    return out


def list_parquet_files(blob, prefix: str) -> List[str]:
    """Return every blob key under *prefix* that ends in ``.parquet``.

    Works against both :class:`_LocalFsBlob` and :class:`datatailr.Blob`
    by normalising the heterogeneous ``ls()`` return shapes.
    """
    entries: List[Any] = []
    try:
        entries = list(blob.ls(prefix))
    except Exception as exc:  # noqa: BLE001
        log.warning("blob.ls(%s) failed: %s", prefix, exc)
        return []
    paths = _normalise_ls_entries(entries, prefix)
    return sorted({p for p in paths if p.endswith(".parquet")})


def today_partition(prefix: str, date: Optional[str] = None) -> str:
    """Path of today's ``dt=YYYY-MM-DD/`` partition under *prefix*."""
    import time as _t
    if date is None:
        date = _t.strftime("%Y-%m-%d", _t.gmtime())
    return f"{prefix}/dt={date}/"


def _materialise_locally(
    blob, paths: Iterable[str], tmpdir: str
) -> Dict[str, Any]:
    """Download *paths* into *tmpdir*.

    Returns ``{"local_files": [...], "errors": [{"path", "error"}, ...]}``
    so callers can both consume the local files and surface why downloads
    failed (network, permissions, path normalisation, ...).
    """
    local_files: List[str] = []
    errors: List[Dict[str, str]] = []
    for i, p in enumerate(paths):
        local = os.path.join(tmpdir, f"{i:06d}.parquet")
        try:
            blob.get_file(p, local)
            local_files.append(local)
        except Exception as exc:  # noqa: BLE001
            log.warning("could not download %s: %s", p, exc)
            errors.append({"path": p, "error": str(exc)})
    return {"local_files": local_files, "errors": errors}


def _empty_view_sql(view_name: str, schema_fields) -> str:
    """SQL that creates a properly-typed empty view.

    DuckDB queries that reference specific columns blow up with a
    ``Binder Error`` when the view is built with no schema (the previous
    ``SELECT NULL WHERE FALSE`` trick), so we instead emit a typed CTE
    that guarantees every expected column exists.
    """
    duck_types = {
        "string": "VARCHAR",
        "int64": "BIGINT",
        "float64": "DOUBLE",
    }
    cols = ", ".join(
        f"NULL::{duck_types[ty]} AS {name}" for name, ty in schema_fields
    )
    return (
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT {cols} WHERE FALSE"
    )


def _view_schema_for(view_name: str):
    if view_name == "trades":
        return FILLS_SCHEMA_FIELDS
    if view_name in ("positions", "positions_history"):
        return POSITIONS_SCHEMA_FIELDS
    return None


def duckdb_query(
    sql: str,
    *,
    blob=None,
    views: Optional[Dict[str, List[str]]] = None,
    params: Optional[List[Any]] = None,
    schemas: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Run *sql* in a fresh DuckDB connection.

    *views* maps a view name to the list of blob paths that should make
    up that view; each list is downloaded to a tempdir and registered
    via ``CREATE VIEW <name> AS SELECT * FROM read_parquet([...])``.

    When the path list is empty (or every download fails) the view is
    still registered, but as a properly-typed empty view derived from
    *schemas* (or the built-in defaults for ``trades`` / ``positions``)
    so callers can ``SELECT specific_column FROM <view>`` without ever
    hitting a Binder Error.
    """
    import duckdb  # type: ignore[import-not-found]

    blob = blob if blob is not None else get_blob()
    schemas = schemas or {}
    con = duckdb.connect()
    tmpdirs: List[str] = []
    try:
        for view_name, paths in (views or {}).items():
            tmp = tempfile.mkdtemp(prefix=f"rg_{view_name}_")
            tmpdirs.append(tmp)
            mat = _materialise_locally(blob, paths, tmp)
            files = mat["local_files"]
            if files:
                files_json = json.dumps(files)
                con.execute(
                    f"CREATE OR REPLACE VIEW {view_name} AS "
                    f"SELECT * FROM read_parquet({files_json}, "
                    "union_by_name=true)"
                )
            else:
                schema = schemas.get(view_name) or _view_schema_for(view_name)
                if schema is not None:
                    con.execute(_empty_view_sql(view_name, schema))
                else:
                    con.execute(
                        f"CREATE OR REPLACE VIEW {view_name} AS SELECT NULL "
                        "WHERE FALSE"
                    )

        cur = con.execute(sql, params) if params else con.execute(sql)
        cols = [d[0] for d in (cur.description or [])]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        con.close()
        for d in tmpdirs:
            shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# High-level convenience queries used by the dashboard + workflows
# ---------------------------------------------------------------------------


def today_trades_paths(blob, date: Optional[str] = None) -> List[str]:
    return list_parquet_files(blob, today_partition(TRADES_PREFIX, date))


def latest_positions_paths(blob) -> List[str]:
    return [POSITIONS_LATEST] if blob.exists(POSITIONS_LATEST) else []


def positions_history_paths(
    blob, date: Optional[str] = None
) -> List[str]:
    return list_parquet_files(
        blob, today_partition(POSITIONS_HISTORY_PREFIX, date)
    )


def _materialise_views(
    blob, view_paths: Dict[str, List[str]]
) -> Dict[str, Dict[str, Any]]:
    """Download every path listed in *view_paths* into temporary files.

    Returns ``{view_name: {"local_files": [...], "errors": [...], "tmpdir"}}``
    so callers can wire the files into DuckDB and clean up tempdirs.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for view_name, paths in view_paths.items():
        tmp = tempfile.mkdtemp(prefix=f"rg_{view_name}_")
        mat = _materialise_locally(blob, paths, tmp)
        mat["tmpdir"] = tmp
        out[view_name] = mat
    return out


def history_summary(blob=None, date: Optional[str] = None) -> Dict[str, Any]:
    """Aggregate today's fills + latest positions via DuckDB.

    Returns a small dict the dashboard renders directly.  Always includes
    diagnostic fields (``trades_prefix``, ``positions_prefix``,
    ``blob_backend``, sample paths, download error counts) so an empty
    result can be explained at a glance.
    """
    import duckdb  # type: ignore[import-not-found]

    blob = blob if blob is not None else get_blob()
    trades_prefix = today_partition(TRADES_PREFIX, date)
    trades_paths = today_trades_paths(blob, date)
    positions_paths = latest_positions_paths(blob)

    out: Dict[str, Any] = {
        "blob_backend": type(blob).__name__,
        "trades_prefix": trades_prefix,
        "positions_prefix": POSITIONS_LATEST,
        "trades_files": len(trades_paths),
        "positions_files": len(positions_paths),
        "sample_trade_paths": trades_paths[:3],
        "sample_position_paths": positions_paths[:3],
        "totals": {"fill_count": 0, "notional": 0.0, "slippage_cost": 0.0},
        "by_symbol": [],
        "by_strategy": [],
        "recent": [],
        "positions": [],
    }

    materialised = _materialise_views(blob, {
        "trades": trades_paths,
        "positions": positions_paths,
    })
    trades_local = materialised["trades"]["local_files"]
    positions_local = materialised["positions"]["local_files"]
    trades_errors = materialised["trades"]["errors"]
    positions_errors = materialised["positions"]["errors"]

    out["trades_downloaded"] = len(trades_local)
    out["positions_downloaded"] = len(positions_local)
    if trades_errors:
        out["trades_download_errors"] = trades_errors[:5]
    if positions_errors:
        out["positions_download_errors"] = positions_errors[:5]

    try:
        con = duckdb.connect()
        try:
            if trades_local:
                con.execute(
                    "CREATE OR REPLACE VIEW trades AS "
                    f"SELECT * FROM read_parquet({json.dumps(trades_local)}, "
                    "union_by_name=true)"
                )
            else:
                con.execute(_empty_view_sql("trades", FILLS_SCHEMA_FIELDS))
            if positions_local:
                con.execute(
                    "CREATE OR REPLACE VIEW positions AS "
                    f"SELECT * FROM read_parquet({json.dumps(positions_local)}, "
                    "union_by_name=true)"
                )
            else:
                con.execute(_empty_view_sql("positions", POSITIONS_SCHEMA_FIELDS))

            totals_rows = con.execute(
                "SELECT COUNT(*) AS fill_count, "
                "COALESCE(SUM(qty * price), 0) AS notional, "
                "COALESCE(SUM(ABS(slippage * qty)), 0) AS slippage_cost "
                "FROM trades"
            ).fetchall()
            if totals_rows:
                cols = ["fill_count", "notional", "slippage_cost"]
                out["totals"] = dict(zip(cols, totals_rows[0]))

            cur = con.execute(
                "SELECT symbol, "
                "SUM(CASE WHEN side='buy' THEN qty ELSE -qty END) AS net_qty, "
                "SUM(qty) AS total_qty, COUNT(*) AS fills, "
                "AVG(price) AS avg_price "
                "FROM trades GROUP BY symbol ORDER BY total_qty DESC"
            )
            cols = [d[0] for d in (cur.description or [])]
            out["by_symbol"] = [dict(zip(cols, r)) for r in cur.fetchall()]

            cur = con.execute(
                "SELECT COALESCE(strategy,'unknown') AS strategy, "
                "COUNT(*) AS fills, SUM(qty) AS qty "
                "FROM trades GROUP BY strategy ORDER BY fills DESC"
            )
            cols = [d[0] for d in (cur.description or [])]
            out["by_strategy"] = [dict(zip(cols, r)) for r in cur.fetchall()]

            cur = con.execute(
                'SELECT symbol, side, qty, price, slippage, strategy, '
                '"at" AS ts FROM trades ORDER BY "at" DESC LIMIT 20'
            )
            cols = [d[0] for d in (cur.description or [])]
            out["recent"] = [dict(zip(cols, r)) for r in cur.fetchall()]

            cur = con.execute(
                "SELECT symbol, net_qty, avg_price, market_price, "
                'realised_pnl, unrealised_pnl, "at" AS ts '
                "FROM positions ORDER BY symbol"
            )
            cols = [d[0] for d in (cur.description or [])]
            out["positions"] = [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        log.exception("history_summary duckdb failure")
        out["error"] = str(exc)
    finally:
        for v in materialised.values():
            shutil.rmtree(v.get("tmpdir") or "", ignore_errors=True)
    return out
