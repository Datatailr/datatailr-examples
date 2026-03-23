"""
Discover and query Hive-partitioned Parquet in blob storage.

All I/O goes through the Datatailr ``Blob`` client (``dt blob ls`` / ``dt blob get``).
"""

from __future__ import annotations

import io
import logging
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)


def _entry_name(entry: str | dict[str, Any]) -> str:
    if isinstance(entry, dict):
        return str(entry.get("name") or "").strip()
    return str(entry).strip()


def _entry_is_file(entry: str | dict[str, Any]) -> bool | None:
    if isinstance(entry, dict) and "is_file" in entry:
        return bool(entry["is_file"])
    return None


def normalize_key_path(path: str) -> str:
    """Normalize blob object key to absolute style: /a/b/c (no trailing slash unless root)."""
    p = (path or "").strip()
    if not p or p == "/":
        return "/"
    p = "/" + p.strip("/")
    return p


def normalize_dir_prefix(path: str) -> str:
    """Normalize blob directory prefix to absolute style: /a/b/c/."""
    key = normalize_key_path(path)
    if key == "/":
        return "/"
    return key.rstrip("/") + "/"


def _join_key(prefix: str, segment: str) -> str:
    p = normalize_key_path(prefix)
    s = segment.strip().strip("/")
    if p == "/":
        return "/" + s if s else "/"
    if not s:
        return p
    return f"{p.rstrip('/')}/{s}"


def _full_key(list_parent: str, entry_name: str) -> str:
    """Join list parent with ls entry and return absolute key path."""
    raw = entry_name.strip()
    if not raw:
        return normalize_key_path(list_parent)
    if raw.startswith("/"):
        return normalize_key_path(raw)
    e = raw.strip("/")
    base = normalize_key_path(list_parent)
    if base == "/":
        return "/" + e
    if not e:
        return base
    base_tail = base.lstrip("/")
    if e == base_tail or e.startswith(base_tail + "/"):
        return "/" + e
    return f"{base.rstrip('/')}/{e}"


def _relative_under_prefix(list_prefix: str, full_key: str) -> str:
    base = normalize_key_path(list_prefix)
    fk = normalize_key_path(full_key)
    if base == "/":
        return fk.lstrip("/")
    if fk == base:
        return ""
    p = base.rstrip("/") + "/"
    if fk.startswith(p):
        return fk[len(p) :]
    return ""


def _list_dir(blob: Any, prefix: str) -> list[str | dict[str, Any]]:
    p = normalize_dir_prefix(prefix)
    out = blob.ls(p)
    if not isinstance(out, list):
        return list(out) if out else []
    return out


def _collect_immediate_children(
    list_prefix: str,
    entries: list[str | dict[str, Any]],
) -> tuple[list[str], list[str]]:
    """Return immediate child directories and files under list_prefix."""
    dirs: set[str] = set()
    files: dict[str, str] = {}

    for entry in entries:
        name = _entry_name(entry)
        if not name:
            continue
        full = _full_key(list_prefix, name).rstrip("/")
        rel = _relative_under_prefix(list_prefix, full)
        if not rel:
            continue
        parts = [p for p in rel.split("/") if p]
        if not parts:
            continue
        head = parts[0]
        nested = len(parts) > 1
        is_file = _entry_is_file(entry)
        is_dir = nested or is_file is False
        if is_dir:
            dirs.add(head)
            continue
        if head not in files:
            files[head] = _join_key(list_prefix, head)

    for d in dirs:
        files.pop(d, None)
    return sorted(dirs), [files[k] for k in sorted(files)]


def _list_immediate(blob: Any, prefix: str) -> tuple[list[str], list[str]]:
    return _collect_immediate_children(prefix, _list_dir(blob, prefix))


def iter_parquet_keys(
    blob: Any,
    root_prefix: str,
    *,
    max_files: int | None = None,
) -> Iterator[str]:
    """
    Yield blob keys ending in ``.parquet`` under ``root_prefix``.

    Uses non-recursive ls() directory walk to avoid broad recursive listings.
    """
    root = normalize_key_path(root_prefix)
    if root == "/":
        return

    yielded = 0
    stack = [root]
    seen: set[str] = set()

    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)

        try:
            dirs, files = _list_immediate(blob, cur)
        except Exception as exc:
            log.debug("ls %s failed: %s", cur, exc)
            continue

        for key in files:
            if not key.endswith(".parquet"):
                continue
            yield key
            yielded += 1
            if max_files is not None and yielded >= max_files:
                return

        for d in reversed(dirs):
            stack.append(_join_key(cur, d))


def _dataset_root(base_prefix: str, dataset: str) -> str:
    ds = dataset.strip().strip("/")
    if not ds:
        raise ValueError("dataset is required")
    return normalize_key_path(_join_key(base_prefix, ds))


def iter_dataset_parquet_keys(
    blob: Any,
    base_prefix: str,
    dataset: str,
    *,
    dt: str | None = None,
    hour: str | None = None,
    max_files: int | None = None,
) -> Iterator[str]:
    """
    Yield parquet keys from one dataset with optional partition pruning.

    Layout assumed:
      {base}/{dataset}/dt=YYYY-MM-DD/hour=HH/ticker=<TICKER>/part-*.parquet

    Also supports legacy files directly under hour partition:
      {base}/{dataset}/dt=YYYY-MM-DD/hour=HH/part-*.parquet
    """
    root = _dataset_root(base_prefix, dataset)
    yielded = 0

    try:
        root_dirs, root_files = _list_immediate(blob, root)
    except Exception as exc:
        raise FileNotFoundError(f"Failed to list dataset root {root!r}: {exc}") from exc

    for key in root_files:
        if key.endswith(".parquet"):
            yield key
            yielded += 1
            if max_files is not None and yielded >= max_files:
                return

    if dt:
        dt_dirs = [f"dt={dt}"]
    else:
        dt_dirs = [d for d in root_dirs if d.startswith("dt=")]

    for dt_dir in sorted(dt_dirs):
        dt_prefix = _join_key(root, dt_dir)
        try:
            hour_dirs, dt_files = _list_immediate(blob, dt_prefix)
        except Exception:
            continue

        for key in dt_files:
            if key.endswith(".parquet"):
                yield key
                yielded += 1
                if max_files is not None and yielded >= max_files:
                    return

        if hour:
            hour_scan = [f"hour={hour}"]
        else:
            hour_scan = [h for h in hour_dirs if h.startswith("hour=")]

        for hour_dir in sorted(hour_scan):
            hour_prefix = _join_key(dt_prefix, hour_dir)
            try:
                hour_subdirs, hour_files = _list_immediate(blob, hour_prefix)
            except Exception:
                continue
            # Legacy layout support: files directly under hour partition.
            for key in hour_files:
                if not key.endswith(".parquet"):
                    continue
                yield key
                yielded += 1
                if max_files is not None and yielded >= max_files:
                    return
            # Current layout: descend one level into ticker=... partition folders.
            for ticker_dir in sorted(d for d in hour_subdirs if d.startswith("ticker=")):
                ticker_prefix = _join_key(hour_prefix, ticker_dir)
                try:
                    _nested, ticker_files = _list_immediate(blob, ticker_prefix)
                except Exception:
                    continue
                for key in ticker_files:
                    if not key.endswith(".parquet"):
                        continue
                    yield key
                    yielded += 1
                    if max_files is not None and yielded >= max_files:
                        return


def get_blob_bytes(blob: Any, key: str) -> bytes:
    """Read object bytes via ``get_blob`` or ``get``."""
    if hasattr(blob, "get_blob"):
        return blob.get_blob(key)
    if hasattr(blob, "get"):
        return blob.get(key)
    raise TypeError("Blob client has no get_blob/get")


def load_parquet_keys_arrow(
    blob: Any,
    keys: list[str],
    *,
    max_rows_per_file: int | None = None,
) -> pa.Table:
    """Download keys and concatenate into one Arrow table (in memory)."""
    tables: list[pa.Table] = []
    for key in keys:
        raw = get_blob_bytes(blob, key)
        t = pq.read_table(io.BytesIO(raw))
        if max_rows_per_file is not None and t.num_rows > max_rows_per_file:
            t = t.slice(0, max_rows_per_file)
        tables.append(t)
    if not tables:
        return pa.table({})
    try:
        return pa.concat_tables(tables, promote_options="default")
    except TypeError:
        return pa.concat_tables(tables, promote=True)


def query_lake_sql(
    blob: Any,
    base_prefix: str,
    sql: str,
    *,
    dataset: str = "analytics",
    dt: str | None = None,
    hour: str | None = None,
    max_files: int | None = 64,
    hive_partitioning: bool = True,
    union_by_name: bool = False,
) -> pa.Table:
    """
    Run DuckDB SQL over one dataset in blob storage.

    Only lists and downloads keys under the selected dataset/partition path.
    """
    import duckdb

    root = _dataset_root(base_prefix, dataset)
    keys = list(
        iter_dataset_parquet_keys(
            blob,
            base_prefix,
            dataset,
            dt=dt,
            hour=hour,
            max_files=max_files,
        )
    )
    if not keys:
        raise FileNotFoundError(
            "No .parquet keys found for "
            f"base={normalize_key_path(base_prefix)!r}, dataset={dataset!r}, dt={dt!r}, hour={hour!r}"
        )

    with tempfile.TemporaryDirectory(prefix="lake_query_") as td:
        base = Path(td)
        for key in keys:
            rel = key[len(root) + 1 :] if key.startswith(root + "/") else key
            out = base / rel
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(get_blob_bytes(blob, key))

        paths = [str(p) for p in base.rglob("*.parquet")]
        con = duckdb.connect(":memory:")
        fp_kwargs: dict[str, Any] = {
            "hive_partitioning": hive_partitioning,
        }
        if union_by_name:
            fp_kwargs["union_by_name"] = True
        con.from_parquet(paths, **fp_kwargs).create_view("lake", replace=True)
        rel = con.execute(sql)
        if hasattr(rel, "to_arrow_table"):
            return rel.to_arrow_table()
        return rel.fetch_arrow_table()


def default_blob_prefix() -> str:
    return normalize_key_path(os.environ.get("COLLECTOR_BLOB_PREFIX", "stock_price_lake"))
