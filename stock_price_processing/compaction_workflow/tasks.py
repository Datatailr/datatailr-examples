from __future__ import annotations

import hashlib
import io
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from datatailr import task

from stock_price_processing.lake_query.reader import normalize_dir_prefix, normalize_key_path

DEFAULT_DATASETS = ("analytics", "market_events")
DEFAULT_LAST_N_HOURS = int(os.environ.get("COMPACTION_LAST_N_HOURS", "24"))
DEFAULT_MIN_FILES = int(os.environ.get("COMPACTION_MIN_FILES", "3"))
DEFAULT_BASE_PREFIX = os.environ.get("COLLECTOR_BLOB_PREFIX", "stock_price_lake")
MARKER_FILE = "_COMPACTED.json"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _safe_token(s: str) -> str:
    return "".join(ch if (ch.isalnum() or ch in ("_", "-", ".")) else "_" for ch in s)


def _list_entries(blob: Any, dir_prefix: str) -> list[str | dict[str, Any]]:
    out = blob.ls(normalize_dir_prefix(dir_prefix))
    if not isinstance(out, list):
        return list(out) if out else []
    return out


def _entry_name(entry: str | dict[str, Any]) -> str:
    if isinstance(entry, dict):
        return str(entry.get("name") or "").strip()
    return str(entry).strip()


def _entry_is_file(entry: str | dict[str, Any]) -> bool | None:
    if isinstance(entry, dict) and "is_file" in entry:
        return bool(entry["is_file"])
    return None


def _join_key(prefix: str, child: str) -> str:
    p = normalize_key_path(prefix).rstrip("/")
    c = child.strip("/")
    if not c:
        return p if p else "/"
    if not p or p == "/":
        return "/" + c
    return f"{p}/{c}"


def _full_key(parent_prefix: str, entry_name: str) -> str:
    raw = entry_name.strip()
    if not raw:
        return normalize_key_path(parent_prefix)
    if raw.startswith("/"):
        return normalize_key_path(raw)
    base = normalize_key_path(parent_prefix).rstrip("/")
    if not base:
        return "/" + raw.strip("/")
    tail = base.lstrip("/")
    child = raw.strip("/")
    if child == tail or child.startswith(tail + "/"):
        return "/" + child
    # Some Blob.ls() implementations return names relative to the bucket root:
    # parent_prefix: /bucket/a/b, entry: a/b/file.parquet (bucket omitted).
    # Reattach the bucket segment to avoid duplicating prefix segments.
    if "/" in tail:
        bucket, current_rel = tail.split("/", 1)
        if current_rel and (child == current_rel or child.startswith(current_rel + "/")):
            return f"/{bucket}/{child}"
    return f"{base}/{child}"


def _relative_under(prefix: str, full_key: str) -> str:
    p = normalize_key_path(prefix).rstrip("/")
    f = normalize_key_path(full_key).rstrip("/")
    if not p or p == "/":
        return f.lstrip("/")
    if f == p:
        return ""
    pref = p + "/"
    if f.startswith(pref):
        return f[len(pref) :]
    return ""


def _collect_immediate_children(prefix: str, entries: list[str | dict[str, Any]]) -> tuple[list[str], list[str]]:
    dirs: set[str] = set()
    files: dict[str, str] = {}
    for entry in entries:
        full = _full_key(prefix, _entry_name(entry))
        rel = _relative_under(prefix, full)
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
        files.setdefault(head, _join_key(prefix, head))
    for d in dirs:
        files.pop(d, None)
    return sorted(dirs), [files[k] for k in sorted(files)]


def _candidate_dt_hour_set(last_n_hours: int, now: datetime | None = None) -> set[tuple[str, str]]:
    t0 = now or _now_utc()
    out: set[tuple[str, str]] = set()
    for i in range(1, last_n_hours + 1):
        t = t0 - timedelta(hours=i)
        out.add((t.strftime("%Y-%m-%d"), t.strftime("%H")))
    return out


@dataclass(frozen=True)
class Partition:
    dataset: str
    dt: str
    hour: str
    ticker: str
    prefix: str


def list_candidate_partitions_impl(
    blob: Any,
    *,
    base_prefix: str,
    datasets: tuple[str, ...] = DEFAULT_DATASETS,
    last_n_hours: int = DEFAULT_LAST_N_HOURS,
) -> list[dict[str, str]]:
    """Discover dataset/dt/hour/ticker partitions in a recent sliding window."""
    base = normalize_key_path(base_prefix)
    dh_set = _candidate_dt_hour_set(last_n_hours)
    wanted_dates = {d for d, _h in dh_set}
    out: list[Partition] = []

    for dataset in datasets:
        ds_root = _join_key(base, dataset)
        try:
            dt_dirs, _files = _collect_immediate_children(ds_root, _list_entries(blob, ds_root))
        except Exception:
            continue
        for dt_dir in dt_dirs:
            if not dt_dir.startswith("dt="):
                continue
            d = dt_dir.split("=", 1)[1]
            if d not in wanted_dates:
                continue
            dt_prefix = _join_key(ds_root, dt_dir)
            try:
                hour_dirs, dt_files = _collect_immediate_children(dt_prefix, _list_entries(blob, dt_prefix))
            except Exception:
                continue
            for hour_dir in hour_dirs:
                if not hour_dir.startswith("hour="):
                    continue
                h = hour_dir.split("=", 1)[1]
                if (d, h) not in dh_set:
                    continue
                hour_prefix = _join_key(dt_prefix, hour_dir)
                try:
                    ticker_dirs, hour_files = _collect_immediate_children(hour_prefix, _list_entries(blob, hour_prefix))
                except Exception:
                    continue

                # legacy layout: parquet directly under hour partition
                if any(k.endswith(".parquet") for k in hour_files):
                    out.append(
                        Partition(
                            dataset=dataset,
                            dt=d,
                            hour=h,
                            ticker="LEGACY",
                            prefix=hour_prefix,
                        )
                    )
                for ticker_dir in ticker_dirs:
                    if not ticker_dir.startswith("ticker="):
                        continue
                    t = ticker_dir.split("=", 1)[1]
                    out.append(
                        Partition(
                            dataset=dataset,
                            dt=d,
                            hour=h,
                            ticker=t,
                            prefix=_join_key(hour_prefix, ticker_dir),
                        )
                    )

    # stable order for deterministic runs/logging
    out.sort(key=lambda p: (p.dataset, p.dt, p.hour, p.ticker, p.prefix))
    return [
        {
            "dataset": p.dataset,
            "dt": p.dt,
            "hour": p.hour,
            "ticker": p.ticker,
            "prefix": p.prefix,
        }
        for p in out
    ]


def _blob_exists(blob: Any, key: str) -> bool:
    try:
        return bool(blob.exists(key))
    except Exception:
        try:
            blob.get_blob(key)
            return True
        except Exception:
            return False


def _blob_get(blob: Any, key: str) -> bytes:
    if hasattr(blob, "get_blob"):
        return blob.get_blob(key)
    return blob.get(key)


def _blob_put(blob: Any, key: str, data: bytes) -> None:
    if hasattr(blob, "put_blob"):
        blob.put_blob(key, data)
    else:
        blob.put(key, data)


def _blob_delete(blob: Any, key: str) -> None:
    if hasattr(blob, "delete"):
        blob.delete(key)
    elif hasattr(blob, "rm"):
        blob.rm(key)
    else:
        raise TypeError("Blob client has no delete/rm")


def _output_key(part: dict[str, str]) -> str:
    suffix = f"dt={part['dt']}-hour={part['hour']}-ticker={_safe_token(part['ticker'])}"
    return _join_key(part["prefix"], f"compact-{suffix}.parquet")


def _marker_key(part: dict[str, str]) -> str:
    return _join_key(part["prefix"], MARKER_FILE)


def compact_partition_impl(
    blob: Any,
    part: dict[str, str],
    *,
    min_files: int = DEFAULT_MIN_FILES,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Idempotently compact one partition.

    Skip if marker+compacted file already exist, or if not enough source files.
    """
    prefix = part["prefix"]
    compact_key = _output_key(part)
    marker_key = _marker_key(part)

    _dirs, files = _collect_immediate_children(prefix, _list_entries(blob, prefix))
    parquet_files = sorted(k for k in files if k.endswith(".parquet"))
    source_files = [k for k in parquet_files if k != compact_key]

    marker_exists = _blob_exists(blob, marker_key)
    compact_exists = _blob_exists(blob, compact_key)
    if marker_exists and compact_exists and not source_files:
        return {
            "partition": part,
            "action": "skip_already_compacted",
            "source_file_count": len(source_files),
            "compact_key": compact_key,
            "marker_key": marker_key,
        }

    if not source_files and compact_exists:
        return {
            "partition": part,
            "action": "skip_compact_only",
            "source_file_count": 0,
            "compact_key": compact_key,
            "marker_key": marker_key,
        }

    # If a compacted file already exists, always fold in newly-arrived files
    # even when below the regular threshold, to keep one coherent schema/file.
    if not compact_exists and len(source_files) < min_files:
        return {
            "partition": part,
            "action": "skip_below_threshold",
            "source_file_count": len(source_files),
            "threshold": min_files,
            "compact_key": compact_key,
            "marker_key": marker_key,
        }

    if dry_run:
        merge_inputs = ([compact_key] if compact_exists else []) + source_files
        return {
            "partition": part,
            "action": "would_compact",
            "source_file_count": len(source_files),
            "merge_input_file_count": len(merge_inputs),
            "compact_key": compact_key,
            "marker_key": marker_key,
            "merge_candidates": merge_inputs,
            "delete_candidates": source_files,
        }

    merge_inputs = ([compact_key] if compact_exists else []) + source_files
    tables: list[pa.Table] = []
    for key in merge_inputs:
        raw = _blob_get(blob, key)
        tables.append(pq.read_table(io.BytesIO(raw)))
    merged = pa.concat_tables(tables, promote_options="default")

    buf = io.BytesIO()
    pq.write_table(merged, buf, compression="snappy")
    compact_bytes = buf.getvalue()
    _blob_put(blob, compact_key, compact_bytes)

    run_id = os.environ.get("DATATAILR_BATCH_RUN_ID", "")
    marker = {
        "dataset": part["dataset"],
        "dt": part["dt"],
        "hour": part["hour"],
        "ticker": part["ticker"],
        "source_file_count": len(source_files),
        "merge_input_file_count": len(merge_inputs),
        "row_count": int(merged.num_rows),
        "compacted_file": compact_key,
        "compacted_at": _now_utc().isoformat(),
        "run_id": run_id,
        "source_hash": hashlib.sha256("\n".join(merge_inputs).encode("utf-8")).hexdigest(),
    }
    _blob_put(blob, marker_key, json.dumps(marker).encode("utf-8"))

    deleted = 0
    for key in source_files:
        _blob_delete(blob, key)
        deleted += 1

    return {
        "partition": part,
        "action": "compacted",
        "source_file_count": len(source_files),
        "merge_input_file_count": len(merge_inputs),
        "row_count": int(merged.num_rows),
        "bytes_written": len(compact_bytes),
        "deleted_files": deleted,
        "compact_key": compact_key,
        "marker_key": marker_key,
    }


@task()
def list_candidate_partitions(
    base_prefix: str = DEFAULT_BASE_PREFIX,
    datasets: tuple[str, ...] = DEFAULT_DATASETS,
    last_n_hours: int = DEFAULT_LAST_N_HOURS,
) -> list[dict[str, str]]:
    from datatailr import Blob

    return list_candidate_partitions_impl(
        Blob(),
        base_prefix=base_prefix,
        datasets=datasets,
        last_n_hours=last_n_hours,
    )


@task()
def compact_partition(
    part: dict[str, str],
    min_files: int = DEFAULT_MIN_FILES,
    dry_run: bool = False,
) -> dict[str, Any]:
    from datatailr import Blob

    return compact_partition_impl(
        Blob(),
        part,
        min_files=min_files,
        dry_run=dry_run,
    )


@task()
def compact_partitions(
    parts: list[dict[str, str]],
    min_files: int = DEFAULT_MIN_FILES,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    from datatailr import Blob

    blob = Blob()
    results: list[dict[str, Any]] = []
    for part in parts:
        results.append(
            compact_partition_impl(
                blob,
                part,
                min_files=min_files,
                dry_run=dry_run,
            )
        )
    return results


@task()
def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "total": len(results),
        "compacted": 0,
        "skip_already_compacted": 0,
        "skip_compact_only": 0,
        "skip_below_threshold": 0,
        "would_compact": 0,
    }
    for r in results:
        a = str(r.get("action") or "")
        if a in summary:
            summary[a] += 1
    return summary
