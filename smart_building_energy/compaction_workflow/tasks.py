from __future__ import annotations

import io
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datatailr import task

DEFAULT_BASE_PREFIX = os.environ.get("ENERGY_BLOB_PREFIX", "smart_building_energy").strip("/")
DEFAULT_DATASETS = os.environ.get("COMPACTION_DATASETS", "raw,curated,alerts")
DEFAULT_LAST_N_HOURS = int(os.environ.get("COMPACTION_LAST_N_HOURS", "24"))
DEFAULT_MIN_FILES = int(os.environ.get("COMPACTION_MIN_FILES", "8"))


def _blob():
    from datatailr import Blob

    return Blob()


def _entry_name(entry: Any) -> str:
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        return str(entry.get("name", ""))
    return str(entry)


def _strip_blob_prefix(path: str) -> str:
    p = path.strip()
    if p.startswith("blob://"):
        p = p[len("blob://") :]
    return p.lstrip("/")


def _join_blob_path(parent: str, child: str) -> str:
    parent = _strip_blob_prefix(parent).rstrip("/")
    child = _strip_blob_prefix(child)
    if not child:
        return parent
    if child.startswith(parent + "/") or child == parent:
        return child

    parent_parts = [p for p in parent.split("/") if p]
    child_parts = [p for p in child.split("/") if p]
    max_overlap = min(len(parent_parts), len(child_parts))
    for k in range(max_overlap, 0, -1):
        if parent_parts[-k:] == child_parts[:k]:
            merged = parent_parts + child_parts[k:]
            return "/".join(merged)

    return f"{parent}/{child}"


def _last_segment(path: str) -> str:
    return _strip_blob_prefix(path).rstrip("/").split("/")[-1]


def _list_children(prefix: str) -> list[str]:
    base = _strip_blob_prefix(prefix).rstrip("/") + "/"
    children: list[str] = []
    for item in _blob().ls(base):
        name = _entry_name(item)
        if not name:
            continue
        children.append(_join_blob_path(base, name))
    return children


def _list_dataset_files(base_prefix: str, dataset: str) -> list[str]:
    dataset_root = f"{_strip_blob_prefix(base_prefix)}/{dataset}"
    files: list[str] = []
    for dt_path in _list_children(dataset_root):
        if not _last_segment(dt_path).startswith("dt="):
            continue
        for hour_path in _list_children(dt_path):
            if not _last_segment(hour_path).startswith("hour="):
                continue
            for building_path in _list_children(hour_path):
                if not _last_segment(building_path).startswith("building_id="):
                    continue
                for fpath in _list_children(building_path):
                    if fpath.endswith(".parquet"):
                        files.append(_strip_blob_prefix(fpath))
    return files


def _parse_dt_hour(path: str) -> tuple[str, str] | None:
    parts = path.split("/")
    dt_s = None
    hour_s = None
    for p in parts:
        if p.startswith("dt="):
            dt_s = p[3:]
        elif p.startswith("hour="):
            hour_s = p[5:]
    if dt_s and hour_s:
        return dt_s, hour_s
    return None


def _recent_set(last_n_hours: int) -> set[tuple[str, str]]:
    now = datetime.now(timezone.utc)
    vals: set[tuple[str, str]] = set()
    for i in range(max(1, last_n_hours)):
        t = now - timedelta(hours=i)
        vals.add((t.strftime("%Y-%m-%d"), t.strftime("%H")))
    return vals


@task(memory="512m", cpu=0.5)
def list_candidate_partitions(
    base_prefix: str = DEFAULT_BASE_PREFIX,
    datasets_csv: str = DEFAULT_DATASETS,
    last_n_hours: int = DEFAULT_LAST_N_HOURS,
) -> list[dict[str, Any]]:
    datasets = [x.strip() for x in datasets_csv.split(",") if x.strip()]
    valid_hours = _recent_set(last_n_hours)
    partitions: dict[str, list[str]] = defaultdict(list)

    b = _blob()
    for dataset in datasets:
        for path in _list_dataset_files(base_prefix, dataset):
            if not path.endswith(".parquet"):
                continue
            parsed = _parse_dt_hour(path)
            if parsed is None or parsed not in valid_hours:
                continue
            if "/compact-" in path:
                continue
            parent = path.rsplit("/", 1)[0]
            marker = f"{parent}/_COMPACTED.json"
            if b.exists(marker):
                continue
            partitions[parent].append(path)

    return [{"partition": k, "files": sorted(v)} for k, v in sorted(partitions.items())]


def _read_files(paths: list[str]) -> pd.DataFrame:
    if not paths:
        return pd.DataFrame()
    b = _blob()
    rows: list[dict[str, Any]] = []
    for p in paths:
        table = pq.read_table(io.BytesIO(b.get(p)))
        rows.extend(table.to_pandas().to_dict(orient="records"))
    return pd.DataFrame(rows)


@task(memory="2g", cpu=1)
def compact_partitions(
    partitions: list[dict[str, Any]],
    min_files: int = DEFAULT_MIN_FILES,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    b = _blob()
    results: list[dict[str, Any]] = []
    for item in partitions:
        parent = item["partition"]
        files = item["files"]
        if len(files) < min_files:
            results.append({"partition": parent, "action": "skip", "reason": "too_few_files", "files": len(files)})
            continue

        if dry_run:
            results.append({"partition": parent, "action": "dry_run", "files": len(files)})
            continue

        df = _read_files(files)
        if df.empty:
            results.append({"partition": parent, "action": "skip", "reason": "empty_data", "files": len(files)})
            continue

        table = pa.Table.from_pandas(df.reset_index(drop=True), preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        compact_path = f"{parent}/compact-{time.time_ns()}.parquet"
        b.put(compact_path, buf.getvalue())
        marker = {
            "compacted_at": datetime.now(timezone.utc).isoformat(),
            "source_files": len(files),
            "compact_file": compact_path,
            "rows": int(len(df)),
        }
        b.put(f"{parent}/_COMPACTED.json", json.dumps(marker))
        results.append(
            {
                "partition": parent,
                "action": "compacted",
                "source_files": len(files),
                "rows": int(len(df)),
                "output": compact_path,
            }
        )
    return results


@task()
def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(results)
    compacted = len([r for r in results if r.get("action") == "compacted"])
    skipped = len([r for r in results if r.get("action") == "skip"])
    dry_runs = len([r for r in results if r.get("action") == "dry_run"])
    return {
        "checked_partitions": total,
        "compacted": compacted,
        "skipped": skipped,
        "dry_runs": dry_runs,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

