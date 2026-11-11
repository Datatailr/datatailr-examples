"""Layout rules of the audit storage: file names, buckets, and the split of a record.

All functions are pure. They read no storage and no clock. All times are UTC.
"""

from __future__ import annotations

import copy
import json
import re
from datetime import datetime, timedelta, timezone

BUCKET_LENGTH = 12  # characters of a stamp that name its 10 minute bucket
_BUCKET = re.compile(r"\d{8}T\d{2}\d")
_DAY = re.compile(r"\d{4}-\d{2}-\d{2}")
_EVENT_KEY = "event"


def stamp(now: datetime) -> str:
    """Format `now` as a UTC stamp with microseconds, for example 20261111T140359.123456."""
    if now.tzinfo is not None:
        now = now.astimezone(timezone.utc)
    return now.strftime("%Y%m%dT%H%M%S.%f")


def segment_name(now: datetime, ident: str) -> str:
    """Return the segment name `<stamp>-<ident>`."""
    return f"{stamp(now)}-{ident}"


def bucket_of(name: str) -> str:
    """Return the bucket of a segment name or a stamp: its first 12 characters."""
    return name[:BUCKET_LENGTH]


def bucket_end(bucket: str) -> datetime:
    """Return the end of the 10 minutes of `bucket` as an aware UTC datetime.

    Raise `ValueError` for a text that is not a bucket.
    """
    if not _BUCKET.fullmatch(bucket):
        raise ValueError(f"not a bucket: {bucket!r}")
    try:
        start = datetime.strptime(bucket[:-1], "%Y%m%dT%H").replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError(f"not a bucket: {bucket!r}") from None
    minute = int(bucket[-1]) * 10
    if minute >= 60:
        raise ValueError(f"not a bucket: {bucket!r}")
    return start + timedelta(minutes=minute + 10)


def day_end(day: str) -> datetime:
    """Return the end of the UTC day `YYYY-MM-DD` as an aware UTC datetime.

    Raise `ValueError` for a text that is not a day.
    """
    if not _DAY.fullmatch(day):
        raise ValueError(f"not a day: {day!r}")
    try:
        start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError(f"not a day: {day!r}") from None
    return start + timedelta(days=1)


def audit_dir(root: str) -> str:
    return f"{root}/audit/"


def segments_dir(root: str, day: str) -> str:
    return f"{root}/audit/{day}/segments/"


def parts_dir(root: str, day: str) -> str:
    return f"{root}/audit/{day}/parts/"


def prompts_dir(root: str, day: str) -> str:
    return f"{root}/prompts/{day}/"


def segment_path(root: str, day: str, name: str) -> str:
    return f"{segments_dir(root, day)}{name}.jsonl"


def part_path(root: str, day: str, bucket: str) -> str:
    return f"{parts_dir(root, day)}{bucket}.jsonl"


def day_path(root: str, day: str) -> str:
    return f"{root}/audit/{day}/day.jsonl"


def prompt_path(root: str, day: str, name: str) -> str:
    return f"{prompts_dir(root, day)}{name}.jsonl"


def classify(root: str, path: str) -> tuple[str, str, str] | None:
    """Classify a blob path under `audit/`.

    Return `(day, kind, name)`. `kind` is "segment", "part", "day" or "legacy". `name` is the
    segment name, the bucket, an empty text, or the old file name without `.jsonl`.
    Return None for any other path.
    """
    prefix = audit_dir(root)
    if not path.startswith(prefix):
        return None
    parts = path[len(prefix):].split("/")
    if not _DAY.fullmatch(parts[0]):
        return None
    day = parts[0]
    if len(parts) == 2 and parts[1].endswith(".jsonl"):
        stem = parts[1][: -len(".jsonl")]
        if not stem:
            return None
        return (day, "day", "") if stem == "day" else (day, "legacy", stem)
    if len(parts) == 3 and parts[2].endswith(".jsonl") and parts[1] in ("segments", "parts"):
        stem = parts[2][: -len(".jsonl")]
        if not stem:
            return None
        return (day, "segment" if parts[1] == "segments" else "part", stem)
    return None


def _is_event(record: dict) -> bool:
    prompt = record.get("prompt")
    return isinstance(prompt, dict) and _EVENT_KEY in prompt


def split_record(record: dict, seg: str, line: int | None) -> tuple[dict, dict | None]:
    """Split an audit record into a metadata row and a prompt line.

    A request gets a `prompt_ref` to `line` of file `seg` and a prompt line. An event keeps its
    data in `detail`, has no `prompt_ref` and no prompt line. The row always gains `seg`.
    The input record is not changed.
    """
    row = {key: copy.deepcopy(value) for key, value in record.items() if key != "prompt"}
    row["seg"] = seg
    if _is_event(record):
        detail = {k: v for k, v in record["prompt"].items() if k != _EVENT_KEY}
        row["event"] = record["prompt"][_EVENT_KEY]
        row["detail"] = copy.deepcopy(detail)
        row["prompt_ref"] = None
        return row, None
    row["event"] = None
    row["detail"] = None
    row["prompt_ref"] = {"file": seg, "line": line}
    prompt_line = {"request_id": record.get("request_id"), "prompt": copy.deepcopy(record.get("prompt"))}
    return row, prompt_line


def public_row(row: dict) -> dict:
    """Return the row without the storage fields `seg` and `prompt_ref`."""
    return {key: value for key, value in row.items() if key not in ("seg", "prompt_ref")}


def join_record(row: dict, prompt: dict | None, deleted: bool = False) -> dict:
    """Return the full record for the API: the row as it was before the split, plus its prompt.

    The storage fields `seg` and `prompt_ref` and the split fields `event` and `detail` are
    removed. An event builds its prompt from `event` and `detail`. A request uses `prompt`,
    which is left out when it is None. `prompt_deleted` is added only when `deleted` is true.
    """
    record = {key: value for key, value in public_row(row).items() if key not in ("event", "detail")}
    if row.get("event") is not None:
        record["prompt"] = {_EVENT_KEY: row["event"], **(row.get("detail") or {})}
    elif prompt is not None:
        record["prompt"] = prompt
    if deleted:
        record["prompt_deleted"] = True
    return record


def legacy_rows(name: str, records: list[dict]) -> list[dict]:
    """Return metadata rows for the records of an old layout file called `name`."""
    rows = []
    for index, record in enumerate(records):
        row, _ = split_record(record, name, index)
        if row["prompt_ref"] is not None:
            row["prompt_ref"]["legacy"] = True
        rows.append(row)
    return rows


def dump_lines(objects: list[dict]) -> str:
    """Return one JSON line for each object. ASCII escapes keep a line break out of a line."""
    return "".join(json.dumps(obj, ensure_ascii=True) + "\n" for obj in objects)


def parse_lines(text: str) -> list[object | None]:
    """Parse JSON lines. Split on "\\n" only. A line that is not JSON gives None."""
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    result: list[object | None] = []
    for item in lines:
        try:
            result.append(json.loads(item))
        except ValueError:
            result.append(None)
    return result
