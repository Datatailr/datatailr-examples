"""Run records: one JSON file per click, which the app and the jobs it starts both use."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import PurePosixPath

from merit_desk.common.store import TextStore, env_prefix

logger = logging.getLogger(__name__)

KINDS = ("scenario", "backtest", "job")
STATUSES = ("queued", "running", "done", "failed")
TERMINAL = ("done", "failed")


def _key(run_id: str) -> str:
    return f"{env_prefix()}runs/{run_id}.json"


def new_run(kind: str, label: str, inputs: dict, started_by: str | None) -> dict:
    """A queued record. The id starts with the time, so sorted ids are in time order."""
    now = datetime.now(timezone.utc)
    stamp = now.isoformat(timespec="seconds")
    return {
        # microseconds keep ids from one second in order
        "id": f"{now:%Y%m%dT%H%M%S}-{now.microsecond:06x}",
        "kind": kind,
        "label": label,
        "inputs": inputs,
        "status": "queued",
        "started_by": started_by,
        "job_name": None,
        "created": stamp,
        "updated": stamp,
        "result": None,
        "error": None,
    }


def save_run(record: dict, store: TextStore) -> None:
    record["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    store.put_text(_key(record["id"]), json.dumps(record))


def load_run(run_id: str, store: TextStore) -> dict | None:
    """The record, or None when there is none or it does not parse (the id is logged).
    An error of the store itself is raised."""
    try:
        text = store.get_text(_key(run_id))  # bytes that are not UTF-8 raise here
        if text is None:
            return None
        record = json.loads(text)
    except ValueError:  # UnicodeDecodeError is a ValueError, too
        record = None
    if not isinstance(record, dict):
        logger.warning("Run record %s is not a JSON object. Skipping it.", run_id)
        return None
    return record


def list_run_ids(store: TextStore) -> list[str]:
    """The ids of all stored runs, newest first. Nothing is read but the names."""
    names = store.list(f"{env_prefix()}runs/")
    return sorted((PurePosixPath(n).stem for n in names if n.endswith(".json")), reverse=True)


def list_runs(store: TextStore, limit: int = 50) -> list[dict]:
    """The newest records first. It reads ids until it has `limit` good records, no more."""
    records = []
    for run_id in list_run_ids(store):
        if len(records) == limit:
            break
        record = load_run(run_id, store)  # a bad or vanished record must not block the rest
        if record is not None:
            records.append(record)
    return records


def finish_run(run_id: str, store: TextStore, *, result: dict | None = None,
               error: str | None = None) -> dict:
    """Store the result (status done) or the error (status failed) and give back the record."""
    record = load_run(run_id, store)
    if record is None:
        raise LookupError(f"There is no run record {run_id}.")
    record.update(status="done" if error is None else "failed", result=result, error=error)
    save_run(record, store)
    return record


def run_label(kind: str, inputs: dict) -> str:
    """A short ASCII name for the run card and the job, such as 'Scenario DE -3.0C 2 plants'."""
    if kind == "scenario":
        market, shift, n = inputs["market"], inputs["shift_c"], len(inputs["overrides"])
        plants = f" {n} plant{'' if n == 1 else 's'}" if n else ""
        if shift == 0 and n:  # only the stack changes
            return f"Stack {market}{plants}"
        return f"Scenario {market} {shift + 0.0:+.1f}C{plants}"  # + 0.0 turns -0.0 into 0.0
    if kind == "backtest":
        return f"Backtest {inputs['market']} M{inputs['tenor']} t{inputs['threshold']:g}"
    if kind == "job":
        return inputs["name"]
    raise ValueError(f"Unknown run kind '{kind}'. Use one of: {', '.join(KINDS)}.")
