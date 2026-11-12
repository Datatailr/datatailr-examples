"""Merge the usage rows of several LLM services into one table."""

from __future__ import annotations

SUM_FIELDS = ("requests", "failed_requests", "prompt_tokens", "completion_tokens", "total_tokens")
KEYS = ("user", "model", "day")


def _add(first: int | None, second: int | None) -> int | None:
    """Add two counts. A null count stays null only when both are null."""
    if first is None:
        return second
    if second is None:
        return first
    return first + second


def _sorted(rows: list[dict], key: str) -> list[dict]:
    """Sort as the services do: largest total tokens first, then by key. A null total counts as 0."""
    return sorted(rows, key=lambda r: (-(r["total_tokens"] or 0), r[key]))


def _fold(groups: dict[str, dict], key: str, name: str, row: dict) -> None:
    target = groups.get(name)
    if target is None:
        groups[name] = {key: name, **{field: row.get(field) for field in SUM_FIELDS}}
        target = groups[name]
        last = row.get("last_request")
        target["last_request"] = last if isinstance(last, str) else None
        return
    for field in SUM_FIELDS:
        target[field] = _add(target[field], row.get(field))
    last = row.get("last_request")
    if isinstance(last, str):
        held = target["last_request"]
        if not isinstance(held, str) or last > held:
            target["last_request"] = last


def merge_rows(per_service: dict[str, list[dict]], group_by: str) -> list[dict]:
    """Merge the rows of each service.

    With `group_by` user, model or day, rows with the same key become one row
    with the sums of `SUM_FIELDS`. With `group_by` service, the rows of each
    service are day rows. The result has one row for each service, with the key
    `service`. All sorts are by total tokens, largest first, then by key.
    """
    groups: dict[str, dict] = {}
    if group_by == "service":
        for name, rows in per_service.items():
            groups[name] = {"service": name, **{field: None for field in SUM_FIELDS}, "last_request": None}
            groups[name]["requests"] = groups[name]["failed_requests"] = 0
            for row in rows:
                _fold(groups, "service", name, row)
        return _sorted(list(groups.values()), "service")
    for rows in per_service.values():
        for row in rows:
            _fold(groups, group_by, row[group_by], row)
    return _sorted(list(groups.values()), group_by)


def own_rows(detail: dict, viewer: str, group_by: str) -> list[dict]:
    """Turn the answer of `GET /v1/usage/me` into rows of the form of `GET /admin/usage`.

    `group_by` is user, model or day. The user rows have the viewer as the only
    user. A viewer without usage gets no user row.
    """
    if group_by == "model":
        return list(detail["by_model"])
    if group_by == "day":
        return list(detail["by_day"])
    totals = detail["totals"]
    if not any(totals.get(field) for field in SUM_FIELDS):
        return []
    return [{**totals, "user": viewer}]
