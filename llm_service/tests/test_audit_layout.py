"""Tests for the layout rules of the audit storage."""

from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone

import pytest

from llm_service.gateway import audit_layout as layout

ROOT = "llm/root"
NOW = datetime(2026, 11, 11, 14, 3, 59, 123456, tzinfo=timezone.utc)


def request_record(**extra) -> dict:
    record = {
        "ts": "2026-11-11T14:03:59.000Z",
        "request_id": "r1",
        "user": "alice",
        "endpoint": "/v1/chat/completions",
        "model": "m",
        "stream": False,
        "prompt": {"messages": [{"role": "user", "content": "hi"}]},
        "status": 200,
        "prompt_tokens": 3,
        "completion_tokens": 4,
        "total_tokens": 7,
        "duration_ms": 12,
    }
    record.update(extra)
    return record


def event_record(prompt: dict) -> dict:
    return request_record(request_id="e1", endpoint="/admin/model", prompt=prompt)


EVENTS = [
    {"event": "model_switch", "revision": "abc"},
    {"event": "engine_reload", "revisions": ["a", "b"]},
    {"event": "model_delete", "error": "boom"},
]


def test_stamp_and_segment_name():
    assert layout.stamp(NOW) == "20261111T140359.123456"
    assert layout.segment_name(NOW, "ab12") == "20261111T140359.123456-ab12"


def test_stamp_converts_to_utc():
    local = NOW.astimezone(timezone(timedelta(hours=2)))
    assert layout.stamp(local) == "20261111T140359.123456"


def test_later_time_gives_greater_name():
    later = NOW + timedelta(microseconds=1)
    assert layout.segment_name(later, "x") > layout.segment_name(NOW, "x")
    next_day = NOW + timedelta(days=30)
    assert layout.segment_name(next_day, "x") > layout.segment_name(later, "x")


def test_bucket_of_and_end():
    assert layout.bucket_of(layout.stamp(NOW)) == "20261111T140"
    assert layout.bucket_of(layout.segment_name(NOW, "x")) == "20261111T140"
    end = layout.bucket_end("20261111T140")
    assert end == datetime(2026, 11, 11, 14, 10, tzinfo=timezone.utc)
    assert end.tzinfo is not None
    assert layout.bucket_end("20261111T235") == datetime(2026, 11, 12, tzinfo=timezone.utc)


@pytest.mark.parametrize("text", ["", "20261111T14", "2026-11-11T14", "20261111X140", "20261111T999", "abc"])
def test_bucket_end_rejects_bad_text(text):
    with pytest.raises(ValueError):
        layout.bucket_end(text)


def test_day_end():
    assert layout.day_end("2026-11-11") == datetime(2026, 11, 12, tzinfo=timezone.utc)
    assert layout.day_end("2026-12-31") == datetime(2027, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        layout.day_end("11/11/2026")


def test_paths():
    name = "20261111T140359.123456-ab12"
    assert layout.audit_dir(ROOT) == "llm/root/audit/"
    assert layout.segments_dir(ROOT, "2026-11-11") == "llm/root/audit/2026-11-11/segments/"
    assert layout.parts_dir(ROOT, "2026-11-11") == "llm/root/audit/2026-11-11/parts/"
    assert layout.prompts_dir(ROOT, "2026-11-11") == "llm/root/prompts/2026-11-11/"
    assert layout.segment_path(ROOT, "2026-11-11", name) == f"llm/root/audit/2026-11-11/segments/{name}.jsonl"
    assert layout.part_path(ROOT, "2026-11-11", "20261111T140") == "llm/root/audit/2026-11-11/parts/20261111T140.jsonl"
    assert layout.day_path(ROOT, "2026-11-11") == "llm/root/audit/2026-11-11/day.jsonl"
    assert layout.prompt_path(ROOT, "2026-11-11", name) == f"llm/root/prompts/2026-11-11/{name}.jsonl"


def test_classify():
    base = "llm/root/audit/2026-11-11"
    name = "20261111T140359.123456-ab12"
    assert layout.classify(ROOT, f"{base}/segments/{name}.jsonl") == ("2026-11-11", "segment", name)
    assert layout.classify(ROOT, f"{base}/parts/20261111T140.jsonl") == ("2026-11-11", "part", "20261111T140")
    assert layout.classify(ROOT, f"{base}/day.jsonl") == ("2026-11-11", "day", "")
    assert layout.classify(ROOT, f"{base}/old-file.jsonl") == ("2026-11-11", "legacy", "old-file")


@pytest.mark.parametrize(
    "path",
    [
        "llm/root/prompts/2026-11-11/20261111T140359.123456-ab12.jsonl",
        "llm/root/usage/2026-11-11.json",
        "other/audit/2026-11-11/day.jsonl",
        "llm/root/audit/2026-11-11/segments/x.txt",
        "llm/root/audit/2026-11-11/segments/sub/x.jsonl",
        "llm/root/audit/not-a-day/day.jsonl",
        "llm/root/audit/2026-11-11/",
        "llm/root/audit/2026-11-11",
        "llm/root/audit/2026-11-11/other/x.jsonl",
        "llm/rootaudit/2026-11-11/day.jsonl",
    ],
)
def test_classify_other_paths(path):
    assert layout.classify(ROOT, path) is None


def test_split_request():
    record = request_record()
    original = copy.deepcopy(record)
    row, line = layout.split_record(record, "seg1", 4)
    assert record == original
    assert "prompt" not in row
    assert row["event"] is None and row["detail"] is None
    assert row["prompt_ref"] == {"file": "seg1", "line": 4}
    assert row["seg"] == "seg1"
    assert row["user"] == "alice" and row["total_tokens"] == 7
    assert line == {"request_id": "r1", "prompt": record["prompt"]}


def test_split_empty_prompt_is_request():
    row, line = layout.split_record(request_record(prompt={}), "s", 0)
    assert row["event"] is None
    assert row["prompt_ref"] == {"file": "s", "line": 0}
    assert line == {"request_id": "r1", "prompt": {}}


def test_split_without_request_id():
    record = request_record()
    del record["request_id"]
    row, line = layout.split_record(record, "s", 1)
    assert line == {"request_id": None, "prompt": record["prompt"]}
    assert "request_id" not in row


@pytest.mark.parametrize("prompt", EVENTS)
def test_split_event(prompt):
    record = event_record(copy.deepcopy(prompt))
    original = copy.deepcopy(record)
    row, line = layout.split_record(record, "seg1", 9)
    assert record == original
    assert line is None
    assert row["event"] == prompt["event"]
    assert row["detail"] == {k: v for k, v in prompt.items() if k != "event"}
    assert row["prompt_ref"] is None
    assert row["seg"] == "seg1"
    assert "prompt" not in row


def test_split_event_without_detail():
    row, line = layout.split_record(event_record({"event": "model_switch"}), "s", None)
    assert row["event"] == "model_switch" and row["detail"] == {} and line is None


def test_split_does_not_share_detail():
    record = event_record({"event": "engine_reload", "revisions": ["a"]})
    row, _ = layout.split_record(record, "s", None)
    row["detail"]["revisions"].append("z")
    assert record["prompt"]["revisions"] == ["a"]


def test_join_request_round_trip():
    record = request_record()
    row, line = layout.split_record(record, "s", 0)
    assert layout.join_record(row, line["prompt"]) == {**record, "event": None, "detail": None}


@pytest.mark.parametrize("prompt", EVENTS)
def test_join_event_round_trip(prompt):
    record = event_record(copy.deepcopy(prompt))
    row, line = layout.split_record(record, "s", None)
    joined = layout.join_record(row, line)
    assert joined == {**record, "event": prompt["event"], "detail": row["detail"]}
    assert joined["prompt"] == prompt


def test_join_deleted():
    row, _ = layout.split_record(request_record(), "s", 0)
    joined = layout.join_record(row, None, deleted=True)
    assert joined["prompt_deleted"] is True
    assert joined["prompt"] is None
    assert layout.join_record(row, None)["prompt"] is None
    assert "prompt_deleted" not in layout.join_record(row, {"a": 1})
    assert layout.join_record(row, {"a": 1})["prompt"] == {"a": 1}


def test_public_row():
    row, _ = layout.split_record(request_record(), "s", 0)
    public = layout.public_row(row)
    assert "seg" not in public and "prompt_ref" not in public
    assert public["event"] is None and public["user"] == "alice"
    assert "seg" in row


def test_legacy_rows():
    records = [request_record(), event_record({"event": "model_delete", "error": "x"})]
    original = copy.deepcopy(records)
    rows = layout.legacy_rows("old-file", records)
    assert records == original
    assert len(rows) == 2
    assert rows[0]["seg"] == "old-file"
    assert rows[0]["prompt_ref"] == {"file": "old-file", "line": 0, "legacy": True}
    assert rows[0]["event"] is None and "prompt" not in rows[0]
    assert rows[1]["seg"] == "old-file"
    assert rows[1]["event"] == "model_delete"
    assert rows[1]["detail"] == {"error": "x"}
    assert rows[1]["prompt_ref"] is None


def test_legacy_rows_without_request_id():
    record = request_record()
    del record["request_id"]
    rows = layout.legacy_rows("old", [record])
    assert rows[0]["prompt_ref"] == {"file": "old", "line": 0, "legacy": True}


def test_dump_and_parse_lines():
    prompt = "a\nb c\u0085d e\r\nf"
    text = layout.dump_lines([{"prompt": prompt}, {"n": 2}])
    assert text.endswith("\n")
    assert len(text.split("\n")) == 3
    assert len(text.splitlines()) == 2
    assert text.isascii()
    parsed = layout.parse_lines(text)
    assert parsed == [{"prompt": prompt}, {"n": 2}]


def test_dump_lines_empty():
    assert layout.dump_lines([]) == ""
    assert layout.parse_lines("") == []


def test_parse_lines_bad_line_keeps_index():
    text = '{"a": 1}\nnot json\n\n{"b": 2}\n'
    assert layout.parse_lines(text) == [{"a": 1}, None, None, {"b": 2}]
    assert layout.parse_lines('{"a": 1}') == [{"a": 1}]
    assert json.loads(layout.dump_lines([{"a": 1}])) == {"a": 1}


def test_parse_lines_gives_none_for_a_line_nested_too_deep():
    deep = "[" * 100_000 + "]" * 100_000
    assert layout.parse_lines(f'{{"a": 1}}\n{deep}\n[2]\n') == [{"a": 1}, None, [2]]
