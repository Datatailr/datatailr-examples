from llm_service.admin_app.usage_merge import SUM_FIELDS, merge_rows, own_rows


def row(key_name, key, requests=1, failed=0, prompt=10, completion=20, total=30, last="2026-01-01T00:00:00Z"):
    return {
        key_name: key,
        "requests": requests,
        "failed_requests": failed,
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
        "last_request": last,
    }


def test_sum_fields():
    assert SUM_FIELDS == ("requests", "failed_requests", "prompt_tokens", "completion_tokens", "total_tokens")


def test_merge_by_user_adds_a_key_found_in_two_services():
    merged = merge_rows(
        {
            "llm-a": [row("user", "alice", requests=2, total=100), row("user", "bob", total=5)],
            "llm-b": [row("user", "alice", requests=3, failed=1, total=50)],
        },
        "user",
    )
    assert [r["user"] for r in merged] == ["alice", "bob"]
    assert merged[0]["requests"] == 5
    assert merged[0]["failed_requests"] == 1
    assert merged[0]["total_tokens"] == 150
    assert merged[0]["prompt_tokens"] == 20


def test_merge_sorts_by_total_tokens_then_key():
    merged = merge_rows(
        {"a": [row("model", "m2", total=10), row("model", "m1", total=10), row("model", "m3", total=99)]}, "model"
    )
    assert [r["model"] for r in merged] == ["m3", "m1", "m2"]


def test_merge_by_day_keeps_the_newest_last_request():
    merged = merge_rows(
        {
            "a": [row("day", "2026-01-02", last="2026-01-02T01:00:00Z")],
            "b": [row("day", "2026-01-02", last="2026-01-02T09:00:00Z")],
        },
        "day",
    )
    assert len(merged) == 1
    assert merged[0]["last_request"] == "2026-01-02T09:00:00Z"


def test_null_token_counts_stay_null_and_a_number_wins_over_null():
    merged = merge_rows(
        {
            "a": [row("user", "alice", prompt=None, completion=None, total=None)],
            "b": [row("user", "alice", prompt=7, completion=None, total=None)],
            "c": [row("user", "bob", prompt=None, completion=None, total=None)],
        },
        "user",
    )
    by_user = {r["user"]: r for r in merged}
    assert by_user["alice"]["prompt_tokens"] == 7
    assert by_user["alice"]["completion_tokens"] is None
    assert by_user["bob"]["total_tokens"] is None
    assert [r["user"] for r in merged] == ["alice", "bob"]


def test_merge_by_service_gives_one_row_for_each_service():
    merged = merge_rows(
        {
            "llm-a": [row("day", "2026-01-01", requests=2, total=10), row("day", "2026-01-02", requests=3, total=20)],
            "llm-b": [row("day", "2026-01-01", requests=1, total=100)],
            "llm-c": [],
        },
        "service",
    )
    assert [r["service"] for r in merged] == ["llm-b", "llm-a", "llm-c"]
    assert merged[1]["requests"] == 5
    assert merged[1]["total_tokens"] == 30
    assert merged[2]["requests"] == 0


DETAIL = {
    "user": "alice",
    "totals": row("user", "alice", requests=4, total=40),
    "by_model": [row("model", "m1", requests=3, total=30), row("model", "m2", total=10)],
    "by_day": [row("day", "2026-01-02", requests=3, total=30), row("day", "2026-01-01", total=10)],
}


def test_own_rows_for_each_group_by():
    assert own_rows(DETAIL, "alice", "user") == [DETAIL["totals"]]
    assert own_rows(DETAIL, "alice", "model") == DETAIL["by_model"]
    assert own_rows(DETAIL, "alice", "day") == DETAIL["by_day"]


def test_own_rows_of_a_viewer_without_usage_has_no_user_row():
    empty = {
        "user": "carol",
        "totals": row("user", "carol", requests=0, failed=0, prompt=0, completion=0, total=0, last=None),
        "by_model": [],
        "by_day": [],
    }
    assert own_rows(empty, "carol", "user") == []
    assert own_rows(empty, "carol", "model") == []


def test_merge_tolerates_a_last_request_that_is_not_a_string():
    merged = merge_rows(
        {"a": [row("user", "alice", last=5)], "b": [row("user", "alice", last="2026-01-01T00:00:00Z")]}, "user"
    )
    assert merged[0]["last_request"] == "2026-01-01T00:00:00Z"
    merged = merge_rows({"a": [row("user", "alice", last="2026-01-01T00:00:00Z")], "b": [row("user", "alice", last=5)]}, "user")
    assert merged[0]["last_request"] == "2026-01-01T00:00:00Z"


def test_merge_by_service_keeps_null_tokens_null():
    merged = merge_rows(
        {
            "llm-a": [row("day", "2026-01-01", prompt=None, completion=None, total=None)],
            "llm-b": [row("day", "2026-01-01", prompt=None, completion=5, total=None)],
        },
        "service",
    )
    by_service = {r["service"]: r for r in merged}
    assert by_service["llm-a"]["total_tokens"] is None
    assert by_service["llm-a"]["prompt_tokens"] is None
    assert by_service["llm-a"]["requests"] == 1
    assert by_service["llm-b"]["completion_tokens"] == 5
    assert by_service["llm-b"]["prompt_tokens"] is None
