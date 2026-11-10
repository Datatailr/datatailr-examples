# LLM Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Datatailr service example that serves a Hugging Face model through an OpenAI-compatible API, with a blob storage weight cache, an audit log, usage metrics and admin endpoints.

**Architecture:** A FastAPI gateway starts `vllm serve` as a child process and forwards `/v1/*` to it. The gateway owns identity, the audit log, the usage totals, the weight cache and the model switch. All durable state is in Datatailr blob storage.

**Tech Stack:** Python, FastAPI, uvicorn, httpx, huggingface_hub, vLLM, datatailr SDK, pytest.

**Spec:** `llm_service/docs/2026-11-10-llm-service-design.md`. Read it before any task.

## Global Constraints

- Work in the worktree root. Run tests with `python -m pytest llm_service/tests -q`.
- Before the first commit, run `git switch -c llm-service-example`. Never commit on a `claude/` branch.
- Commit messages start with `llm_service: `. No co-author trailer and no AI attribution line.
- The tests need no GPU, no vLLM, no network and no Datatailr platform. Only `gateway/storage.py` (`DatatailrBlobStore`), `gateway/identity.py` (`platform_group_members`), `gateway/engine.py` (`VllmEngine`) and `gateway/app.py` (`main`) import `datatailr` or start vLLM, and they do so inside functions, not at module import.
- Errors on `/v1/*` and `/admin/*` use the body `{"error": {"message": str, "type": str, "code": str}}`.
- Timestamps are UTC, ISO 8601 with a `Z` suffix. Days are `YYYY-MM-DD` in UTC.
- Blob root default `llm-service`. Layout as in the spec section "Weights".
- Default values from the spec: flush every 5 seconds or at 100 records, admin group cache 60 seconds, default model `Qwen/Qwen2.5-1.5B-Instruct`.
- No directory names with hyphens. No secrets, internal host names or real user names in any file.
- Documentation text (README, docstrings) uses plain English: short sentences, active voice, no contractions.

## Review Focus

1. A request body that is not JSON, or JSON that is not an object, on a `/v1/*` POST. Expected: 400 with the OpenAI error body, and one audit record with status 400. (Task 8)
2. A model name with a path component such as `../x`, an absolute path, or more than one `/`. Expected: `POST /admin/model` answers 400 and nothing touches blob storage or the disk. (Task 6)
3. A client that disconnects in the middle of a stream. Expected: the audit record is still written, with the tokens counted so far unknown (`null`) and status 499. (Task 7)
4. A blob storage error during an audit flush. Expected: the records stay in the buffer, the next flush sends them, and no request fails. (Task 3)
5. The vLLM process exits while the state is `ready`. Expected: requests get 503, the state becomes `failed` with the error, and `POST /admin/model` can load a model again. (Task 6)

---

### Task 1: Scaffold, configuration and blob store

**Files:**
- Create: `llm_service/__init__.py`, `llm_service/gateway/__init__.py`, `llm_service/tests/__init__.py` (all empty)
- Create: `llm_service/gateway/config.py`, `llm_service/gateway/storage.py`
- Create: `llm_service/requirements-dev.txt` with `pytest`, `fastapi`, `httpx`, `uvicorn`
- Test: `llm_service/tests/test_config.py`, `llm_service/tests/test_storage.py`

**Interfaces:**
- Produces `Config` (frozen dataclass) with `Config.from_env(env: Mapping[str, str]) -> Config` and fields: `default_model: str`, `blob_root: str`, `admin_group: str | None`, `hf_token_key: str | None`, `vllm_args: list[str]` (from `shlex.split`), `owner: str` (from `DATATAILR_USER`, default `""`), `local_dir: Path` (from `LLM_LOCAL_DIR`, default `/tmp/llm-service`), `engine_port: int` (from `LLM_ENGINE_PORT`, default `8001`), `flush_interval: float = 5.0`, `flush_size: int = 100`.
- Produces `BlobStore` (Protocol): `put(path: str, data: bytes | str) -> None`, `get(path: str) -> bytes`, `put_file(path: str, local: Path) -> None`, `get_file(path: str, local: Path) -> None`, `ls(prefix: str) -> list[str]` (full blob paths of every file under the prefix, recursive, sorted), `exists(path: str) -> bool`.
- Produces `MemoryBlobStore` (dict backed, used by tests and by a local run) and `DatatailrBlobStore` (wraps `datatailr.Blob`, imported inside `__init__`).

- [ ] **Step 1: Write the failing tests**

```python
# test_config.py
def test_defaults():
    c = Config.from_env({})
    assert c.default_model == "Qwen/Qwen2.5-1.5B-Instruct"
    assert c.blob_root == "llm-service"
    assert c.admin_group is None and c.hf_token_key is None
    assert c.vllm_args == [] and c.engine_port == 8001

def test_overrides():
    c = Config.from_env({"LLM_BLOB_ROOT": "/x/", "LLM_VLLM_ARGS": "--max-model-len 4096",
                         "LLM_ADMIN_GROUP": "llm-admins", "DATATAILR_USER": "alice"})
    assert c.blob_root == "x"                      # slashes stripped
    assert c.vllm_args == ["--max-model-len", "4096"]
    assert c.admin_group == "llm-admins" and c.owner == "alice"

# test_storage.py
def test_memory_store_roundtrip(tmp_path):
    s = MemoryBlobStore()
    s.put("r/a/1.txt", "hello"); s.put("r/a/b/2.bin", b"\x00")
    assert s.get("r/a/1.txt") == b"hello"
    assert s.ls("r/a") == ["r/a/1.txt", "r/a/b/2.bin"]
    assert s.ls("r/a/") == s.ls("r/a")
    assert s.ls("r/ab") == []                      # prefix is a directory, not a string prefix
    assert s.exists("r/a/1.txt") and not s.exists("r/a")
    s.get_file("r/a/1.txt", tmp_path / "d" / "1.txt")   # creates parent directories
    assert (tmp_path / "d" / "1.txt").read_text() == "hello"
    s.put_file("r/c", tmp_path / "d" / "1.txt"); assert s.get("r/c") == b"hello"
    with pytest.raises(KeyError): s.get("missing")
```

- [ ] **Step 2: Run** `python -m pytest llm_service/tests -q`. Expected: import errors.
- [ ] **Step 3: Implement `config.py` and `storage.py`.** For `DatatailrBlobStore.ls`, read `datatailr/blob.py` in the installed SDK (`python -c "import datatailr.blob as b; print(b.__file__)"`) and normalize its result to the contract above: full paths, files only, recursive. If the SDK listing is one level deep, walk the directories. `get` raises `KeyError` when the blob does not exist.
- [ ] **Step 4: Run the tests.** Expected: pass.
- [ ] **Step 5: Commit** `llm_service: scaffold, configuration and blob store`.

### Task 2: Identity and admin check

**Files:**
- Create: `llm_service/gateway/identity.py`
- Test: `llm_service/tests/test_identity.py`

**Interfaces:**
- Produces `user_from_headers(headers: Mapping[str, str]) -> str | None`: reads `x-datatailr-user`. The value is JSON `{"name": ...}` or a plain name.
- Produces `AdminCheck(owner: str, group: str | None, members: Callable[[str], list[str]], ttl: float = 60.0, clock: Callable[[], float] = time.monotonic)` with `is_admin(user: str) -> bool`.
- Produces `platform_group_members(group: str) -> list[str]`: user names from `datatailr.Group.get(group).members()`, imported inside the function. Check the element type of `members()` in the installed SDK and return names.

- [ ] **Step 1: Write the failing tests**

```python
def test_user_from_headers():
    assert user_from_headers({"x-datatailr-user": '{"name": "alice", "signature": "s"}'}) == "alice"
    assert user_from_headers({"x-datatailr-user": "bob"}) == "bob"
    assert user_from_headers({}) is None
    assert user_from_headers({"x-datatailr-user": "{broken"}) is None
    assert user_from_headers({"x-datatailr-user": '{"name": ""}'}) is None

def test_admin_owner_and_group_cache():
    calls, now = [], [0.0]
    def members(g): calls.append(g); return ["carol"]
    check = AdminCheck("alice", "llm-admins", members, clock=lambda: now[0])
    assert check.is_admin("alice") and check.is_admin("carol") and not check.is_admin("dave")
    assert calls == ["llm-admins"]            # owner needs no lookup, group fetched once
    now[0] = 61; check.is_admin("carol"); assert len(calls) == 2

def test_admin_without_group_and_lookup_error():
    assert not AdminCheck("alice", None, lambda g: 1 / 0).is_admin("bob")
    def boom(g): raise RuntimeError("kv down")
    assert not AdminCheck("alice", "g", boom).is_admin("bob")   # an error means not admin
    assert not AdminCheck("", None, lambda g: []).is_admin("")  # empty owner matches nobody
```

- [ ] **Step 2: Run, expect failure. Step 3: Implement. Step 4: Run, expect pass.**
- [ ] **Step 5: Commit** `llm_service: identity and admin check`.

### Task 3: Audit log

**Files:**
- Create: `llm_service/gateway/audit.py`
- Test: `llm_service/tests/test_audit.py`

**Interfaces:**
- Consumes `BlobStore`.
- Produces `AuditLog(store: BlobStore, root: str, flush_size: int = 100, now: Callable[[], datetime] = utc_now)`:
  - `add(record: dict) -> None`: appends to the buffer, calls each listener, and calls `flush()` when the buffer holds `flush_size` records. A record is the dict of the spec section "Audit log". `ts` is set by the caller.
  - `listeners: list[Callable[[dict], None]]`.
  - `flush() -> None`: writes the buffered records, grouped by the day of `ts`, to `<root>/audit/<day>/<HHMMSS.ffffff>-<uuid4 hex>.jsonl`, one JSON object per line. On a store error it keeps the records in the buffer and logs a warning.
  - `records_for_day(day: str) -> list[dict]`: the records in the files of that day plus the buffered records of that day.
  - `query(user=None, model=None, since: datetime | None = None, until: datetime | None = None, limit: int = 100, offset: int = 0) -> list[dict]`: newest first. Reads only the days in the range. Without `since`, the range starts at the oldest day directory.
  - `users() -> list[dict]`: `{"user", "first_request", "last_request", "requests"}`, sorted by user.
  - `days() -> list[str]`: the day directories that exist, sorted.
- Produces `async run_flusher(log: AuditLog, interval: float) -> None`: calls `await asyncio.to_thread(log.flush)` every `interval` seconds until cancelled, then flushes one last time.
- Produces `utc_now() -> datetime` and `iso(dt: datetime) -> str` (`...Z`, millisecond precision), `parse_time(text: str) -> datetime` (accepts a date or an ISO time, raises `ValueError`).

- [ ] **Step 1: Write the failing tests.** Use a helper `rec(user, ts, model="m", tokens=10)`.

```python
def test_flush_writes_one_file_per_day():
    log.add(rec("alice", "2026-11-10T23:59:59.000Z")); log.add(rec("bob", "2026-11-11T00:00:01.000Z"))
    log.flush()
    paths = store.ls("root/audit")
    assert [p.split("/")[2] for p in paths] == ["2026-11-10", "2026-11-11"]
    assert json.loads(store.get(paths[0]).decode().splitlines()[0])["user"] == "alice"
    log.flush(); assert len(store.ls("root/audit")) == 2        # an empty buffer writes nothing

def test_flush_at_size():       # flush_size=2: the second add writes a file, the buffer is empty
def test_query_filters_and_order():
    # 3 records for alice and bob over 2 days, half flushed and half buffered
    assert [r["ts"] for r in log.query()] == sorted(all_ts, reverse=True)
    assert {r["user"] for r in log.query(user="alice")} == {"alice"}
    assert log.query(since=parse_time("2026-11-11")) == [newest two]
    assert len(log.query(limit=1, offset=1)) == 1 and log.query(limit=1, offset=1)[0] == log.query()[1]
def test_users_summary():       # first_request, last_request and requests for each user
def test_store_error_keeps_records():   # Review Focus 4
    store.fail_puts = True; log.add(rec("alice", T)); log.flush()
    assert log.query() != []                      # still readable from the buffer
    store.fail_puts = False; log.flush(); assert len(store.ls("root/audit")) == 1
def test_listener_called_once_per_record()
def test_parse_time_rejects_garbage():  # pytest.raises(ValueError) for "yesterday"
```

For `fail_puts`, subclass `MemoryBlobStore` in `llm_service/tests/fakes.py` as `FlakyBlobStore`.

- [ ] **Step 2: Run, expect failure. Step 3: Implement.** Guard the buffer with a `threading.Lock`, because `flush` runs in a worker thread. **Step 4: Run, expect pass.**
- [ ] **Step 5: Commit** `llm_service: audit log in blob storage`.

### Task 4: Usage totals

**Files:**
- Create: `llm_service/gateway/usage.py`
- Test: `llm_service/tests/test_usage.py`

**Interfaces:**
- Consumes `AuditLog.records_for_day`, `AuditLog.days`, `BlobStore`.
- Produces `UsageTotals(store: BlobStore, root: str, audit: AuditLog, today: Callable[[], str])`:
  - `load() -> None`: for each day in `audit.days()`, read `<root>/usage/<day>.json` if it exists, else rebuild the day from `audit.records_for_day(day)`. Then write the daily file of each finished day (each day before `today()`) that has none.
  - `record(rec: dict) -> None`: add one audit record to the totals. A listener of `AuditLog`. When the day of the record is after the newest day seen, call `close_days()` first.
  - `close_days() -> None`: write the daily file of each finished day that has none.
  - `summary(since: str | None, until: str | None, user: str | None, model: str | None, group_by: str = "user") -> list[dict]`: `since` and `until` are days, inclusive. `group_by` is `user`, `model` or `day`, else `ValueError`. Each row: `{<group_by>: key, "requests", "failed_requests", "prompt_tokens", "completion_tokens", "total_tokens", "last_request"}`, sorted by `total_tokens` descending, then by key.
  - `user_detail(user: str, since: str | None, until: str | None) -> dict`: `{"user", "totals": row, "by_model": [rows], "by_day": [rows]}`.
- A request is failed when `status >= 400`. A `null` token count adds 0. A record whose `endpoint` does not start with `/v1/` (a model switch event) is not counted.
- Daily file format: `{"day": str, "rows": [{"user", "model", "requests", "failed_requests", "prompt_tokens", "completion_tokens", "total_tokens", "last_request"}]}`.

- [ ] **Step 1: Write the failing tests**

```python
def test_summary_by_user():
    # alice: 2 requests, 10+20 prompt, 5+5 completion, one with status 500; bob: 1 request
    rows = usage.summary(None, None, None, None)
    assert rows[0] == {"user": "alice", "requests": 2, "failed_requests": 1, "prompt_tokens": 30,
                       "completion_tokens": 10, "total_tokens": 40, "last_request": T2}
def test_group_by_model_and_day_and_filters()
def test_group_by_invalid():            # ValueError for group_by="hour"
def test_null_tokens_and_admin_events_ignored()
def test_user_detail_shape()
def test_load_uses_daily_file_and_rebuilds_missing_day():
    # day 1 has a daily file with requests=99 and audit files with 1 record: the summary says 99
    # day 2 (finished) has only audit files: rebuilt, and load() writes root/usage/<day2>.json
    # today has only audit files: rebuilt, and no daily file is written
def test_rollover_writes_daily_file():
    # record a day-1 record, then a day-2 record: root/usage/<day1>.json exists and matches
```

- [ ] **Step 2: Run, expect failure. Step 3: Implement** with a dict keyed by `(day, user, model)` and a lock. **Step 4: Run, expect pass.**
- [ ] **Step 5: Commit** `llm_service: usage totals by user, model and day`.

### Task 5: Weight cache

**Files:**
- Create: `llm_service/gateway/weights.py`
- Test: `llm_service/tests/test_weights.py`

**Interfaces:**
- Consumes `BlobStore`.
- Produces `validate_model_name(model: str) -> str`: accepts exactly `org/name` where each part matches `[A-Za-z0-9][A-Za-z0-9._-]*`, and no part is `.` or `..`. Else `ValueError`. `validate_revision(revision: str) -> str`: same character set, no `/`.
- Produces `Downloader = Callable[[str, str, Path], None]` (model, revision, target directory).
- Produces `hf_downloader(token: str | None) -> Downloader`: calls `huggingface_hub.snapshot_download(repo_id=model, revision=revision, local_dir=target, token=token)`, imported inside the function.
- Produces `WeightCache(store: BlobStore, root: str, local_dir: Path, download: Downloader)`:
  - `ensure(model: str, revision: str) -> Path`: the three steps of the spec section "Weights". Returns `<local_dir>/<org>--<name>/<revision>`. Skips `.cache/` directories and the `.complete` marker when it uploads files. Writes the blob `.complete` last, then the local `.complete`.
  - `cached() -> list[dict]`: `{"model": "org/name", "revision": str}` for each blob directory with a `.complete` marker, sorted.

- [ ] **Step 1: Write the failing tests.** The fake downloader writes `config.json` and `sub/model.safetensors` and counts its calls.

```python
def test_first_use_downloads_and_uploads():
    path = cache.ensure("org/m", "main")
    assert (path / "sub" / "model.safetensors").exists() and calls == 1
    assert store.exists("root/models/org--m/main/sub/model.safetensors")
    assert store.exists("root/models/org--m/main/.complete")
def test_local_hit_touches_nothing():        # second ensure: calls == 1, no store reads of files
def test_blob_hit_after_local_loss():        # delete local_dir, ensure again: calls == 1, files are back
def test_interrupted_upload_is_not_a_hit():
    # blob has files but no .complete: ensure downloads again (calls == 1 on a fresh cache), then .complete exists
def test_cached_lists_only_complete()
def test_validate_model_name():              # Review Focus 2
    for bad in ["../x", "/abs/path", "a/b/c", "org/..", "org", "", "org/na me", "org/-x"]:
        with pytest.raises(ValueError): validate_model_name(bad)
    assert validate_model_name("Qwen/Qwen2.5-1.5B-Instruct") == "Qwen/Qwen2.5-1.5B-Instruct"
```

- [ ] **Step 2: Run, expect failure. Step 3: Implement. Step 4: Run, expect pass.**
- [ ] **Step 5: Commit** `llm_service: weight cache on local disk and blob storage`.

### Task 6: Engine and model manager

**Files:**
- Create: `llm_service/gateway/engine.py`, `llm_service/gateway/models.py`
- Create or extend: `llm_service/tests/fakes.py` (`FakeEngine`)
- Test: `llm_service/tests/test_models.py`, `llm_service/tests/test_engine.py`

**Interfaces:**
- Consumes `WeightCache.ensure`, `validate_model_name`, `validate_revision`, `BlobStore`.
- Produces `Engine` (Protocol): `base_url: str`, `async start(model_dir: Path, served_name: str) -> None` (returns when the engine answers its health check, raises `EngineError` with the tail of the engine output on failure or timeout), `async stop() -> None`, `alive() -> bool`.
- Produces `VllmEngine(port: int, extra_args: list[str], startup_timeout: float = 1800.0)`. `command(model_dir, served_name) -> list[str]` returns `["vllm", "serve", str(model_dir), "--served-model-name", served_name, "--host", "127.0.0.1", "--port", str(port), *extra_args]`. `start` runs it with `asyncio.create_subprocess_exec`, forwards its output to the service log, and polls `GET {base_url}/health` every 2 seconds. `stop` sends SIGTERM, waits 30 seconds, then SIGKILL.
- Produces `ModelManager(weights: WeightCache, engine: Engine, store: BlobStore, root: str, default_model: str, on_event: Callable[[dict], None] = lambda e: None)`:
  - `state: str` (`empty`, `loading`, `ready`, `failed`), `active: dict | None` (`{"model", "revision"}`), `target: dict | None`, `error: str | None`.
  - `async startup() -> None`: loads `<root>/state/active.json`, or `default_model` at revision `main`. It never raises. A failure sets `failed` and `error`.
  - `request_switch(model: str, revision: str = "main") -> asyncio.Task`: validates (raises `ValueError`), raises `SwitchInProgress` when the state is `loading`, sets `loading` and `target`, and starts the switch task of the spec section "Model switch".
  - `check_engine() -> None`: when the state is `ready` and `engine.alive()` is false, sets `failed` with error `engine exited`.
  - `status() -> dict`: `{"state", "active", "target", "error"}`.
- Switch order: `await asyncio.to_thread(weights.ensure, ...)` while the old engine still runs, then `engine.stop()`, then `engine.start()`. On an `ensure` failure the old model keeps serving and the state returns to `ready` (or `empty`) with `error` set. On a `start` failure the manager starts the previous model again. If that also fails, the state is `failed`.
- `on_event` receives `{"event": "model_switch", "model", "revision", "result": "ok" | "failed", "error"}`.

- [ ] **Step 1: Write the failing tests.** `FakeEngine` records `started` and `stopped` calls, has `fail_on: set[str]` of served names for which `start` raises `EngineError`, and `alive_flag`.

```python
async def test_startup_loads_default_and_persists():
    await mgr.startup()
    assert mgr.state == "ready" and mgr.active == {"model": "org/a", "revision": "main"}
    assert json.loads(store.get("root/state/active.json")) == mgr.active
async def test_startup_prefers_active_json()
async def test_switch_ok():
    task = mgr.request_switch("org/b"); assert mgr.state == "loading"; await task
    assert mgr.active["model"] == "org/b" and engine.stopped == 1
async def test_second_switch_during_loading_raises_SwitchInProgress()
async def test_switch_start_failure_rolls_back():
    engine.fail_on = {"org/b"}; await mgr.request_switch("org/b")
    assert mgr.state == "ready" and mgr.active["model"] == "org/a" and "org/b" in mgr.error
    assert json.loads(store.get("root/state/active.json"))["model"] == "org/a"
async def test_download_failure_keeps_old_model_running():   # engine.stopped == 0
async def test_invalid_name_raises_and_touches_nothing():    # Review Focus 2: ValueError, store unchanged
async def test_engine_exit_sets_failed_and_allows_new_switch():   # Review Focus 5
    engine.alive_flag = False; mgr.check_engine()
    assert mgr.state == "failed" and mgr.error == "engine exited"
    engine.alive_flag = True; await mgr.request_switch("org/a"); assert mgr.state == "ready"
async def test_events_emitted_for_ok_and_failed()
# test_engine.py
def test_vllm_command():
    assert VllmEngine(8001, ["--max-model-len", "4096"]).command(Path("/w/m"), "org/m") == [
        "vllm", "serve", "/w/m", "--served-model-name", "org/m", "--host", "127.0.0.1",
        "--port", "8001", "--max-model-len", "4096"]
async def test_start_fails_fast_when_process_exits():
    # an engine subclass whose command() returns [sys.executable, "-c", "print('boom'); raise SystemExit(3)"]
    # start() raises EngineError that contains "boom" in under 10 seconds
```

Async tests use `asyncio.run` inside plain test functions, so no pytest plugin is needed.

- [ ] **Step 2: Run, expect failure. Step 3: Implement. Step 4: Run, expect pass.**
- [ ] **Step 5: Commit** `llm_service: vLLM engine process and model switch`.

### Task 7: Proxy and token counts

**Files:**
- Create: `llm_service/gateway/proxy.py`
- Extend: `llm_service/tests/fakes.py` (`fake_vllm_app()`)
- Test: `llm_service/tests/test_proxy.py`

**Interfaces:**
- Produces `prepare_body(body: dict) -> tuple[dict, bool]`: returns a copy. When `body["stream"]` is true, it sets `stream_options.include_usage = True` and returns whether the client had already asked for it. For a request without streaming it returns the body unchanged and `True`.
- Produces `StreamUsageFilter(keep_usage_chunk: bool)`: `feed(data: bytes) -> bytes` takes raw SSE bytes in any chunking and returns the bytes to send to the client. `usage: dict | None` holds the `usage` object of the last chunk that has one. When `keep_usage_chunk` is false, it drops the chunk that has `"choices": []` and a `usage` object, and it removes a `"usage": null` key from other chunks. It always passes `data: [DONE]`. `flush() -> bytes` returns any bytes still buffered.
- Produces `prompt_of(body: dict) -> dict`: `{"messages": ...}`, `{"prompt": ...}` or `{"input": ...}`, whichever the body has.
- Produces `async forward(client: httpx.AsyncClient, base_url: str, path: str, body: dict, on_done: Callable[[int, dict | None], None]) -> starlette.responses.Response`: sends the prepared body to `base_url + path`. Without streaming it returns the upstream status and JSON body and calls `on_done(status, body.get("usage"))`. With streaming it returns a `StreamingResponse` (`text/event-stream`) through `StreamUsageFilter`, and calls `on_done` exactly once when the stream ends: `(200, usage)` on a normal end, `(499, None)` when the client disconnects (`asyncio.CancelledError` or `GeneratorExit`), `(502, None)` on an upstream error in the middle of the stream. An upstream connection error before the first byte returns 502 with the OpenAI error body.
- `fake_vllm_app()` is a FastAPI app with `/health`, `/v1/chat/completions`, `/v1/completions` and `/v1/embeddings`. It returns `usage` `{"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}`. With `stream` true it sends two content chunks, then the usage chunk only if `stream_options.include_usage` is true, then `[DONE]`. It records each request body in `app.state.requests`. Tests reach it with `httpx.AsyncClient(transport=httpx.ASGITransport(app=fake))`.

- [ ] **Step 1: Write the failing tests**

```python
def test_prepare_body_adds_include_usage():
    out, wanted = prepare_body({"stream": True}); assert out["stream_options"] == {"include_usage": True} and not wanted
    out, wanted = prepare_body({"stream": True, "stream_options": {"include_usage": True}}); assert wanted
    body = {"stream": False}; assert prepare_body(body) == (body, True)
def test_filter_strips_usage_chunk_across_split_bytes():
    f = StreamUsageFilter(keep_usage_chunk=False)
    out = b"".join(f.feed(SSE[i:i + 7]) for i in range(0, len(SSE), 7)) + f.flush()
    assert f.usage == {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}
    assert b'"choices": []' not in out and b'"usage"' not in out and out.endswith(b"data: [DONE]\n\n")
def test_filter_keeps_usage_chunk_when_wanted():     # the output equals the input byte for byte
def test_forward_plain():                            # on_done(200, usage), the body is passed through
def test_forward_stream_reports_usage_and_hides_chunk()
def test_forward_upstream_error_status_passes_through():   # fake returns 400: client gets 400, on_done(400, None)
def test_forward_connect_error_is_502()
def test_client_disconnect_reports_499():            # Review Focus 3
    # take the body iterator of the StreamingResponse, read one chunk, then aclose() it
    # on_done was called once with (499, None)
def test_prompt_of()
```

- [ ] **Step 2: Run, expect failure. Step 3: Implement.** The filter splits on `\n\n` event boundaries and keeps the incomplete tail between `feed` calls. **Step 4: Run, expect pass.**
- [ ] **Step 5: Commit** `llm_service: proxy to the engine with token counts`.

### Task 8: Application

**Files:**
- Create: `llm_service/gateway/app.py`
- Test: `llm_service/tests/test_app.py`

**Interfaces:**
- Consumes everything above.
- Produces `create_app(config: Config, store: BlobStore, engine: Engine, admin: AdminCheck, download: Downloader, http: httpx.AsyncClient | None = None) -> FastAPI`. The lifespan starts `ModelManager.startup()` as a background task (so `/health` answers during the first load), runs `UsageTotals.load()` in a thread, starts `run_flusher`, and on shutdown stops the engine and flushes the log.
- Produces `main(port) -> None`: builds `Config.from_env(os.environ)`, `DatatailrBlobStore`, `VllmEngine`, `AdminCheck(config.owner, config.admin_group, platform_group_members)`, and `hf_downloader(token)`, where the token comes from `datatailr.Secrets().get(config.hf_token_key)` when the key is set. Then `uvicorn.run(app, host="0.0.0.0", port=int(port))`.
- Routes and behavior: exactly the tables of the spec section "Endpoints". Details the spec leaves open:
  - Dependency `current_user`: 401 `{"type": "authentication_error", "code": "missing_user"}` without a user. Dependency `require_admin`: 403 `{"type": "permission_error", "code": "admin_required"}`. `/health` and `/openapi.json` need no user.
  - `/v1/*` POST order of checks: user, JSON object body (400 `invalid_request_error`, code `invalid_json`), `manager.check_engine()`, state `ready` (else 503, `Retry-After: 30`, type `service_unavailable`, code `model_loading` or `no_model`), model match (404 `invalid_request_error`, code `model_not_found`, message names the active model). A body without `model` gets the active model filled in.
  - Each `/v1/*` POST that passed the user check writes one audit record, also for 400, 404 and 503 answers. `request_id` is a uuid4 hex and is returned in the `x-request-id` header.
  - `GET /v1/models`: `{"object": "list", "data": [{"id", "object": "model", "created": 0, "owned_by": <org>, "status", "revision"}]}`. The active model is first. A model that is loading has status `loading`.
  - `POST /admin/model`: 202 with `manager.status()`. `ValueError` gives 400 (code `invalid_model`), `SwitchInProgress` gives 409 (code `switch_in_progress`). Model switch events from `on_event` go to the audit log with `endpoint` `/admin/model` and the admin as `user`.
  - `GET /admin/audit`: `{"data": [...], "limit", "offset"}`. `limit` default 100, maximum 1000. `since` and `until` go through `parse_time`. A `ValueError` gives 400 (code `invalid_time`). Runs in a thread.
  - `GET /admin/usage`, `/admin/usage/{user}`, `/v1/usage/me`: `since` and `until` are days. `{"data": rows}` for the first, the `user_detail` dict for the other two. An invalid `group_by` gives 400.

- [ ] **Step 1: Write the failing tests.** Build the app with `MemoryBlobStore`, `FakeEngine`, the fake downloader, and an `httpx.AsyncClient` on `fake_vllm_app()`. Use `fastapi.testclient.TestClient` as a context manager so the lifespan runs. Headers: `ALICE` (owner, admin), `BOB` (user).

```python
def test_health_needs_no_user():                 # 200 "OK"
def test_chat_completion_roundtrip_and_audit():
    r = c.post("/v1/chat/completions", headers=BOB, json={"model": "org/a", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200 and r.headers["x-request-id"]
    rec = c.get("/admin/audit?user=bob", headers=ALICE).json()["data"][0]
    assert rec["prompt"] == {"messages": [{"role": "user", "content": "hi"}]} and rec["total_tokens"] == 5
def test_streaming_client_sees_no_usage_chunk_but_audit_has_tokens()
def test_missing_model_field_uses_active_model()
def test_wrong_model_is_404_model_not_found()
def test_no_user_is_401()
def test_non_json_and_non_object_body_is_400_and_audited():     # Review Focus 1: body b"hello" and body "[1]"
def test_503_with_retry_after_while_loading():   # FakeEngine.start waits on an asyncio.Event
def test_models_list_marks_active_and_cached()
def test_admin_endpoints_reject_non_admin():     # 403 for each /admin route with BOB
def test_switch_model_202_then_ready_and_event_in_audit()
def test_switch_invalid_name_400_and_conflict_409()
def test_audit_filters_and_invalid_time_400()
def test_audit_users()
def test_usage_by_user_and_user_detail():
    rows = c.get("/admin/usage", headers=ALICE).json()["data"]
    assert rows[0]["user"] == "bob" and rows[0]["total_tokens"] == 5 and rows[0]["requests"] == 1
def test_usage_me_returns_only_the_caller()
def test_openai_sdk_shape_of_errors():           # each 4xx and 5xx body has error.message, error.type, error.code
```

- [ ] **Step 2: Run, expect failure. Step 3: Implement. Step 4: Run the whole suite, expect pass.**
- [ ] **Step 5: Commit** `llm_service: gateway application and admin endpoints`.

### Task 9: Deploy script, README and metadata

**Files:**
- Create: `llm_service/deploy.py`, `llm_service/requirements.txt`, `llm_service/README.md`, `llm_service/metadata.json`
- Modify: `README.md` at the repository root only if it lists the examples (it names `datatailr_demo/` only, so leave it)
- Test: `llm_service/tests/test_deploy.py`

**Interfaces:**
- Consumes `llm_service.gateway.app.main`.
- Produces in `deploy.py`: `service_env(env: Mapping[str, str]) -> dict[str, str]` (the `LLM_*` variables that the container needs: `LLM_DEFAULT_MODEL`, `LLM_BLOB_ROOT`, and `LLM_ADMIN_GROUP`, `LLM_HF_TOKEN_KEY`, `LLM_VLLM_ARGS` when set), `resources(env) -> dict` (`{"memory": "24g", "cpu": 4.0, "vram": "16g"}` by default, from `LLM_MEMORY`, `LLM_CPU`, `LLM_VRAM`), `build_service(env)` and the `__main__` block. Follow `neon_postgres/deploy.py` for the import pattern (`sys.path.append` of the parent directory, `datatailr` imports, `Service(...)`, `service.run()`).
- ACL: the default ACL when `LLM_ACCESS_GROUP` is not set. When it is set: read and access for the owner and that group, write, operate and promote for the owner.
- `requirements.txt`: `fastapi`, `uvicorn`, `httpx`, `huggingface_hub`, `vllm`.
- `metadata.json`: same keys as `neon_postgres/metadata.json`. Name `LLM Service`. Tags: `llm`, `openai`, `vllm`, `gpu`, `service`, `fastapi`, `blob`, `audit`.

- [ ] **Step 1: Write the failing tests** for `service_env` and `resources` (defaults from the spec table, overrides, unset optional variables are absent). The tests import `deploy.py` with `pytest.importorskip("datatailr")`.
- [ ] **Step 2: Run, expect failure. Step 3: Implement `deploy.py`, `requirements.txt`, `metadata.json`. Step 4: Run, expect pass.**
- [ ] **Step 5: Write `README.md`** with these sections, in this order: what the example does (with the architecture diagram of the spec), requirements (GPU node, how to start one with `dt node`, checked against the `datatailr-nodes` skill), deploy (the environment variable table), use from a job inside Datatailr (`OpenAI(base_url="http://llm-service/v1", api_key="unused")`), use from outside (the Cookie header form of the spec, and the limit for tools that take only an API key), switch the model (`curl` on `/admin/model`, and `LLM_HF_TOKEN_KEY` for gated models), list the models, read the audit log, read the usage metrics, where the data lives in blob storage, limits (the 5 second loss window, one model at a time, prompts are stored and answers are not), run the tests.
- [ ] **Step 6: Run a local smoke test without a GPU.** Start the app with `MemoryBlobStore`, `FakeEngine` and `fake_vllm_app()` through a small script in the scratchpad directory, then call `/v1/models`, `/v1/chat/completions` with the `openai` Python package if it is installed (else `curl`), and `/admin/usage`. Expected: 200 for each, and the usage row shows the request.
- [ ] **Step 7: Run** `python -m pytest llm_service/tests -q` and `python -m compileall -q llm_service`. Expected: all pass, no output from compileall.
- [ ] **Step 8: Commit** `llm_service: deploy script, README and metadata`.

## After the plan

The deploy on the Azure installation and the run on a real GPU are done by the user. Things to confirm there, in order: the image builds with `vllm`, the container sees the GPU (`nvidia-smi` in the service log or a vLLM start without a CUDA error), the first start downloads from Hugging Face and uploads to blob storage, a restart loads from blob storage, and a streaming chat request returns tokens.
