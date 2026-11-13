"""Tests for the readers of the engine metrics and the GPU memory."""

from __future__ import annotations

import asyncio
import logging
import subprocess
import time

import httpx

from llm_service.gateway.engine_metrics import (
    EngineReader,
    GpuReader,
    parse_gpu_memory,
    parse_metrics,
)

LOGGER_NAME = "llm_service.gateway.engine_metrics"

FULL_TEXT = """\
# HELP vllm:num_requests_running Number of requests in model execution batches.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="org/m"} 3.0
vllm:num_requests_waiting{model_name="org/m"} 2.0
vllm:prompt_tokens_total{model_name="org/m"} 9100.0
vllm:generation_tokens_total{model_name="org/m"} 5200.0
"""


def test_parse_metrics_all_four_names():
    numbers, names = parse_metrics(FULL_TEXT)
    assert numbers == {
        "running": 3.0,
        "waiting": 2.0,
        "prompt_tokens_total": 9100.0,
        "completion_tokens_total": 5200.0,
    }
    assert names == [
        "vllm:generation_tokens_total",
        "vllm:num_requests_running",
        "vllm:num_requests_waiting",
        "vllm:prompt_tokens_total",
    ]


def test_parse_metrics_uses_the_second_name_of_a_pair():
    text = "vllm:prompt_tokens 70\nvllm:generation_tokens 30\n"
    numbers, _ = parse_metrics(text)
    assert numbers["prompt_tokens_total"] == 70.0
    assert numbers["completion_tokens_total"] == 30.0


def test_parse_metrics_first_name_wins():
    text = "vllm:prompt_tokens 1\nvllm:prompt_tokens_total 2\n"
    numbers, _ = parse_metrics(text)
    assert numbers["prompt_tokens_total"] == 2.0


def test_parse_metrics_missing_name_is_none():
    text = "vllm:num_requests_running 1\nvllm:num_requests_waiting 0\n"
    numbers, _ = parse_metrics(text)
    assert numbers["running"] == 1.0
    assert numbers["waiting"] == 0.0
    assert numbers["prompt_tokens_total"] is None
    assert numbers["completion_tokens_total"] is None


def test_parse_metrics_sums_label_sets():
    text = (
        'vllm:num_requests_running{engine="0",model_name="a"} 2\n'
        'vllm:num_requests_running{engine="1",model_name="a"} 3\n'
    )
    numbers, _ = parse_metrics(text)
    assert numbers["running"] == 5.0


def test_parse_metrics_label_values_with_spaces_commas_quotes_and_braces():
    text = (
        'vllm:num_requests_running{model_name="my model, v2 } x",note="say \\"hi\\""} 4\n'
        'vllm:num_requests_waiting{a="b \\\\"} 1\n'
    )
    numbers, _ = parse_metrics(text)
    assert numbers["running"] == 4.0
    assert numbers["waiting"] == 1.0


def test_parse_metrics_ignores_comments_and_bad_lines():
    text = (
        "# TYPE vllm:num_requests_running gauge\n"
        "   # an indented comment\n"
        "this is not a metric line\n"
        "vllm:num_requests_running\n"
        "vllm:num_requests_running abc\n"
        "vllm:num_requests_running{broken 5\n"
        "\n"
        "vllm:num_requests_running 7\n"
    )
    numbers, names = parse_metrics(text)
    assert numbers["running"] == 7.0
    assert names == ["vllm:num_requests_running"]


def test_parse_metrics_number_forms_and_timestamp():
    text = (
        "vllm:prompt_tokens_total 1.5e+06 1700000000000\n"
        "vllm:num_requests_running +2\n"
    )
    numbers, _ = parse_metrics(text)
    assert numbers["prompt_tokens_total"] == 1500000.0
    assert numbers["running"] == 2.0


def test_parse_metrics_non_finite_is_missing():
    text = (
        "vllm:num_requests_running NaN\n"
        "vllm:num_requests_waiting +Inf\n"
        "vllm:prompt_tokens_total -Inf\n"
        'vllm:generation_tokens_total{a="1"} NaN\n'
        'vllm:generation_tokens_total{a="2"} 5\n'
    )
    numbers, names = parse_metrics(text)
    assert numbers["running"] is None
    assert numbers["waiting"] is None
    assert numbers["prompt_tokens_total"] is None
    assert numbers["completion_tokens_total"] == 5.0
    assert "vllm:num_requests_running" in names


def test_parse_metrics_empty_text():
    numbers, names = parse_metrics("")
    assert numbers == {
        "running": None,
        "waiting": None,
        "prompt_tokens_total": None,
        "completion_tokens_total": None,
    }
    assert names == []


def test_parse_metrics_names_only_vllm_prefix_sorted_and_unique():
    text = (
        'python_gc_objects_collected_total{generation="0"} 10\n'
        'vllm:zeta{a="1"} 1\n'
        'vllm:zeta{a="2"} 1\n'
        "vllm:alpha 1\n"
        "process_cpu_seconds_total 3\n"
    )
    _, names = parse_metrics(text)
    assert names == ["vllm:alpha", "vllm:zeta"]


def test_parse_gpu_memory_one_gpu():
    assert parse_gpu_memory("1024, 15360\n") == (1024, 15360)


def test_parse_gpu_memory_two_gpus_are_summed():
    assert parse_gpu_memory("1024, 15360\n2048, 15360\n") == (3072, 30720)


def test_parse_gpu_memory_spaces():
    assert parse_gpu_memory("  100 ,  200  \r\n\n  300,400\n\n") == (400, 600)


def test_parse_gpu_memory_empty_text():
    assert parse_gpu_memory("") is None
    assert parse_gpu_memory("  \n\n") is None


def test_parse_gpu_memory_words():
    assert parse_gpu_memory("No devices were found\n") is None
    assert parse_gpu_memory("[N/A], [N/A]\n") is None
    assert parse_gpu_memory("1024, 15360\nbad, line\n") is None
    assert parse_gpu_memory("1024\n") is None


def _reader(handler, timeout: float = 2.0) -> tuple[EngineReader, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return EngineReader(client, "http://engine:8001/", timeout=timeout), client


def _read(reader: EngineReader) -> dict:
    return asyncio.run(reader.read())


def _run_reads(handler, count: int = 1) -> list[dict]:
    async def go() -> list[dict]:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        reader = EngineReader(client, "http://engine:8001")
        try:
            return [await reader.read() for _ in range(count)]
        finally:
            await client.aclose()

    return asyncio.run(go())


def test_engine_reader_good_answer():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, text=FULL_TEXT)

    (numbers,) = _run_reads(handler)
    assert seen == ["http://engine:8001/metrics"]
    assert numbers == {
        "running": 3.0,
        "waiting": 2.0,
        "prompt_tokens_total": 9100.0,
        "completion_tokens_total": 5200.0,
    }


def test_engine_reader_trailing_slash_in_base_url():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, text=FULL_TEXT)

    reader, client = _reader(handler)
    try:
        _read(reader)
    finally:
        asyncio.run(client.aclose())
    assert seen == ["http://engine:8001/metrics"]


ALL_NONE = {
    "running": None,
    "waiting": None,
    "prompt_tokens_total": None,
    "completion_tokens_total": None,
}


def test_engine_reader_status_500():
    (numbers,) = _run_reads(lambda request: httpx.Response(500, text="boom"))
    assert numbers == ALL_NONE


def test_engine_reader_transport_error():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    (numbers,) = _run_reads(handler)
    assert numbers == ALL_NONE


def test_engine_reader_timeout():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow")

    (numbers,) = _run_reads(handler)
    assert numbers == ALL_NONE


def _messages(caplog, level: int) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == LOGGER_NAME and r.levelno == level
    ]


def test_engine_reader_logs_missing_numbers_once_and_again_on_change(caplog):
    caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)
    texts = iter([
        "vllm:num_requests_running 1\n",
        "vllm:num_requests_running 2\n",
        "vllm:num_requests_running 3\nvllm:num_requests_waiting 0\n",
    ])
    _run_reads(lambda request: httpx.Response(200, text=next(texts)), count=3)
    warnings = _messages(caplog, logging.WARNING)
    assert len(warnings) == 2
    assert "vllm:num_requests_running" in warnings[0]
    assert "prompt_tokens_total" in warnings[0]
    assert "waiting" in warnings[0]
    assert "waiting" not in warnings[1].split("saw")[0]


def test_engine_reader_missing_warning_repeats_after_the_set_clears(caplog):
    caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)
    texts = iter([
        "vllm:num_requests_running 1\n",
        FULL_TEXT,
        "vllm:num_requests_running 1\n",
    ])
    _run_reads(lambda request: httpx.Response(200, text=next(texts)), count=3)
    assert len(_messages(caplog, logging.WARNING)) == 2


def test_engine_reader_logs_no_warning_when_all_numbers_exist(caplog):
    caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)
    _run_reads(lambda request: httpx.Response(200, text=FULL_TEXT), count=3)
    assert _messages(caplog, logging.WARNING) == []
    assert _messages(caplog, logging.INFO) == []


def test_engine_reader_logs_one_warning_for_a_failure_and_one_info_on_recovery(caplog):
    caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)
    answers = iter([
        httpx.Response(500),
        httpx.Response(500),
        httpx.Response(200, text=FULL_TEXT),
        httpx.Response(200, text=FULL_TEXT),
    ])
    _run_reads(lambda request: next(answers), count=4)
    assert len(_messages(caplog, logging.WARNING)) == 1
    assert len(_messages(caplog, logging.INFO)) == 1


def _completed(stdout: str = "", returncode: int = 0, stderr: str = ""):
    return subprocess.CompletedProcess(
        args=["nvidia-smi"], returncode=returncode, stdout=stdout, stderr=stderr
    )


def test_gpu_reader_good_run():
    calls = []

    def run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return _completed("1024, 15360\n2048, 15360\n")

    assert GpuReader(run=run, timeout=3.0).read() == (3072, 30720)
    cmd, kwargs = calls[0]
    assert cmd == [
        "nvidia-smi",
        "--query-gpu=memory.used,memory.total",
        "--format=csv,noheader,nounits",
    ]
    assert kwargs["timeout"] == 3.0
    assert kwargs["capture_output"] is True
    assert kwargs["text"] is True
    assert "shell" not in kwargs


def test_gpu_reader_non_zero_exit():
    reader = GpuReader(run=lambda cmd, **kw: _completed("1, 2\n", returncode=9))
    assert reader.read() is None


def test_gpu_reader_bad_text():
    reader = GpuReader(run=lambda cmd, **kw: _completed("No devices were found\n"))
    assert reader.read() is None


def test_gpu_reader_file_not_found():
    def run(cmd, **kwargs):
        raise FileNotFoundError("nvidia-smi")

    assert GpuReader(run=run).read() is None


def test_gpu_reader_timeout():
    def run(cmd, **kwargs):
        raise subprocess.TimeoutExpired(cmd, 5.0)

    assert GpuReader(run=run).read() is None


def test_gpu_reader_missing_binary_is_one_warning_for_the_process(caplog):
    caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)

    def run(cmd, **kwargs):
        raise FileNotFoundError("nvidia-smi")

    reader = GpuReader(run=run)
    for _ in range(5):
        assert reader.read() is None
    assert len(_messages(caplog, logging.WARNING)) == 1
    assert _messages(caplog, logging.INFO) == []


def test_gpu_reader_missing_binary_is_not_retried():
    calls = []

    def run(cmd, **kwargs):
        calls.append(cmd)
        raise FileNotFoundError("nvidia-smi")

    reader = GpuReader(run=run)
    reader.read()
    reader.read()
    assert len(calls) == 1


def test_gpu_reader_one_warning_for_a_failure_and_one_info_on_recovery(caplog):
    caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)
    results = iter([
        _completed(returncode=1),
        _completed(returncode=1),
        _completed("10, 20\n"),
        _completed("10, 20\n"),
    ])
    reader = GpuReader(run=lambda cmd, **kw: next(results))
    reader.read()
    reader.read()
    assert reader.read() == (10, 20)
    assert reader.read() == (10, 20)
    assert len(_messages(caplog, logging.WARNING)) == 1
    assert len(_messages(caplog, logging.INFO)) == 1


def test_gpu_reader_warns_again_after_a_recovery(caplog):
    caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)

    def timeout(cmd, **kwargs):
        raise subprocess.TimeoutExpired(cmd, 5.0)

    results = iter([None, _completed("1, 2\n"), None])

    def run(cmd, **kwargs):
        item = next(results)
        if item is None:
            return timeout(cmd)
        return item

    reader = GpuReader(run=run)
    reader.read()
    reader.read()
    reader.read()
    assert len(_messages(caplog, logging.WARNING)) == 2
    assert len(_messages(caplog, logging.INFO)) == 1


def test_gpu_reader_any_exception_of_run_is_a_failure(caplog):
    caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)

    def run(cmd, **kwargs):
        raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad byte")

    reader = GpuReader(run=run)
    assert reader.read() is None
    assert reader.read() is None
    assert len(_messages(caplog, logging.WARNING)) == 1


def test_gpu_reader_passes_a_copy_of_the_command():
    seen = []

    def run(cmd, **kwargs):
        seen.append(list(cmd))
        cmd.append("changed")
        return _completed("1, 2\n")

    reader = GpuReader(run=run)
    reader.read()
    reader.read()
    assert seen[1] == [
        "nvidia-smi",
        "--query-gpu=memory.used,memory.total",
        "--format=csv,noheader,nounits",
    ]


def test_engine_reader_total_time_limit():
    async def go():
        async def slow(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(5)
            return httpx.Response(200, text=FULL_TEXT)

        client = httpx.AsyncClient(transport=httpx.MockTransport(slow))
        reader = EngineReader(client, "http://engine:8001", timeout=0.05)
        try:
            begin = time.monotonic()
            numbers = await reader.read()
            return numbers, time.monotonic() - begin
        finally:
            await client.aclose()

    numbers, elapsed = asyncio.run(go())
    assert numbers == ALL_NONE
    assert elapsed < 1.0


def test_parse_metrics_created_name_is_not_a_token_counter():
    numbers, names = parse_metrics("vllm:prompt_tokens_total_created 5\n")
    assert numbers["prompt_tokens_total"] is None
    assert names == ["vllm:prompt_tokens_total_created"]


def test_parse_metrics_overflowing_sum_is_missing():
    text = "vllm:num_requests_running{a=\"1\"} 1e308\nvllm:num_requests_running{a=\"2\"} 1e308\n"
    numbers, _ = parse_metrics(text)
    assert numbers["running"] is None


def test_parse_metrics_splits_on_newline_only():
    text = "vllm:num_requests_running{a=\"x\x0by\x1cz\u2028w\"} 4\n"
    numbers, _ = parse_metrics(text)
    assert numbers["running"] == 4.0
