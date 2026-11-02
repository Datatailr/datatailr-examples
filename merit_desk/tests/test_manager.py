import inspect
import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from merit_desk.common.runs import finish_run, list_runs, load_run, new_run, save_run
from merit_desk.common.store import LocalDirStore, MemoryStore, env_prefix
from merit_desk.desk_app import manager as manager_module
from merit_desk.desk_app.manager import DESK_JOBS, RunManager, TooManyRuns
from merit_desk.desk_app.runner import Runner, RunRefused
from merit_desk.workflows.desk_runs import job_name

BACKTEST = {"market": "DE", "tenor": 2, "threshold": 1.5}
SCENARIO = {"market": "DE", "shift_c": -3.0, "overrides": []}
DAILY = "Merit Desk Daily Curve"
SWEEP = "Merit Desk Strategy Sweep"
NOT_FOUND = "The run's job was not found."
NO_RESULT = "The run ended without a result."
TIMED_OUT = "The run timed out after 15 minutes."
DAMAGED = "The run record is damaged."
KEEP = timedelta(minutes=10)  # a finished run's workflow stays this long, at least


class Clock:
    """The clock of the manager. It starts now and moves only when a test moves it."""

    def __init__(self):
        self.time = datetime.now(timezone.utc)

    def __call__(self):
        return self.time

    def advance(self, **delta):
        self.time += timedelta(**delta)


class FlakyStore(MemoryStore):
    """A store that fails when a test says so."""

    def __init__(self):
        super().__init__()
        self.unreadable = set()  # ids of runs whose records cannot be read
        self.listing_fails = False

    def get_text(self, name):
        if any(run_id in name for run_id in self.unreadable):
            raise OSError("the store is down")
        return super().get_text(name)

    def list(self, prefix):
        if self.listing_fails:
            raise OSError("the store is down")
        return super().list(prefix)


class FakeRunner:
    """Stands in for a Runner. It answers from tables and keeps the calls it gets."""

    def __init__(self):
        self.submitted, self.cleaned, self.asked = [], [], []
        self.desk_started, self.stale_calls = [], []
        self.states = {}  # run id -> what state() says. A run that is not submitted is missing
        self.errors = {}  # run id -> an exception that state() raises
        self.refuse = None  # a text: submit() and start_desk_job() refuse with it
        self.submit_error = None  # an exception that submit() raises, other than a refusal
        self.cleanup_error = self.stale_error = None
        self.on_submit = self.on_state = None  # called inside, like a task that is quick
        self.delay = 0  # seconds that submit() takes

    def submit(self, record):
        if self.on_submit:
            self.on_submit(record)
        time.sleep(self.delay)
        if self.submit_error:
            raise self.submit_error
        if self.refuse:
            raise RunRefused(self.refuse)
        self.submitted.append(record["id"])
        return job_name(record)

    def state(self, record):
        self.asked.append(record["id"])
        if self.on_state:
            self.on_state(record)
        if record["id"] in self.errors:
            raise self.errors[record["id"]]
        submitted = record["id"] in self.submitted or record["kind"] == "job"
        return self.states.get(record["id"], "running" if submitted else "missing")

    def cleanup(self, record):
        self.cleaned.append(record["id"])
        if self.cleanup_error:
            raise self.cleanup_error

    def remove_stale(self, max_age):
        self.stale_calls.append(max_age)
        if self.stale_error:
            raise self.stale_error
        return 0

    def start_desk_job(self, workflow_name):
        if self.refuse:
            raise RunRefused(self.refuse)
        self.desk_started.append(workflow_name)
        return workflow_name

    def desk_job_state(self, workflow_name):
        return {"last_state": None, "last_time": None}

    def shutdown(self):
        pass


@pytest.fixture
def store():
    return FlakyStore()


@pytest.fixture
def runner():
    return FakeRunner()


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def manager(store, runner, clock):
    return RunManager(store, runner, now=clock)


def edit(store, run_id, drop=(), **changes):
    """Change a stored record the way another process would: `updated` stays as it is."""
    record = load_run(run_id, store)
    record.update(changes)
    for key in drop:
        del record[key]
    store.put_text(f"{env_prefix()}runs/{run_id}.json", json.dumps(record))


def start_runs(manager, count):
    return [manager.start("backtest", BACKTEST, "denis") for _ in range(count)]


def kept_then_removed(manager, runner, clock, record):
    """The workflow of an ended run that the platform has ended too: kept, then removed once."""
    runner.states[record["id"]] = "done"
    clock.advance(minutes=9)
    assert manager.poll() == [] and runner.cleaned == []
    clock.advance(minutes=1)
    assert manager.poll() == [] and runner.cleaned == [record["id"]]
    clock.advance(hours=1)
    assert manager.poll() == [] and runner.cleaned == [record["id"]]  # once


def call_apart(call, problems):
    """Run `call` in another thread, for a call that must not wait for the lock of the caller.
    When it is stuck, note it in `problems` and go on: a regression fails the test, no hang."""
    outcome = []

    def target():
        try:
            outcome.append(call())
        except BaseException as error:
            outcome.append(error)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout=5)
    if thread.is_alive():
        problems.append("the call is stuck")
        return None
    if isinstance(outcome[0], BaseException):
        raise outcome[0]
    return outcome[0]


# ---- start and the cap

def test_three_runs_are_active_and_the_fourth_is_refused(manager, runner, store):
    start_runs(manager, 3)
    with pytest.raises(TooManyRuns) as refusal:
        manager.start("scenario", SCENARIO, "denis")
    assert str(refusal.value) == "Three runs are in progress. Wait for one to finish."
    assert len(runner.submitted) == 3 and len(manager.active()) == 3
    assert len(list_runs(store)) == 3  # the refused run left no record and no job


def test_a_finished_run_frees_its_slot(manager, store):
    first, *_ = start_runs(manager, 3)
    finish_run(first["id"], store, result={})
    manager.poll()
    assert manager.start("scenario", SCENARIO, "denis")["status"] == "running"
    with pytest.raises(TooManyRuns):
        manager.start("scenario", SCENARIO, "denis")


def test_start_stores_the_record_before_the_run_and_then_the_job_name_and_running(
        manager, runner, store):
    statuses = []
    runner.on_submit = lambda record: statuses.append(load_run(record["id"], store)["status"])
    record = manager.start("backtest", BACKTEST, "denis")
    assert statuses == ["queued"]  # the task finds its inputs in the store
    assert load_run(record["id"], store) == record
    assert (record["status"], record["job_name"]) == ("running", job_name(record))
    assert (record["kind"], record["label"]) == ("backtest", "Backtest DE M2 t1.5")
    assert (record["inputs"], record["started_by"]) == (BACKTEST, "denis")
    assert runner.submitted == [record["id"]] and manager.active() == [record]


@pytest.mark.parametrize("kind", ["job", "other"])
def test_a_click_starts_only_a_scenario_or_a_backtest(manager, runner, store, kind):
    with pytest.raises(ValueError, match="scenario"):
        manager.start(kind, {"name": DAILY}, "denis")
    assert runner.submitted == [] and list_runs(store) == []


def test_a_task_that_is_quicker_than_the_manager_keeps_its_result(manager, runner, store):
    def quick_task(record):
        finish_run(record["id"], store, result={"total_pnl_eur": 7.0})

    runner.on_submit = quick_task
    record = manager.start("backtest", BACKTEST, "denis")
    assert (record["status"], record["result"]) == ("done", {"total_pnl_eur": 7.0})
    assert record["job_name"] == job_name(record)  # the job name is stored all the same
    assert load_run(record["id"], store) == record
    assert [r["id"] for r in manager.poll()] == [record["id"]]  # and the poll ends it
    assert runner.cleaned == [] and manager.active() == []  # the job goes later


def test_a_refused_run_is_failed_with_the_message_of_the_platform(manager, runner, store):
    runner.refuse = "Budget 'Trading desk' is used up."
    record = manager.start("backtest", BACKTEST, "denis")
    assert (record["status"], record["error"]) == ("failed", "Budget 'Trading desk' is used up.")
    assert load_run(record["id"], store) == record
    assert manager.active() == [] and manager.poll() == []
    assert runner.cleaned == [record["id"]]  # the platform may have saved a workflow
    runner.refuse = None
    assert len(start_runs(manager, 3)) == 3  # a refused run holds no slot


def test_an_error_while_cleaning_up_a_refused_run_is_logged_and_ignored(
        manager, runner, caplog):
    runner.refuse, runner.cleanup_error = "No budget.", RuntimeError("the platform is down")
    with caplog.at_level(logging.WARNING):
        record = manager.start("backtest", BACKTEST, "denis")
    assert (record["status"], record["error"]) == ("failed", "No budget.")
    assert record["id"] in caplog.text and "RuntimeError" in caplog.text


def test_an_error_of_submit_that_is_no_refusal_fails_the_record_frees_the_slot_and_is_raised(
        manager, runner, store):
    runner.submit_error = RuntimeError("cannot schedule new futures after shutdown")
    with pytest.raises(RuntimeError, match="cannot schedule"):
        manager.start("backtest", BACKTEST, "denis")
    [record] = list_runs(store)
    assert record["status"] == "failed"
    assert record["error"] == "RuntimeError: cannot schedule new futures after shutdown"
    assert manager.active() == []
    runner.submit_error = None
    assert len(start_runs(manager, 3)) == 3  # the failed click holds no slot


def test_start_gives_back_a_copy_of_the_record_it_keeps(manager):
    record, = start_runs(manager, 1)
    record["status"] = "failed"
    assert manager.active()[0]["status"] == "running"


def test_the_options_after_max_active_are_keyword_only(store, runner):
    with pytest.raises(TypeError):
        RunManager(store, runner, 3, timedelta(minutes=5))  # a timeout, but by position


def test_a_store_error_while_writing_the_first_record_leaves_nothing_active():
    class Down(MemoryStore):
        def put_text(self, name, text):
            raise OSError("the store is down")

    broken = RunManager(Down(), FakeRunner(), now=Clock())
    with pytest.raises(OSError):
        broken.start("backtest", BACKTEST, "denis")
    assert broken.active() == []


# ---- poll

def test_a_run_that_the_task_finished_is_returned_once_and_cleaned_up_later(
        manager, runner, store, clock):
    record, = start_runs(manager, 1)
    assert manager.poll() == [] and runner.cleaned == []  # still running: nothing changed
    finish_run(record["id"], store, result={"total_pnl_eur": 7.0})
    done = manager.poll()
    assert [(r["id"], r["status"], r["result"]) for r in done] == [
        (record["id"], "done", {"total_pnl_eur": 7.0})]
    assert manager.active() == [] and manager.poll() == []  # not again
    kept_then_removed(manager, runner, clock, record)


def test_poll_gives_back_only_the_runs_that_changed(manager, store):
    first, second, third = start_runs(manager, 3)
    finish_run(second["id"], store, error="KeyError: 'tenor'")
    [failed] = manager.poll()
    assert (failed["id"], failed["status"], failed["error"]) == (
        second["id"], "failed", "KeyError: 'tenor'")
    assert [r["id"] for r in manager.active()] == [first["id"], third["id"]]


@pytest.mark.parametrize("state, message", [("failed", NO_RESULT), ("missing", NOT_FOUND)])
def test_a_job_that_failed_or_is_missing_fails_its_run_at_the_next_poll(
        manager, runner, store, clock, state, message):
    record, = start_runs(manager, 1)
    runner.states[record["id"]] = state
    [failed] = manager.poll()
    assert (failed["status"], failed["error"]) == ("failed", message)
    assert load_run(record["id"], store)["error"] == message
    assert runner.cleaned == [] and manager.active() == []
    clock.advance(minutes=10)  # the run is over on the platform: its job goes after the keep
    assert manager.poll() == [] and runner.cleaned == [record["id"]]


def test_a_job_that_is_done_but_has_no_result_yet_is_waited_for(manager, runner):
    record, = start_runs(manager, 1)
    runner.states[record["id"]] = "done"  # the record may be a moment behind the job
    assert manager.poll() == [] and len(manager.active()) == 1


def test_a_task_that_finishes_while_the_runner_is_asked_keeps_its_own_error(
        manager, runner, store):
    record, = start_runs(manager, 1)
    runner.states[record["id"]] = "failed"  # the job fails because the task raised
    runner.on_state = lambda rec: finish_run(rec["id"], store, error="KeyError: 'tenor'")
    [failed] = manager.poll()
    assert failed["error"] == "KeyError: 'tenor'"
    assert load_run(record["id"], store)["error"] == "KeyError: 'tenor'"
    assert manager.active() == []


def test_a_run_without_a_change_for_15_minutes_fails_and_its_job_is_cleaned_up(
        manager, runner, store, clock):
    record, = start_runs(manager, 1)
    clock.advance(minutes=14)
    assert manager.poll() == [] and runner.cleaned == []
    clock.advance(minutes=2)
    [failed] = manager.poll()
    assert (failed["status"], failed["error"]) == ("failed", TIMED_OUT)
    assert load_run(record["id"], store)["error"] == TIMED_OUT
    assert manager.active() == []
    clock.advance(minutes=15)  # the platform still runs it: stopped and removed as a backstop
    assert manager.poll() == [] and runner.cleaned == [record["id"]]


def test_the_timeout_text_names_the_timeout_of_the_manager(store, runner, clock):
    short = RunManager(store, runner, timeout=timedelta(minutes=5), now=clock)
    record = short.start("backtest", BACKTEST, "denis")
    clock.advance(minutes=6)
    [failed] = short.poll()
    assert (failed["id"], failed["error"]) == (record["id"], "The run timed out after 5 minutes.")


def test_the_timeout_counts_from_the_last_change_of_the_record(manager, store, clock):
    record, = start_runs(manager, 1)
    clock.advance(minutes=10)
    edit(store, record["id"], updated=clock().isoformat(timespec="seconds"))
    clock.advance(minutes=10)  # 20 minutes after the start, 10 after the change
    assert [r["id"] for r in manager.poll()] == [record["id"]]  # the change is news
    clock.advance(minutes=6)
    [failed] = manager.poll()
    assert failed["error"] == TIMED_OUT


def test_the_timeout_counts_from_created_when_the_record_has_no_updated_time(
        manager, store, clock):
    record, = start_runs(manager, 1)
    edit(store, record["id"], drop=["updated"],
         created=(clock() - timedelta(minutes=16)).isoformat(timespec="seconds"))
    [failed] = manager.poll()
    assert failed["error"] == TIMED_OUT


def test_a_desk_job_record_is_tracked_like_a_run_but_never_cleaned_up(
        manager, runner, store, clock):
    ended, failed, stuck = (manager.start_desk_job("daily-curve", "denis") for _ in range(3))
    runner.states.update({ended["id"]: "done", failed["id"]: "failed"})  # the stuck one is running
    results = {r["id"]: r for r in manager.poll()}
    outcome = {run_id: (r["status"], r["error"]) for run_id, r in results.items()}
    assert outcome == {ended["id"]: ("done", None), failed["id"]: ("failed", NO_RESULT)}
    assert load_run(ended["id"], store)["status"] == "done"
    assert runner.cleaned == [] and [r["id"] for r in manager.active()] == [stuck["id"]]
    clock.advance(minutes=20)
    manager.poll()
    assert runner.cleaned == []  # not later either


def test_a_desk_job_record_is_still_running_after_15_minutes_and_fails_after_6_hours(
        manager, runner, clock):
    record = manager.start_desk_job("strategy-sweep", "denis")
    clock.advance(minutes=16)  # a click run is over by now, a desk job is not
    assert manager.poll() == [] and [r["id"] for r in manager.active()] == [record["id"]]
    clock.advance(hours=5)  # 5 hours 16 minutes
    assert manager.poll() == []
    clock.advance(hours=1)  # 6 hours 16 minutes
    [failed] = manager.poll()
    assert (failed["id"], failed["status"]) == (record["id"], "failed")
    assert failed["error"] == "The run timed out after 360 minutes."
    clock.advance(hours=1)
    assert manager.poll() == []
    assert runner.cleaned == [] and manager.active() == []  # and its workflow stays


@pytest.mark.parametrize("stored", [
    {},  # no status
    {"status": "weird"},
    {"status": None},
    {"status": "running", "updated": "yesterday"},  # no time to measure from
    {"status": "running", "updated": 12, "created": 12},
])
def test_a_record_that_is_damaged_is_failed_and_cleaned_up(
        manager, runner, store, clock, stored):
    record, = start_runs(manager, 1)
    store.put_text(f"{env_prefix()}runs/{record['id']}.json", json.dumps(stored))
    [failed] = manager.poll()
    assert (failed["id"], failed["status"], failed["error"]) == (record["id"], "failed", DAMAGED)
    assert load_run(record["id"], store)["error"] == DAMAGED
    assert load_run(record["id"], store)["kind"] == "backtest"  # a whole record again
    assert manager.active() == []
    kept_then_removed(manager, runner, clock, record)


def test_a_record_that_cannot_be_read_is_no_news_until_the_runner_or_the_clock_ends_it(
        manager, runner, store):
    record, = start_runs(manager, 1)
    store.put_text(f"{env_prefix()}runs/{record['id']}.json", "not json {")
    assert manager.poll() == [] and len(manager.active()) == 1
    runner.states[record["id"]] = "missing"
    [failed] = manager.poll()
    assert (failed["status"], failed["error"]) == ("failed", NOT_FOUND)
    assert load_run(record["id"], store)["status"] == "failed"  # and it is whole again


def test_poll_returns_a_record_that_changed_in_the_store_once(manager, runner, store):
    record = {**new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, "denis"), "job_name": "x"}
    save_run(record, store)
    manager.resume()
    runner.states[record["id"]] = "running"  # the job exists
    edit(store, record["id"], status="running")
    [changed] = manager.poll()
    assert (changed["id"], changed["status"]) == (record["id"], "running")
    assert manager.active()[0]["status"] == "running"
    assert manager.poll() == [] and runner.cleaned == []  # it is not over


def test_poll_goes_on_after_an_error_with_one_record_and_retries_it(
        manager, runner, store, caplog):
    store_down, platform_down, quick = start_runs(manager, 3)
    store.unreadable.add(store_down["id"])
    runner.errors[platform_down["id"]] = RuntimeError("the platform is down")
    finish_run(quick["id"], store, result={})  # the task finishes
    with caplog.at_level(logging.WARNING):
        assert [r["id"] for r in manager.poll()] == [quick["id"]]  # no error reaches the caller
    assert store_down["id"] in caplog.text and "OSError" in caplog.text
    assert platform_down["id"] in caplog.text and "RuntimeError" in caplog.text
    assert [r["id"] for r in manager.active()] == [store_down["id"], platform_down["id"]]
    store.unreadable.clear()  # both are well again
    del runner.errors[platform_down["id"]]
    runner.states[platform_down["id"]] = "missing"
    assert [r["id"] for r in manager.poll()] == [platform_down["id"]]  # retried at the next poll
    assert [r["id"] for r in manager.active()] == [store_down["id"]]


def test_a_run_whose_state_keeps_failing_still_times_out_and_is_cleaned_up(
        manager, runner, store, clock, caplog):
    record, = start_runs(manager, 1)
    runner.errors[record["id"]] = RuntimeError("the platform is down")
    clock.advance(minutes=14)
    with caplog.at_level(logging.WARNING):
        assert manager.poll() == [] and len(manager.active()) == 1  # logged, and kept for now
    assert record["id"] in caplog.text and "RuntimeError" in caplog.text
    clock.advance(minutes=2)
    [failed] = manager.poll()
    assert (failed["id"], failed["status"], failed["error"]) == (record["id"], "failed", TIMED_OUT)
    assert load_run(record["id"], store)["error"] == TIMED_OUT
    assert manager.active() == []  # the slot is free again
    clock.advance(minutes=15)  # the platform cannot be asked: the backstop removes the job
    assert manager.poll() == [] and runner.cleaned == [record["id"]]


def test_a_desk_job_whose_state_keeps_failing_times_out_without_a_cleanup(
        manager, runner, clock):
    record = manager.start_desk_job("daily-curve", "denis")
    runner.errors[record["id"]] = RuntimeError("the platform is down")
    clock.advance(minutes=16)
    assert manager.poll() == []  # a desk job has hours
    clock.advance(hours=6)
    [failed] = manager.poll()
    assert failed["error"] == "The run timed out after 360 minutes."
    clock.advance(minutes=20)
    assert manager.poll() == []
    assert runner.cleaned == [] and manager.active() == []


def test_a_failed_cleanup_is_tried_again_at_the_next_poll(
        manager, runner, store, clock, caplog):
    record, = start_runs(manager, 1)
    finish_run(record["id"], store, result={})
    runner.cleanup_error = RuntimeError("the platform is down")
    assert [r["id"] for r in manager.poll()] == [record["id"]]  # the page hears of it at once
    runner.states[record["id"]] = "done"
    clock.advance(minutes=10)
    with caplog.at_level(logging.WARNING):
        assert manager.poll() == []
    assert record["id"] in caplog.text and "RuntimeError" in caplog.text
    assert runner.cleaned == [record["id"]] and manager.active() == []
    runner.cleanup_error = None
    assert manager.poll() == [] and runner.cleaned == [record["id"]] * 2
    assert manager.poll() == [] and runner.cleaned == [record["id"]] * 2  # and then no more


# ---- the workflow of an ended run: kept a while, then removed

def test_an_ended_run_whose_job_still_runs_is_published_and_its_job_kept(
        manager, runner, store, clock):
    record, = start_runs(manager, 1)
    finish_run(record["id"], store, result={})  # the task writes first, then its run ends
    assert [r["status"] for r in manager.poll()] == ["done"]
    clock.advance(minutes=10)
    assert manager.poll() == [] and runner.cleaned == []  # the platform still runs it
    assert runner.asked[-1] == record["id"]


def test_the_job_of_an_ended_run_goes_once_its_run_is_over_and_the_keep_has_passed(
        manager, runner, store, clock):
    record, = start_runs(manager, 1)
    finish_run(record["id"], store, result={})
    manager.poll()
    clock.advance(minutes=12)
    assert manager.poll() == [] and runner.cleaned == []  # still running on the platform
    runner.states[record["id"]] = "done"
    assert manager.poll() == [] and runner.cleaned == [record["id"]]


def test_a_job_that_is_still_running_15_minutes_after_its_run_ended_is_removed(
        manager, runner, store, clock):
    record, = start_runs(manager, 1)
    finish_run(record["id"], store, result={})
    manager.poll()
    clock.advance(minutes=14, seconds=59)
    assert manager.poll() == [] and runner.cleaned == []
    clock.advance(seconds=1)  # 10 minutes to keep, and 5 more for the platform to end the run
    asked = len(runner.asked)
    assert manager.poll() == [] and runner.cleaned == [record["id"]]
    assert len(runner.asked) == asked  # the backstop does not wait for the platform


def test_the_keep_period_is_an_option(store, runner, clock):
    short = RunManager(store, runner, keep_finished=timedelta(minutes=1), now=clock)
    record = short.start("backtest", BACKTEST, "denis")
    finish_run(record["id"], store, result={})
    runner.states[record["id"]] = "done"
    short.poll()
    clock.advance(minutes=1)
    assert short.poll() == [] and runner.cleaned == [record["id"]]


def test_a_job_waiting_for_removal_survives_a_poll_error(manager, runner, store, clock, caplog):
    record, other = start_runs(manager, 2)
    finish_run(record["id"], store, result={})
    manager.poll()
    clock.advance(minutes=10)
    runner.errors[record["id"]] = RuntimeError("the platform is down")
    store.unreadable.add(other["id"])  # and the check of the other run fails too
    with caplog.at_level(logging.WARNING):
        assert manager.poll() == []
    assert caplog.text.count("RuntimeError") == 1 and runner.cleaned == []
    del runner.errors[record["id"]]
    runner.states[record["id"]] = "done"
    manager.poll()
    assert runner.cleaned == [record["id"]]


def test_an_ended_record_that_names_another_run_still_ends_the_run_it_belongs_to(
        manager, runner, store, clock):
    record, = start_runs(manager, 1)
    edit(store, record["id"], id="20260101T000000-000000", kind="job", status="done")
    assert [r["status"] for r in manager.poll()] == ["done"]
    assert manager.active() == [] and manager.poll() == []
    runner.states[record["id"]] = "done"
    clock.advance(minutes=10)
    manager.poll()
    assert runner.cleaned == [record["id"]]  # the job of the click, not of another run


def test_the_slot_of_a_run_is_free_once_its_record_has_ended(manager, runner, store):
    first, *_ = start_runs(manager, 3)
    finish_run(first["id"], store, result={})
    manager.poll()
    assert runner.cleaned == []  # its job is still there, and does not count
    assert manager.start("scenario", SCENARIO, "denis")["status"] == "running"
    with pytest.raises(TooManyRuns):
        manager.start("scenario", SCENARIO, "denis")


# ---- a record that cannot be read

def test_a_record_that_cannot_be_read_from_the_store_still_times_out(
        manager, runner, store, clock, caplog):
    record, = start_runs(manager, 1)
    store.unreadable.add(record["id"])
    clock.advance(minutes=14)
    with caplog.at_level(logging.WARNING):
        assert manager.poll() == [] and len(manager.active()) == 1  # logged, and kept for now
    assert record["id"] in caplog.text and "OSError" in caplog.text
    clock.advance(minutes=2)
    [failed] = manager.poll()
    assert (failed["id"], failed["status"], failed["error"]) == (record["id"], "failed", TIMED_OUT)
    assert manager.active() == []  # the slot is free again
    store.unreadable.clear()
    assert load_run(record["id"], store)["error"] == TIMED_OUT


def test_a_record_that_is_not_utf_8_is_no_news_until_the_clock_ends_it(runner, clock, tmp_path):
    store = LocalDirStore(tmp_path)
    manager = RunManager(store, runner, now=clock)
    record, = start_runs(manager, 1)
    (tmp_path / f"{env_prefix()}runs/{record['id']}.json").write_bytes(b"\xff\xfe not text")
    assert manager.poll() == [] and len(manager.active()) == 1
    clock.advance(minutes=16)
    [failed] = manager.poll()
    assert (failed["status"], failed["error"]) == ("failed", TIMED_OUT)
    assert load_run(record["id"], store)["error"] == TIMED_OUT  # a whole record again


def test_a_fresh_run_is_not_timed_out_with_the_default_clock(store, runner):
    default = RunManager(store, runner)
    default.start("backtest", BACKTEST, "denis")
    assert default.poll() == []


# ---- start_desk_job

def test_the_desk_jobs_are_the_two_deployed_workflows():
    assert DESK_JOBS == {
        "daily-curve": {"name": DAILY, "schedule": "Weekdays 07:00 London"},
        "strategy-sweep": {"name": SWEEP, "schedule": "Sundays 02:00 London"},
    }


def test_start_desk_job_starts_the_workflow_and_writes_a_record_of_kind_job(
        manager, runner, store):
    record = manager.start_desk_job("strategy-sweep", "denis")
    assert runner.desk_started == [SWEEP]
    assert (record["kind"], record["label"], record["job_name"]) == ("job", SWEEP, SWEEP)
    assert (record["inputs"], record["status"], record["started_by"]) == (
        {"name": SWEEP}, "running", "denis")
    assert load_run(record["id"], store) == record and manager.active() == [record]


def test_an_unknown_desk_job_is_a_value_error(manager, runner, store):
    with pytest.raises(ValueError, match="daily-curve"):
        manager.start_desk_job("nightly", "denis")
    assert runner.desk_started == [] and list_runs(store) == []


def test_desk_jobs_and_runs_do_not_count_against_each_other(manager):
    for _ in range(4):
        manager.start_desk_job("daily-curve", "denis")  # four, and there is no cap on them
    assert len(start_runs(manager, 3)) == 3  # and the three runs still fit
    with pytest.raises(TooManyRuns):
        manager.start("scenario", SCENARIO, "denis")
    assert manager.start_desk_job("daily-curve", "denis")["kind"] == "job"  # and still allowed


def test_a_refused_desk_job_is_failed_not_cleaned_up_and_the_refusal_is_raised(
        manager, runner, store):
    runner.refuse = f"The workflow '{DAILY}' is not deployed in this environment."
    with pytest.raises(RunRefused) as refusal:
        manager.start_desk_job("daily-curve", "denis")
    assert str(refusal.value) == runner.refuse
    [record] = list_runs(store)
    assert (record["kind"], record["status"], record["error"]) == ("job", "failed", runner.refuse)
    assert manager.active() == [] and runner.cleaned == []


# ---- resume

def test_resume_takes_up_the_unfinished_records_and_removes_stale_jobs_once(
        manager, runner, store):
    records = {status: new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, "denis")
               for status in ("queued", "running", "done", "failed")}
    for status, record in records.items():
        record["status"] = status
        save_run(record, store)
    desk = new_run("job", DAILY, {"name": DAILY}, "denis")
    desk["status"] = "running"
    save_run(desk, store)
    assert manager.resume() == 3
    assert {r["id"] for r in manager.active()} == {
        records["queued"]["id"], records["running"]["id"], desk["id"]}
    assert runner.stale_calls == [timedelta(hours=1)]
    assert manager.resume() == 0 and len(runner.stale_calls) == 2  # none taken up twice


def test_resumed_runs_are_polled_like_any_other(manager, runner, store, clock):
    record = new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, "denis")
    record["status"] = "running"
    save_run(record, store)
    manager.resume()
    runner.states[record["id"]] = "missing"  # the job went with the old app
    [failed] = manager.poll()
    assert (failed["id"], failed["error"]) == (record["id"], NOT_FOUND)
    clock.advance(minutes=10)
    assert manager.poll() == [] and runner.cleaned == [record["id"]]


def test_resume_counts_a_run_toward_the_cap(manager, store):
    for _ in range(3):
        record = new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, "denis")
        save_run(record, store)
    assert manager.resume() == 3
    with pytest.raises(TooManyRuns):
        manager.start("scenario", SCENARIO, "denis")


def test_resume_reads_the_newest_200_records(manager, store):
    for _ in range(201):
        save_run(new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, "denis"), store)
    assert manager.resume() == 200


def test_resume_skips_a_record_that_lacks_what_the_manager_needs(manager, store):
    whole = new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, "denis")
    save_run(whole, store)
    store.put_text(f"{env_prefix()}runs/20260101T000000-000000.json", json.dumps({"status": "x"}))
    assert manager.resume() == 1
    assert [r["id"] for r in manager.active()] == [whole["id"]]


@pytest.mark.parametrize("bad_id", [["a"], {"a": 1}, 7])
def test_resume_skips_a_record_whose_id_is_not_text(manager, store, bad_id):
    whole = new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, "denis")
    save_run(whole, store)
    odd = {**new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, "denis"), "id": bad_id}
    store.put_text(f"{env_prefix()}runs/20260101T000000-000000.json", json.dumps(odd))
    assert manager.resume() == 1  # and it did not raise
    assert [r["id"] for r in manager.active()] == [whole["id"]]


def test_resume_does_not_raise_when_the_runs_cannot_be_listed(manager, runner, store, caplog):
    store.listing_fails = True
    with caplog.at_level(logging.WARNING):
        assert manager.resume() == 0
    assert "OSError" in caplog.text
    assert runner.stale_calls == [timedelta(hours=1)]  # the other job is still done


def test_resume_does_not_raise_when_stale_jobs_cannot_be_removed(manager, runner, store, caplog):
    save_run(new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, "denis"), store)
    runner.stale_error = RuntimeError("the platform is down")
    with caplog.at_level(logging.WARNING):
        assert manager.resume() == 1  # the run is taken up all the same
    assert "RuntimeError" in caplog.text


def test_a_click_waits_for_resume_so_the_runs_of_the_last_app_count(store, runner, clock):
    for _ in range(3):  # the last app left three runs
        save_run(new_run("backtest", "Backtest DE M2 t1.5", BACKTEST, "denis"), store)
    manager = RunManager(store, runner, now=clock, resume_pending=True)
    outcome = []

    def click():
        try:
            outcome.append(manager.start("scenario", SCENARIO, "denis"))
        except TooManyRuns as refusal:
            outcome.append(refusal)

    clicked = threading.Thread(target=click)
    clicked.start()
    time.sleep(0.1)
    assert clicked.is_alive()  # the click waits for resume
    assert manager.resume() == 3
    clicked.join(timeout=5)
    assert [type(o) for o in outcome] == [TooManyRuns] and runner.submitted == []


def test_a_click_waits_for_resume_even_when_resume_fails(store, runner, clock):
    store.listing_fails = True
    manager = RunManager(store, runner, now=clock, resume_pending=True)
    manager.resume()
    started = time.monotonic()
    assert manager.start("scenario", SCENARIO, "denis")["status"] == "running"
    assert time.monotonic() - started < 1


def test_a_click_goes_ahead_when_resume_takes_too_long(monkeypatch, store, runner, clock):
    monkeypatch.setattr(manager_module, "RESUME_WAIT", 0.05)
    manager = RunManager(store, runner, now=clock, resume_pending=True)  # resume never comes
    assert manager.start("scenario", SCENARIO, "denis")["status"] == "running"


def test_without_a_pending_resume_a_click_does_not_wait(monkeypatch, store, runner, clock):
    monkeypatch.setattr(manager_module, "RESUME_WAIT", 30)
    started = time.monotonic()
    RunManager(store, runner, now=clock).start("scenario", SCENARIO, "denis")
    assert time.monotonic() - started < 1


# ---- threads and the contract

def test_a_run_that_is_being_started_is_left_alone_by_poll(manager, runner):
    polled, problems = [], []
    runner.on_submit = lambda record: polled.append(call_apart(manager.poll, problems))
    record = manager.start("backtest", BACKTEST, "denis")
    assert problems == []  # poll did not wait for a lock that start holds during submit
    assert polled == [[]] and runner.asked == []  # it was not asked about a job still starting
    assert record["status"] == "running" and manager.poll() == []


def test_the_cap_holds_while_a_run_is_being_started(store, runner, clock):
    one = RunManager(store, runner, max_active=1, now=clock)
    refused, problems = [], []

    def click_again(record):
        try:
            call_apart(lambda: one.start("backtest", BACKTEST, "denis"), problems)
        except TooManyRuns:
            refused.append(record["id"])

    runner.on_submit = click_again
    first = one.start("backtest", BACKTEST, "denis")
    assert problems == []  # start did not wait for a lock that another start holds during submit
    assert refused == [first["id"]] and len(runner.submitted) == 1


def test_many_threads_starting_runs_never_pass_the_cap_and_a_poll_does_not_disturb_them(
        manager, runner, store):
    runner.delay = 0.01
    gate = threading.Barrier(12, timeout=10)
    results, stop = [], threading.Event()

    def click():
        gate.wait()
        try:
            results.append(manager.start("backtest", BACKTEST, "denis"))
        except TooManyRuns:
            results.append(None)

    def keep_polling():
        while not stop.is_set():
            manager.poll()

    poller = threading.Thread(target=keep_polling)
    clickers = [threading.Thread(target=click) for _ in range(12)]
    for thread in (poller, *clickers):
        thread.start()
    for thread in clickers:
        thread.join(timeout=10)
    stop.set()
    poller.join(timeout=10)
    started = [r for r in results if r is not None]
    assert len(started) == 3 and len(results) == 12
    assert [load_run(r["id"], store)["status"] for r in started] == ["running"] * 3
    assert len(manager.active()) == 3


def test_active_gives_copies(manager):
    start_runs(manager, 1)
    manager.active()[0]["status"] = "failed"
    assert manager.active()[0]["status"] == "running"


@pytest.mark.parametrize("name", ["submit", "state", "cleanup", "remove_stale",
                                  "start_desk_job", "desk_job_state", "shutdown"])
def test_the_fake_runner_has_the_signature_of_the_protocol(name):
    expected = list(inspect.signature(getattr(Runner, name)).parameters)
    assert list(inspect.signature(getattr(FakeRunner, name)).parameters) == expected
