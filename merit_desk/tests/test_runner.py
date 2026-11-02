import inspect
import logging
import threading
from datetime import datetime, timedelta, timezone

import pytest
from datatailr import Workflow
from datatailr.scheduler import base
from datatailr.scheduler.utils import normalize_name
from datatailr.user import User

from merit_desk.common.runs import load_run, new_run, save_run
from merit_desk.common.store import MemoryStore
from merit_desk.desk_app import runner as runner_module
from merit_desk.desk_app.runner import (LocalRunner, PlatformRunner, Runner, RunRefused,
                                        list_run_jobs, load_workflow)
from merit_desk.workflows.desk_runs import RUN_PREFIX, job_name

BACKTEST = {"market": "DE", "tenor": 2, "threshold": 1.5}
DAILY = "Merit Desk Daily Curve"


@pytest.fixture(autouse=True)
def no_run_threads_left():
    yield
    assert [t.name for t in threading.enumerate() if t.name.startswith("desk-run")] == []


def now():
    return datetime.now(timezone.utc)  # read when needed: a suite takes seconds


def make_record(kind="backtest", inputs=None, age_s=0, label="Backtest DE M2 t1.5"):
    record = new_run(kind, label, BACKTEST if inputs is None else inputs, "denis")
    record["created"] = (now() - timedelta(seconds=age_s)).isoformat()
    return record


def desk_job_record(name=DAILY, age_s=0):
    return make_record("job", {"name": name}, age_s=age_s, label=name)


def run_entry(run_id, state, submitted=None, started=None):
    """One entry of Workflow.runs(), with the keys the SDK leaves in it."""
    return {"run_id": run_id, "state": state, "submit_time": submitted or now(),
            "start_time": started, "end_time": None, "job_version": 1}


class FakeWorkflow:
    """Stands in for datatailr.Workflow. It records the calls it gets, in order."""

    def __init__(self, name, runs=(), run=(True, "started"), start=(True, "started"),
                 stop=(True, ""), remove=(True, "")):
        self.name, self.calls, self.trigger_now, self.refreshed = name, [], None, []
        self.listed = list(runs)  # what runs() gives back. A test can change it
        self._results = {"run": run, "start": start, "stop": stop, "remove": remove}

    def _answer(self, call):
        self.calls.append(call)
        result = self._results[call]
        if isinstance(result, Exception):
            raise result
        return result

    def run(self):
        return self._answer("run")

    def start(self, trigger_now=False, version=None, quiet=False):
        self.trigger_now = trigger_now
        return self._answer("start")

    def stop(self):
        return self._answer("stop")

    def remove(self, all_versions=True):
        return self._answer("remove")

    def runs(self, start_time=None, end_time=None, refresh=False):
        self.calls.append("runs")
        self.refreshed.append(refresh)
        return self.listed


class FakePlatform:
    """The deployed jobs, seen through the three callables of PlatformRunner."""

    def __init__(self, *workflows, created=None):
        self.jobs = {w.name: w for w in workflows}
        self.created = created or {}
        self.looked_up = []

    def existing(self, name, environment):
        self.looked_up.append((name, environment))
        return self.jobs.get(name)

    def list_jobs(self):
        return [{"name": name, "created": self.created.get(name, now())} for name in self.jobs]

    def factory(self, record, environment):
        return self.jobs.setdefault(job_name(record), FakeWorkflow(job_name(record)))

    def runner(self, factory=None):
        return PlatformRunner("dev", workflow_factory=factory or self.factory,
                              existing=self.existing, list_jobs=self.list_jobs)


def factory_of(workflow):
    return lambda record, environment: workflow


# ---- PlatformRunner.submit

def test_submit_runs_the_workflow_and_returns_the_job_name():
    platform, record = FakePlatform(), make_record()
    seen = []

    def factory(rec, environment):
        seen.append((rec, environment))
        return platform.factory(rec, environment)

    assert platform.runner(factory).submit(record) == job_name(record)
    assert seen == [(record, "dev")]
    assert platform.jobs[job_name(record)].calls == ["run"]


def test_a_refused_run_raises_with_the_text_of_the_platform():
    refused = FakeWorkflow("x", run=(False, "Budget 'Trading desk' is used up."))
    with pytest.raises(RunRefused) as refusal:
        PlatformRunner("dev", workflow_factory=factory_of(refused)).submit(make_record())
    assert str(refusal.value) == "Budget 'Trading desk' is used up."


def test_a_refusal_without_text_still_says_something():
    refused = FakeWorkflow("x", run=(False, ""))
    with pytest.raises(RunRefused, match="platform"):
        PlatformRunner("dev", workflow_factory=factory_of(refused)).submit(make_record())


@pytest.mark.parametrize("where", ["building", "running"])
def test_an_error_while_building_or_running_becomes_a_refusal(where):
    error = RuntimeError("Command failed: dt cost get-budget\nNo budget 'Trading desk'.")

    def factory(record, environment):
        if where == "building":
            raise error
        return FakeWorkflow("x", run=error)

    with pytest.raises(RunRefused, match="No budget 'Trading desk'") as refusal:
        PlatformRunner("dev", workflow_factory=factory).submit(make_record())
    assert refusal.value.__cause__ is error


def test_an_error_without_text_is_named_by_its_type():
    def factory(record, environment):
        raise KeyError()

    with pytest.raises(RunRefused, match="KeyError"):
        PlatformRunner("dev", workflow_factory=factory).submit(make_record())


SDK_ERROR = ("Command failed: dt job run file:///tmp/tmpab12cd.json --environment dev\n"
             "Budget 'Trading desk' is used up.")


@pytest.mark.parametrize("how", ["building", "running", "refused run"])
def test_a_refusal_keeps_the_stderr_of_the_sdk_and_drops_the_command_line(how):
    def factory(record, environment):
        if how == "building":
            raise RuntimeError(SDK_ERROR)
        # Workflow.run() turns the error of the command into (False, text)
        return FakeWorkflow("x", run=RuntimeError(SDK_ERROR) if how == "running"
                            else (False, SDK_ERROR))

    with pytest.raises(RunRefused) as refusal:
        PlatformRunner("dev", workflow_factory=factory).submit(make_record())
    assert str(refusal.value) == "Budget 'Trading desk' is used up."


@pytest.mark.parametrize("start", [(False, SDK_ERROR), RuntimeError(SDK_ERROR)])
def test_a_refused_desk_job_start_keeps_the_stderr_only(start):
    platform = FakePlatform(FakeWorkflow(DAILY, start=start))
    with pytest.raises(RunRefused) as refusal:
        platform.runner().start_desk_job(DAILY)
    assert str(refusal.value) == "Budget 'Trading desk' is used up."


@pytest.mark.parametrize("text, expected", [
    ("Command failed: dt job run file:///tmp/x.json\nline one\nline two", "line one\nline two"),
    ("Command failed: dt job run file:///tmp/x.json", "The platform refused the request."),
    ("Command failed: dt job run file:///tmp/x.json\n", "The platform refused the request."),
    ("Command timed out after 60s: dt job run file:///tmp/x.json",
     "The platform did not answer in time."),
    ("Budget 'Trading desk' is used up.", "Budget 'Trading desk' is used up."),
    ("The command failed: no route", "The command failed: no route"),  # not the SDK's form
])
def test_only_the_message_of_the_platform_reaches_the_refusal(text, expected):
    def factory(record, environment):
        raise RuntimeError(text)

    with pytest.raises(RunRefused) as refusal:
        PlatformRunner("dev", workflow_factory=factory).submit(make_record())
    assert str(refusal.value) == expected


def refusal_text(text):
    """What the page gets when the SDK gives this text for a refused run."""
    refused = FakeWorkflow("x", run=(False, text))
    with pytest.raises(RunRefused) as refusal:
        PlatformRunner("dev", workflow_factory=factory_of(refused)).submit(make_record())
    return str(refusal.value)


def test_a_refusal_without_any_text_gets_the_default():
    assert refusal_text(None) == "The platform refused the request."


def test_file_paths_do_not_reach_the_page():
    text = ("Command failed: dt job run file:///tmp/tmpab12cd.json\n"
            "Cannot read file:///tmp/tmpab12cd.json now. Check the budget.")
    shown = refusal_text(text)
    assert "file://" not in shown and "tmpab12cd" not in shown
    assert shown.startswith("Cannot read") and shown.endswith("Check the budget.")


def test_a_long_refusal_is_cut_to_300_characters_and_ends_with_an_ellipsis():
    exact = "x" * 300
    assert refusal_text(exact) == exact  # 300 fit
    shown = refusal_text("Budget " + "x" * 500)
    assert len(shown) == 300 and shown.startswith("Budget xxx") and shown.endswith("...")
    cut = refusal_text("Command failed: dt job run file:///tmp/a.json\n" + "word " * 100)
    assert len(cut) <= 300 and cut.endswith("...")


def test_the_log_names_the_job_and_not_the_record(caplog):
    record = make_record(inputs={**BACKTEST, "note": "SECRET"})

    def factory(rec, environment):
        raise RuntimeError("platform down")

    with caplog.at_level(logging.INFO), pytest.raises(RunRefused):
        PlatformRunner("dev", workflow_factory=factory).submit(record)
    assert job_name(record) in caplog.text and "SECRET" not in caplog.text


@pytest.fixture
def silent_platform(monkeypatch):
    """Any question to the platform fails the test."""
    def contact_the_platform(*args, **kwargs):
        raise AssertionError("this must not contact the platform")

    class Exploding:
        def __getattr__(self, name):
            return contact_the_platform

    monkeypatch.setattr(base, "__client__", Exploding())
    monkeypatch.setattr(User, "signed_user", staticmethod(contact_the_platform))
    monkeypatch.setattr(base, "Budget", contact_the_platform)


def test_making_a_runner_does_not_contact_the_platform(silent_platform):
    LocalRunner(MemoryStore()).shutdown()
    PlatformRunner("dev")  # builds no workflow and asks for no job


def test_both_runners_shut_down_without_arguments_and_the_platform_one_does_nothing(
        silent_platform):
    assert list(inspect.signature(Runner.shutdown).parameters) == ["self"]
    LocalRunner(MemoryStore()).shutdown()
    PlatformRunner("dev").shutdown()  # nothing runs in this process, and it asks for nothing


def test_the_workflow_is_built_inside_submit_only():
    calls = []
    PlatformRunner("dev", workflow_factory=lambda record, environment: calls.append(1))
    assert calls == []


# ---- PlatformRunner.state

@pytest.mark.parametrize("platform_state, expected", [
    ("completed", "done"),
    ("failed", "failed"), ("cancelled", "failed"), ("stopped", "failed"),
    ("failed_after", "failed"), ("out_of_memory", "failed"), ("expired", "failed"),
    ("did_not_run", "failed"),
    ("running", "running"), ("pending", "running"), ("starting", "running"),
    ("building", "running"), ("unknown", "running"), (None, "running"),
])
def test_state_maps_the_state_of_the_newest_run(platform_state, expected):
    record = make_record()
    workflow = FakeWorkflow(job_name(record), runs=[run_entry(4, platform_state)])
    assert FakePlatform(workflow).runner().state(record) == expected


def test_state_reads_the_runs_afresh_and_takes_the_highest_run_id():
    record = make_record()
    runs = [run_entry(7, "failed"), run_entry(9, "completed"), run_entry(8, "running")]
    workflow = FakeWorkflow(job_name(record), runs=runs)
    platform = FakePlatform(workflow)
    assert platform.runner().state(record) == "done"
    assert workflow.refreshed == [True]
    assert platform.looked_up == [(job_name(record), "dev")]


def test_state_of_an_unknown_job_is_missing():
    assert FakePlatform().runner().state(make_record()) == "missing"


def test_a_job_without_a_run_is_running_for_a_minute_and_then_missing():
    young, old = make_record(age_s=20), make_record(age_s=120)
    platform = FakePlatform(FakeWorkflow(job_name(young)), FakeWorkflow(job_name(old)))
    assert platform.runner().state(young) == "running"
    assert platform.runner().state(old) == "missing"


def test_a_desk_job_record_follows_the_run_that_its_click_started():
    """The workflow has older runs. A run from before the click is not this click's."""
    record = desk_job_record()
    runs = [run_entry(3, "completed", submitted=now() - timedelta(hours=5))]
    workflow = FakeWorkflow(DAILY, runs=runs)
    platform = FakePlatform(workflow)
    assert platform.runner().state(record) == "running"  # the new run is not listed yet
    workflow.listed.append(run_entry(4, "running", submitted=now() + timedelta(seconds=2)))
    assert platform.runner().state(record) == "running"
    workflow.listed[-1]["state"] = "completed"
    assert platform.runner().state(record) == "done"
    assert {name for name, _ in platform.looked_up} == {DAILY}  # not a "Merit Desk Run ..." name


def test_a_desk_job_record_without_a_new_run_is_missing_after_a_minute():
    runs = [run_entry(3, "completed", submitted=now() - timedelta(hours=5))]
    platform = FakePlatform(FakeWorkflow(DAILY, runs=runs))
    assert platform.runner().state(desk_job_record(age_s=120)) == "missing"


def test_a_desk_job_record_uses_its_job_name_when_it_has_one():
    record = desk_job_record()
    record["job_name"] = "Merit Desk Strategy Sweep"
    workflow = FakeWorkflow("Merit Desk Strategy Sweep", runs=[run_entry(1, "completed")])
    assert FakePlatform(workflow).runner().state(record) == "done"


# ---- PlatformRunner.cleanup

def test_cleanup_stops_a_running_job_and_then_removes_it():
    record = make_record()
    workflow = FakeWorkflow(job_name(record), runs=[run_entry(1, "running")])
    FakePlatform(workflow).runner().cleanup(record)
    assert workflow.calls == ["runs", "stop", "remove"]


@pytest.mark.parametrize("runs", [[run_entry(1, "completed")], [run_entry(1, "failed")], []])
def test_cleanup_only_removes_a_job_that_is_finished_or_never_ran(runs):
    record = make_record()
    workflow = FakeWorkflow(job_name(record), runs=runs)
    FakePlatform(workflow).runner().cleanup(record)
    assert workflow.calls == ["runs", "remove"]


def test_cleanup_of_a_job_that_is_gone_is_fine():
    FakePlatform().runner().cleanup(make_record())  # no error


def test_cleanup_never_touches_the_record_of_a_desk_job():
    workflow = FakeWorkflow(DAILY, runs=[run_entry(1, "running")])
    platform = FakePlatform(workflow)
    platform.runner().cleanup(desk_job_record())
    assert workflow.calls == [] and platform.looked_up == []


def test_cleanup_logs_a_failure_and_does_not_raise(caplog):
    record = make_record()
    workflow = FakeWorkflow(job_name(record), runs=[run_entry(1, "completed")],
                            remove=(False, "No permission."))
    with caplog.at_level(logging.WARNING):
        FakePlatform(workflow).runner().cleanup(record)
    assert job_name(record) in caplog.text and "No permission." in caplog.text


@pytest.mark.parametrize("stop", [(False, "Busy."), RuntimeError("the platform is busy")])
def test_cleanup_removes_the_job_even_when_stopping_it_fails(stop):
    record = make_record()
    workflow = FakeWorkflow(job_name(record), runs=[run_entry(1, "running")], stop=stop)
    FakePlatform(workflow).runner().cleanup(record)
    assert workflow.calls == ["runs", "stop", "remove"]


def test_cleanup_logs_an_error_and_does_not_raise(caplog):
    record = make_record()

    def existing(name, environment):
        raise RuntimeError("the platform is down")

    with caplog.at_level(logging.WARNING):
        PlatformRunner("dev", existing=existing).cleanup(record)
    assert job_name(record) in caplog.text and "the platform is down" in caplog.text


# ---- PlatformRunner.remove_stale

def test_remove_stale_removes_only_old_run_workflows():
    old = "Merit Desk Run Scenario DE -3.0C 0f2a9c"
    fresh = "Merit Desk Run Backtest DE M2 t1.5 0001ab"
    others = (DAILY, "Merit Desk Runner Notes", "Other Job")  # none starts with the run prefix
    workflows = {name: FakeWorkflow(name) for name in (old, fresh, *others)}
    created = {name: now() - timedelta(days=30) for name in others}
    created.update({old: now() - timedelta(hours=2), fresh: now() - timedelta(minutes=5)})
    platform = FakePlatform(*workflows.values(), created=created)
    assert platform.runner().remove_stale(timedelta(hours=1)) == 1
    assert [name for name, w in workflows.items() if w.calls] == [old]
    assert workflows[old].calls == ["remove"]


def test_remove_stale_goes_on_after_a_failure_and_counts_only_removals(caplog):
    names = [f"Merit Desk Run Scenario DE +0.0C {i:06d}" for i in range(4)]
    results = [RuntimeError("platform error"), (False, "No permission."), (True, ""), (True, "")]
    workflows = [FakeWorkflow(name, remove=result) for name, result in zip(names, results)]
    three_hours_ago = now() - timedelta(hours=3)
    platform = FakePlatform(*workflows, created={name: three_hours_ago for name in names})
    with caplog.at_level(logging.WARNING):
        assert platform.runner().remove_stale(timedelta(hours=1)) == 2
    assert all(w.calls == ["remove"] for w in workflows)  # every one was tried
    assert names[0] in caplog.text and names[1] in caplog.text and "No permission." in caplog.text


def test_remove_stale_skips_a_job_that_vanished_in_between():
    name = "Merit Desk Run Scenario DE +0.0C 000001"
    platform = FakePlatform(FakeWorkflow(name), created={name: now() - timedelta(hours=3)})
    gone = PlatformRunner("dev", existing=lambda n, e: None, list_jobs=platform.list_jobs)
    assert gone.remove_stale(timedelta(hours=1)) == 0


@pytest.mark.parametrize("name", [DAILY, "Merit Desk Runner Notes", "Other Job", ""])
def test_the_one_function_that_removes_jobs_refuses_a_name_that_is_not_a_run_job(name, caplog):
    workflow = FakeWorkflow(name, runs=[run_entry(1, "running")])
    platform = FakePlatform(workflow)
    with caplog.at_level(logging.WARNING):
        assert platform.runner()._remove(name, stop_first=True) is False
    assert workflow.calls == [] and platform.looked_up == []  # it did not even look the job up
    assert "not a run job" in caplog.text


# ---- PlatformRunner.start_desk_job and desk_job_state

def test_start_desk_job_starts_the_deployed_workflow_now():
    workflow = FakeWorkflow(DAILY)
    assert FakePlatform(workflow).runner().start_desk_job(DAILY) == DAILY
    assert workflow.calls == ["start"] and workflow.trigger_now is True


def test_start_desk_job_names_a_workflow_that_is_not_deployed():
    with pytest.raises(RunRefused) as refusal:
        FakePlatform().runner().start_desk_job(DAILY)
    assert str(refusal.value) == f"The workflow '{DAILY}' is not deployed in this environment."


def test_start_desk_job_raises_the_message_of_a_refused_start():
    platform = FakePlatform(FakeWorkflow(DAILY, start=(False, "You may not start this job.")))
    with pytest.raises(RunRefused) as refusal:
        platform.runner().start_desk_job(DAILY)
    assert str(refusal.value) == "You may not start this job."


def test_start_desk_job_turns_an_error_into_a_refusal():
    platform = FakePlatform(FakeWorkflow(DAILY, start=RuntimeError("no route")))
    with pytest.raises(RunRefused, match="no route"):
        platform.runner().start_desk_job(DAILY)


def test_desk_job_state_gives_the_state_and_start_time_of_the_newest_run():
    started = datetime(2026, 10, 30, 6, 0, 3, tzinfo=timezone.utc)
    runs = [run_entry(11, "completed", started=started), run_entry(10, "failed")]
    platform = FakePlatform(FakeWorkflow(DAILY, runs=runs))
    assert platform.runner().desk_job_state(DAILY) == {
        "last_state": "done", "last_time": "2026-10-30T06:00:03+00:00"}


def test_desk_job_state_uses_the_submit_time_of_a_run_that_has_not_started():
    submitted = datetime(2026, 10, 30, 6, 0, 0, tzinfo=timezone.utc)
    runs = [run_entry(11, "pending", submitted=submitted, started=0)]  # the SDK leaves 0 as it is
    platform = FakePlatform(FakeWorkflow(DAILY, runs=runs))
    assert platform.runner().desk_job_state(DAILY) == {
        "last_state": "running", "last_time": "2026-10-30T06:00:00+00:00"}


@pytest.mark.parametrize("platform", [FakePlatform(), FakePlatform(FakeWorkflow(DAILY))])
def test_desk_job_state_is_empty_without_a_job_or_a_run(platform):
    assert platform.runner().desk_job_state(DAILY) == {"last_state": None, "last_time": None}


# ---- the default SDK calls (the SDK itself gets a fake client)

class FakeClient:
    def __init__(self, answer):
        self._answer, self.calls = answer, []

    def get(self, name, **kwargs):
        self.calls.append((name, kwargs))
        if isinstance(self._answer, Exception):
            raise self._answer
        return self._answer


@pytest.mark.parametrize("answer", [
    [],  # the SDK turns an empty list into a TaskError
    RuntimeError("Command failed: dt job get x\nJob with Id 1234 does not exist"),
])
def test_the_default_lookup_gives_none_for_an_unknown_job(monkeypatch, answer):
    client = FakeClient(answer)
    monkeypatch.setattr(base, "__client__", client)
    assert load_workflow("Merit Desk Run Backtest DE M2 t1.5 0001ab", "DEV") is None
    name, kwargs = client.calls[0]
    assert name == "merit-desk-run-backtest-de-m2-t1-5-0001ab"
    assert str(kwargs["environment"]) == "dev"


def test_the_default_lookup_does_not_hide_other_errors(monkeypatch):
    monkeypatch.setattr(base, "__client__", FakeClient(RuntimeError("Command failed: permission")))
    with pytest.raises(RuntimeError, match="permission"):
        load_workflow(DAILY, "dev")


def test_the_default_list_gives_display_names_and_creation_times(monkeypatch):
    asked = {}

    def ls(environment=None, filter_by=None):
        asked.update(environment=environment, filter_by=filter_by)
        return [{"name": "merit-desk-run-a-000001", "display_name": "Merit Desk Run A 000001",
                 "create_time": 1_761_800_000_000_000}]  # microseconds

    monkeypatch.setattr(Workflow, "ls", staticmethod(ls))
    assert list_run_jobs("dev") == [{
        "name": "Merit Desk Run A 000001",
        "created": datetime.fromtimestamp(1_761_800_000, timezone.utc)}]
    assert asked == {"environment": "dev", "filter_by": "name starts_with merit-desk-run-"}


def test_the_default_list_copes_with_no_output(monkeypatch):
    monkeypatch.setattr(Workflow, "ls", staticmethod(lambda **kwargs: None))  # SDK: None for none
    assert list_run_jobs("dev") == []


def test_the_run_prefix_names_the_jobs_and_is_the_server_filter_in_normalized_form(monkeypatch):
    asked = {}
    monkeypatch.setattr(Workflow, "ls", staticmethod(
        lambda environment=None, filter_by=None: asked.update(filter_by=filter_by) or []))
    list_run_jobs("dev")
    prefix = normalize_name(RUN_PREFIX) + "-"  # the SDK strips the dash that the space gives
    assert job_name(make_record()).startswith(RUN_PREFIX)
    assert asked["filter_by"] == f"name starts_with {prefix}"
    assert normalize_name(job_name(make_record())).startswith(prefix)  # the server finds the jobs
    assert not normalize_name(DAILY).startswith(prefix)  # and not the desk workflows


def test_the_platform_runner_lists_jobs_of_its_own_environment(monkeypatch):
    seen = []

    def list_jobs(environment):
        seen.append(environment)
        return []

    monkeypatch.setattr(runner_module, "list_run_jobs", list_jobs)
    assert PlatformRunner("pre").remove_stale(timedelta(hours=1)) == 0
    assert seen == ["pre"]


# ---- LocalRunner

def test_local_submit_finishes_a_backtest_in_the_store():
    store, record = MemoryStore(), make_record()
    save_run(record, store)
    local = LocalRunner(store)
    assert local.submit(record) == job_name(record)
    local.shutdown(wait=True)  # waits for the run, and leaves no thread behind
    done = load_run(record["id"], store)
    assert done["status"] == "done" and done["result"]["total_pnl_eur"] is not None


def test_local_state_follows_the_record():
    store, record = MemoryStore(), make_record()
    local = LocalRunner(store)
    assert local.state(record) == "missing"  # nothing stored yet
    for status, expected in [("queued", "running"), ("running", "running"),
                             ("done", "done"), ("failed", "failed")]:
        record["status"] = status
        save_run(record, store)
        assert local.state(record) == expected
    local.shutdown()


def test_a_run_that_fails_in_its_thread_is_logged_and_written_to_its_record(caplog):
    store, record = MemoryStore(), make_record(inputs={"market": "DE"})  # tenor is missing
    save_run(record, store)
    local = LocalRunner(store)
    with caplog.at_level(logging.WARNING):
        local.submit(record)
        local.shutdown(wait=True)
    failed = load_run(record["id"], store)
    assert failed["status"] == "failed" and failed["error"].startswith("KeyError")
    assert record["id"] in caplog.text and "KeyError" in caplog.text


def test_local_runs_never_use_more_than_max_workers_threads(monkeypatch):
    both_running = threading.Barrier(2, timeout=10)  # two runs must be in progress together
    lock, active, peak = threading.Lock(), [0], [0]

    def execute(run_id, store):
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        try:
            both_running.wait()
        finally:
            with lock:
                active[0] -= 1

    monkeypatch.setattr(runner_module, "execute_run", execute)
    local = LocalRunner(MemoryStore(), max_workers=2)
    for _ in range(4):
        local.submit(make_record())
    local.shutdown(wait=True)
    assert peak[0] == 2


def test_local_shutdown_does_not_wait_by_default():
    LocalRunner(MemoryStore()).shutdown()  # the app's lifespan calls it like this


def test_local_runner_has_no_platform_to_clean_or_start_jobs_on():
    local = LocalRunner(MemoryStore())
    local.cleanup(make_record())  # does nothing
    assert local.remove_stale(timedelta(hours=1)) == 0
    assert local.desk_job_state(DAILY) == {"last_state": None, "last_time": None}
    with pytest.raises(RunRefused) as refusal:
        local.start_desk_job(DAILY)
    assert str(refusal.value) == "Starting desk jobs needs the Datatailr platform."
    local.shutdown()


# ---- the contract

@pytest.mark.parametrize("name", ["submit", "state", "cleanup", "remove_stale",
                                  "start_desk_job", "desk_job_state"])
def test_both_runners_have_the_signature_of_the_protocol(name):
    expected = list(inspect.signature(getattr(Runner, name)).parameters)
    for cls in (LocalRunner, PlatformRunner):
        assert list(inspect.signature(getattr(cls, name)).parameters) == expected


@pytest.mark.parametrize("name", ["run", "start", "stop", "remove", "runs"])
def test_the_fake_workflow_takes_the_arguments_of_the_sdk(name):
    sdk = list(inspect.signature(getattr(Workflow, name)).parameters)
    assert list(inspect.signature(getattr(FakeWorkflow, name)).parameters) == sdk
