"""The two ways to run a click: a thread on this machine, or a workflow on Datatailr."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Callable, Protocol

from datatailr import Environment, Workflow
from datatailr.errors import TaskError

from merit_desk.common.messages import clean_platform_text
from merit_desk.common.runs import TERMINAL, load_run
from merit_desk.common.store import TextStore
from merit_desk.workflows.desk_runs import RUN_PREFIX, build_run_workflow, execute_run, job_name

logger = logging.getLogger(__name__)

NEW_JOB_WAIT = timedelta(seconds=60)  # a job that was just submitted has no run for a moment
CLOCK_SKEW = timedelta(seconds=5)  # the app and the platform do not share one clock
UNSUCCESSFUL = ("failed", "failed_after", "out_of_memory", "stopped", "cancelled", "expired",
                "did_not_run")  # run states that mean "ended without success"


class RunRefused(Exception):
    """The platform would not start a job. The text is its own message, for the page."""

    record: dict | None = None  # the failed run record, when the run manager made one


class Runner(Protocol):
    def submit(self, record: dict) -> str:
        """Start the run of this record. Gives back the job name."""

    def state(self, record: dict) -> str:
        """One of 'running', 'done', 'failed' or 'missing'."""

    def cleanup(self, record: dict) -> None:
        """Remove what the run left behind."""

    def remove_stale(self, max_age: timedelta) -> int:
        """Remove leftover run jobs older than max_age. Gives back how many."""

    def start_desk_job(self, workflow_name: str) -> str:
        """Run a deployed desk workflow now. Gives back its name."""

    def desk_job_state(self, workflow_name: str) -> dict:
        """The state and the time of its last run: {'last_state': ..., 'last_time': ...}."""

    def shutdown(self) -> None:
        """Stop what the runner started in this process."""


def _text(error: Exception) -> str:
    return str(error) or type(error).__name__


def _platform_message(text: str | None) -> str:
    """The platform's own words, for the page: no command line, no paths, 300 characters."""
    return clean_platform_text(text, "The platform refused the request.")


def _refuse_unless(ok: bool, message: str) -> None:
    if not ok:
        raise RunRefused(_platform_message(message))


class LocalRunner:
    """Runs each click in a thread of this process, for a laptop and for tests."""

    def __init__(self, store: TextStore, max_workers: int = 3) -> None:
        self._store = store
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="desk-run")

    def submit(self, record: dict) -> str:
        self._pool.submit(self._run, record["id"])
        return job_name(record)

    def _run(self, run_id: str) -> None:
        try:
            execute_run(run_id, self._store)
        except Exception as error:  # execute_run has already put it in the record
            logger.warning("Run %s failed: %r", run_id, error)

    def state(self, record: dict) -> str:
        stored = load_run(record["id"], self._store)
        if stored is None:
            return "missing"
        return stored["status"] if stored.get("status") in TERMINAL else "running"

    def cleanup(self, record: dict) -> None:
        """A thread leaves nothing behind."""

    def remove_stale(self, max_age: timedelta) -> int:
        return 0

    def start_desk_job(self, workflow_name: str) -> str:
        raise RunRefused("Starting desk jobs needs the Datatailr platform.")

    def desk_job_state(self, workflow_name: str) -> dict:
        return {"last_state": None, "last_time": None}

    def shutdown(self, wait: bool = False) -> None:
        self._pool.shutdown(wait=wait)


def load_workflow(name: str, environment: str) -> Workflow | None:
    """The deployed workflow, or None. The SDK says "does not exist" when there is none."""
    try:
        return Workflow(name=name, environment=Environment(environment.lower()), get_existing=True)
    except (TaskError, RuntimeError) as error:  # TaskError from the SDK, RuntimeError from dt
        if "does not exist" in str(error):
            return None
        raise


def list_run_jobs(environment: str) -> list[dict]:
    """The per-click workflows on the platform, as {'name', 'created'} with a UTC datetime."""
    # the filter uses the normalized name, which has no spaces to quote
    jobs = Workflow.ls(environment=environment,
                       filter_by="name starts_with merit-desk-run-") or []
    return [{"name": job["display_name"],
             "created": datetime.fromtimestamp(job["create_time"] / 1_000_000, timezone.utc)}
            for job in jobs]  # create_time is in microseconds


def _status(platform_state: str | None) -> str:
    """The word the app uses for the state of a run on the platform."""
    state = (platform_state or "").lower()
    if state == "completed":
        return "done"
    return "failed" if state in UNSUCCESSFUL else "running"


def _newest_run(workflow: Workflow, since: datetime | None = None) -> dict | None:
    """The run with the highest run id. With `since`, runs submitted before it do not count."""
    runs = workflow.runs(refresh=True)
    if since is not None:  # a time that is not set yet is 0 or None, and the run stays in
        runs = [run for run in runs if not isinstance(run.get("submit_time"), datetime)
                or run["submit_time"] >= since]
    return max(runs, key=lambda run: run.get("run_id", 0), default=None)


def _iso(*times) -> str | None:
    """The first of the times that is a datetime, as text."""
    return next((t.isoformat(timespec="seconds") for t in times if isinstance(t, datetime)), None)


def _attempt(name: str, what: str, action: Callable[[], tuple[bool, str]]) -> bool:
    """Call stop or remove. A failure is logged with the job name. Says if it worked."""
    try:
        ok, message = action()
    except Exception as error:
        ok, message = False, _text(error)
    if not ok:
        logger.warning("Could not %s job %s: %s", what, name, message)
    return ok


class PlatformRunner:
    """Runs each click as a workflow of its own on Datatailr.

    Building a Workflow asks the platform for the signed user and the budget, so the
    workflow is built in submit() only. Making a runner touches nothing.
    """

    def __init__(self, environment: str,
                 workflow_factory: Callable[[dict, str], Workflow] = build_run_workflow,
                 existing: Callable[[str, str], Workflow | None] | None = None,
                 list_jobs: Callable[[], list[dict]] | None = None) -> None:
        self._environment = environment
        self._factory = workflow_factory
        self._existing = existing or load_workflow
        self._list_jobs = list_jobs or partial(list_run_jobs, environment)

    def submit(self, record: dict) -> str:
        name = job_name(record)
        try:
            ok, message = self._factory(record, self._environment).run()
        except Exception as error:  # the user, the budget, the save: any of them can fail
            logger.warning("Job %s was not started: %s", name, _text(error))
            raise RunRefused(_platform_message(_text(error))) from error
        if not ok:
            logger.warning("The platform refused job %s: %s", name, message)
        _refuse_unless(ok, message)
        logger.info("Started job %s.", name)
        return name

    def state(self, record: dict) -> str:
        desk_job = record["kind"] == "job"  # a desk workflow keeps its name and its old runs
        name = (record.get("job_name") or record["label"]) if desk_job else job_name(record)
        workflow = self._existing(name, self._environment)
        if workflow is None:
            return "missing"
        created = datetime.fromisoformat(record["created"])
        run = _newest_run(workflow, since=created - CLOCK_SKEW if desk_job else None)
        if run is not None:
            return _status(run.get("state"))
        return "running" if datetime.now(timezone.utc) - created < NEW_JOB_WAIT else "missing"

    def _remove(self, name: str, stop_first: bool) -> bool:
        """Remove one job, after stopping it if it still runs. Logs a failure, never raises."""
        if not name.startswith(RUN_PREFIX):  # the one place that removes jobs: only run jobs
            logger.warning("Will not remove job %s: it is not a run job.", name)
            return False
        try:
            workflow = self._existing(name, self._environment)
            if workflow is None:  # gone already
                return False
            run = _newest_run(workflow) if stop_first else None
        except Exception as error:
            logger.warning("Could not look up job %s: %s", name, _text(error))
            return False
        if run is not None and _status(run.get("state")) == "running":
            _attempt(name, "stop", workflow.stop)
        return _attempt(name, "remove", workflow.remove)

    def cleanup(self, record: dict) -> None:
        if record["kind"] != "job":  # the desk workflows must never be removed
            self._remove(job_name(record), stop_first=True)

    def remove_stale(self, max_age: timedelta) -> int:
        """Failures are logged per job. A failed listing is raised."""
        cutoff = datetime.now(timezone.utc) - max_age
        stale = [job["name"] for job in self._list_jobs()
                 if job["name"].startswith(RUN_PREFIX) and job["created"] < cutoff]
        removed = sum(self._remove(name, stop_first=False) for name in stale)
        if removed:
            logger.info("Removed %d leftover run jobs.", removed)
        return removed

    def start_desk_job(self, workflow_name: str) -> str:
        try:
            workflow = self._existing(workflow_name, self._environment)
            if workflow is None:
                ok = False
                message = f"The workflow '{workflow_name}' is not deployed in this environment."
            else:
                ok, message = workflow.start(trigger_now=True)
        except Exception as error:
            raise RunRefused(_platform_message(_text(error))) from error
        _refuse_unless(ok, message)
        return workflow_name

    def desk_job_state(self, workflow_name: str) -> dict:
        workflow = self._existing(workflow_name, self._environment)
        run = _newest_run(workflow) if workflow is not None else None
        if run is None:
            return {"last_state": None, "last_time": None}
        return {"last_state": _status(run.get("state")),
                "last_time": _iso(run.get("start_time"), run.get("submit_time"))}

    def shutdown(self) -> None:
        """The jobs run on the platform, not in this process."""
