"""The run manager: caps the runs in progress, watches them and ends the ones that are stuck."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Callable

from merit_desk.common.runs import (STATUSES, TERMINAL, list_runs, load_run, new_run, run_label,
                                    save_run)
from merit_desk.common.store import TextStore
from merit_desk.desk_app.runner import Runner, RunRefused

logger = logging.getLogger(__name__)

DESK_JOBS = {
    "daily-curve": {"name": "Merit Desk Daily Curve", "schedule": "Weekdays 07:00 London"},
    "strategy-sweep": {"name": "Merit Desk Strategy Sweep", "schedule": "Sundays 02:00 London"},
}
CLICK_KINDS = ("scenario", "backtest")  # what a click starts. Only these count against the cap
STALE_AFTER = timedelta(hours=1)  # a run job older than this was left behind by an old app
STOP_AFTER = timedelta(minutes=5)  # after the keep period: a run job still running is stopped
RESUME_WAIT = 10.0  # seconds a click waits for resume(), so the runs it takes up count
TRACKED_KEYS = {"id", "kind", "label", "created"}  # what the manager needs of a resumed record
STATE_ERRORS = {"failed": "The run ended without a result.",
                "missing": "The run's job was not found."}


class TooManyRuns(Exception):
    """The cap is reached. The app answers 429 with this text."""

    def __init__(self, text: str = "Three runs are in progress. Wait for one to finish.") -> None:
        super().__init__(text)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _last_change(record: dict) -> datetime | None:
    """When the record last changed, or None when it has no time that can be read."""
    try:
        stamp = datetime.fromisoformat(record.get("updated") or record["created"])
    except (KeyError, TypeError, ValueError):
        return None
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)


class RunManager:
    """Starts runs, watches them until they end, and cleans up after them.

    The app calls start() from request threads and poll() from a worker thread. The lock
    guards the collections only. It is never held while the store or the runner is called,
    because those calls can be slow.

    A task writes its record before its run ends on the platform. So an ended run frees its
    slot at once, but its workflow is removed only once the platform has ended the run too and
    `keep_finished` has passed. A run still running STOP_AFTER later is stopped and removed.
    """

    def __init__(self, store: TextStore, runner: Runner, max_active: int = 3, *,
                 timeout: timedelta = timedelta(minutes=15),
                 job_timeout: timedelta = timedelta(hours=6),
                 keep_finished: timedelta = timedelta(minutes=10),
                 now: Callable[[], datetime] = _utc_now, resume_pending: bool = False) -> None:
        self._store, self._runner, self._max_active = store, runner, max_active
        self._timeout, self._job_timeout, self._now = timeout, job_timeout, now
        self._keep_finished = keep_finished
        self._lock = threading.Lock()
        self._active: dict[str, dict] = {}  # run id -> its record, as last seen
        self._starting: set[str] = set()  # runs that start() is still setting up: poll skips them
        self._ended: dict[str, tuple[dict, datetime]] = {}  # run id -> record, when it ended
        self._resumed = threading.Event()  # start() waits for it. The app sets resume_pending
        if not resume_pending:
            self._resumed.set()

    # ---- starting a run

    def start(self, kind: str, inputs: dict, started_by: str | None) -> dict:
        """Start a scenario or a backtest. A run that the platform refuses comes back failed."""
        if kind not in CLICK_KINDS:
            raise ValueError(f"A click starts a scenario or a backtest, not a '{kind}' run.")
        self._resumed.wait(RESUME_WAIT)  # the unfinished runs of the last app count, too
        record = new_run(kind, run_label(kind, inputs), inputs, started_by)
        self._open(record, capped=True)
        try:
            try:
                job = self._runner.submit(record)
            except RunRefused as refusal:
                self._failed_to_start(record, str(refusal))
                self._clean_up(record)  # a workflow may have been saved before the refusal
                return record
            except Exception as error:  # a local pool that was shut down, say: free the slot
                self._failed_to_start(record, f"{type(error).__name__}: {error}")
                raise
            return self._mark_running(record, job)
        finally:
            self._done_starting(record)

    def start_desk_job(self, job: str, started_by: str | None) -> dict:
        """Run a deployed desk workflow now. A refusal is raised after the record is failed."""
        if job not in DESK_JOBS:
            raise ValueError(f"Unknown desk job '{job}'. Use one of: {', '.join(DESK_JOBS)}.")
        name = DESK_JOBS[job]["name"]
        record = new_run("job", run_label("job", {"name": name}), {"name": name}, started_by)
        record["job_name"] = name  # the workflow keeps its name: there is no job to look for
        self._open(record, capped=False)
        try:
            try:
                self._runner.start_desk_job(name)
            except RunRefused as refusal:
                self._failed_to_start(record, str(refusal))
                refusal.record = dict(record)  # so the app can show the failed run on every page
                raise
            return self._mark_running(record, name)
        finally:
            self._done_starting(record)

    def _open(self, record: dict, capped: bool) -> None:
        """Take a slot (starting runs count) and store the queued record."""
        with self._lock:
            if capped and self._busy() >= self._max_active:
                raise TooManyRuns()
            self._active[record["id"]] = record
            self._starting.add(record["id"])
        try:
            save_run(record, self._store)
        except Exception:  # nothing was stored and nothing started
            self._forget(record["id"])
            raise

    def _busy(self) -> int:
        return sum(record["kind"] in CLICK_KINDS for record in self._active.values())

    def _failed_to_start(self, record: dict, error: str) -> None:
        """The run did not start: the record is failed with `error` and not active."""
        record.update(status="failed", error=error)
        save_run(record, self._store)
        self._forget(record["id"])

    def _mark_running(self, record: dict, job: str) -> dict:
        """Store the job name and the status running, unless the task has finished the run."""
        # no compare-and-set in the store: a task that ends after this read can be overwritten
        stored = load_run(record["id"], self._store)
        if stored is not None and stored.get("status") in TERMINAL:
            record = {**record, **stored}  # the task was quicker than we are
        else:
            record = {**record, "status": "running"}
        record["job_name"] = job
        save_run(record, self._store)
        with self._lock:
            self._active[record["id"]] = record
        return dict(record)

    def _done_starting(self, record: dict) -> None:
        """The run is started, or failed to start: from now on poll may look at it."""
        with self._lock:
            self._starting.discard(record["id"])

    def _forget(self, run_id: str) -> None:
        with self._lock:
            self._active.pop(run_id, None)
            self._starting.discard(run_id)

    def _clean_up(self, record: dict) -> None:
        """Ask the runner to remove what the run left. An error is logged, never raised."""
        try:
            self._runner.cleanup(record)
        except Exception as error:
            logger.warning("Could not clean up run %s: %s", record["id"], type(error).__name__)

    # ---- watching the runs

    def poll(self) -> list[dict]:
        """Check each active run once, and remove the workflows of ended runs that are due.
        Gives back the records that changed, each ended one once.

        No task writes the record of a desk job: the manager finishes it done when the runner says
        the run is done. A desk job has its own, longer timeout, as a backstop.
        Poll assumes a single poller, the one worker loop of the app."""
        with self._lock:
            watched = [record for run_id, record in self._active.items()
                       if run_id not in self._starting]
        changed = []
        for record in watched:
            try:
                latest = self._check(record)
            except Exception as error:  # one record must not stop the others: retry it next time
                logger.warning("Could not check run %s: %s", record["id"], type(error).__name__)
                continue
            if latest is not None:
                changed.append(dict(latest))
        self._remove_ended()
        return changed

    def _check(self, record: dict) -> dict | None:
        """Bring one run up to date. Gives back its record when that changed."""
        current = self._reload(record)
        if current.get("status") not in STATUSES or _last_change(current) is None:
            current = self._finish(record, "The run record is damaged.")
        elif current["status"] not in TERMINAL:
            current = self._verdict(record, current) or current
        latest = {**record, **current}
        if latest["status"] in TERMINAL:
            self._end(record)  # the record the manager tracks names the run and its job
            return latest
        if latest == record:
            return None
        with self._lock:
            self._active[record["id"]] = latest
        return latest

    def _reload(self, record: dict) -> dict:
        """The stored record, or the one we know when it is missing or does not parse."""
        try:
            stored = load_run(record["id"], self._store)
        except Exception:  # the clock still has its say: else the slot is held forever
            late = self._late(record, record)
            if late is None:
                raise  # poll logs it and tries again
            return self._finish(record, late)
        return record if stored is None else stored

    def _verdict(self, record: dict, current: dict) -> dict | None:
        """The finished record when the runner or the clock say the run is over, else None."""
        state, state_error = None, None
        try:
            state = self._runner.state(record)
        except Exception as error:  # the clock still has its say: else the slot is held forever
            state_error = error
        if state == "done" and record["kind"] == "job":  # no task writes the record of a desk job
            return self._finish(record, None)
        if state in STATE_ERRORS:
            return self._finish(record, STATE_ERRORS[state])
        late = self._late(record, current)
        if late is not None:
            return self._finish(record, late)
        if state_error is not None:  # not over yet: poll logs the error and keeps the run
            raise state_error
        return None

    def _late(self, record: dict, current: dict) -> str | None:
        """The timeout text when `current` has not changed for too long, else None."""
        timeout = self._job_timeout if record["kind"] == "job" else self._timeout
        changed = _last_change(current)
        if changed is not None and self._now() - changed <= timeout:
            return None
        return f"The run timed out after {timeout.total_seconds() / 60:g} minutes."

    def _finish(self, record: dict, error: str | None) -> dict:
        """Store the run as failed with `error`, or as done when there is none.
        A record that the task has finished by now stays as it is."""
        # no compare-and-set in the store: a task that ends after this read can be overwritten
        try:
            stored = load_run(record["id"], self._store)
        except Exception:  # it cannot be read: the verdict stands
            stored = None
        if stored is not None and stored.get("status") in TERMINAL:
            return {**record, **stored}
        finished = {**record, "status": "done" if error is None else "failed", "error": error}
        save_run(finished, self._store)
        return finished

    # ---- removing the workflow of an ended run

    def _end(self, record: dict) -> None:
        """The run is over: its slot is free at once, and its workflow goes later."""
        ended = self._now()
        kept = {key: value for key, value in record.items() if key != "result"}  # can be large
        with self._lock:
            self._active.pop(record["id"], None)
            self._starting.discard(record["id"])
            if record["kind"] != "job":  # a desk workflow stays where it is
                self._ended[record["id"]] = (kept, ended)

    def _remove_ended(self) -> None:
        """Remove each workflow that is due: its run has ended on the platform too and the keep
        period has passed, or STOP_AFTER more has passed. A failure is tried at the next poll."""
        with self._lock:
            ended = list(self._ended.items())
        for run_id, (record, ended_at) in ended:
            waited = self._now() - ended_at
            if waited < self._keep_finished:
                continue
            backstop = waited >= self._keep_finished + STOP_AFTER  # whatever the platform says
            try:
                if not backstop and self._runner.state(record) == "running":
                    continue  # the task has written its record, and its run is still ending
                self._runner.cleanup(record)  # it stops a run that still runs, then removes it
            except Exception as error:
                logger.warning("Could not clean up run %s: %s", run_id, type(error).__name__)
                continue
            with self._lock:
                self._ended.pop(run_id, None)

    # ---- the app starting up

    def resume(self) -> int:
        """Take up the unfinished runs of a past app and remove its leftover jobs. Never raises.

        Gives back how many runs it took up. Clicks wait until the runs are taken up."""
        try:
            taken = self._take_up()
        finally:
            self._resumed.set()  # also after a failure: a click waits no longer
        try:
            self._runner.remove_stale(STALE_AFTER)
        except Exception as error:
            logger.warning("Could not remove old run jobs: %s", type(error).__name__)
        return taken

    def _take_up(self) -> int:
        try:
            records = list_runs(self._store, limit=200)
        except Exception as error:
            logger.warning("Could not list the runs: %s", type(error).__name__)
            records = []
        with self._lock:
            fresh = [record for record in records
                     if record.get("status") not in TERMINAL and record.keys() >= TRACKED_KEYS
                     and isinstance(record["id"], str) and record["id"] not in self._active]
            self._active.update((record["id"], record) for record in fresh)
        return len(fresh)

    def active(self) -> list[dict]:
        with self._lock:
            return [dict(record) for record in self._active.values()]
