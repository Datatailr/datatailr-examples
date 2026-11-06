"""The Merit Desk app: the desk page, a JSON API for its four functions, and one live stream.

The manager and the runner modules load the Datatailr SDK. The app imports them when it starts,
not when this module is imported, so `import merit_desk.desk_app.app` loads no SDK module.
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import pathlib
import re
import threading
import time
from contextlib import asynccontextmanager
from datetime import date
from types import SimpleNamespace
from typing import TYPE_CHECKING, Annotated, Awaitable, Callable, Generic, Literal, TypeVar

from fastapi import FastAPI, HTTPException, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from merit_desk.common.curve import build_curve
from merit_desk.common.dates import london_today
from merit_desk.common.markets import MARKETS, UnknownMarketError, get_market
from merit_desk.common.runs import TERMINAL, list_run_ids, load_run
from merit_desk.common.scenario import (PlantOverride, Scenario, ScenarioError, scenario_curve,
                                        stack_view)
from merit_desk.common.settings import get_sweep_size
from merit_desk.common.store import (SAMPLE_FORECAST, default_store, latest_backtest_summary,
                                     newest_forecast)
from merit_desk.common.weather import CREDIT, Forecast
from merit_desk.desk_app import client

if TYPE_CHECKING:
    from merit_desk.common.store import TextStore
    from merit_desk.desk_app.runner import Runner

logger = logging.getLogger(__name__)

NO_SWEEP = "No sweep in this environment yet."
NO_LAST_RUN = "The last run could not be read from the platform."
NO_SUCH_RUN = "No such run."
NOT_JSON = "The request body is not valid JSON."
FEED_DOWN = "The Market Feed is not reachable."
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}-[0-9a-f]{6}")  # what new_run makes: the blob name uses it
MAX_OVERRIDES = 10  # a stack has five plants
FORECAST_SECONDS = 60.0  # how long the previews reuse the forecast they read
KEEP_ENDED = 500  # summaries of ended runs that the run list keeps
STATIC = pathlib.Path(__file__).with_name("static")
ECMWF_NOTICE = ('© <span id="credit-year">{{year}}</span> European Centre for Medium-Range '
                'Weather Forecasts (ECMWF). '
                '<a href="https://apps.ecmwf.int/datasets/licences/general/" target="_blank" '
                'rel="noopener">ECMWF terms of use</a>')

Market = Annotated[str, Field(max_length=8)]  # a short text: an unknown market is echoed back


# ---- what a request may hold: the bounds of the desk, checked before any work starts


class Body(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)


class Override(Body):
    name: str = Field(max_length=40)
    cost_eur_mwh: float | None = Field(None, ge=0, le=1000)  # None keeps the plant's own value
    capacity_gw: float | None = Field(None, ge=0, le=200)


class StackIn(Body):
    market: Market
    temp_c: float = Field(ge=-40, le=45)
    overrides: list[Override] = Field(default_factory=list, max_length=MAX_OVERRIDES)


class ScenarioIn(Body):
    market: Market
    shift_c: float = Field(0.0, ge=-10, le=10)
    start: date | None = None  # the window must lie inside the curve: scenario_curve checks that
    end: date | None = None
    overrides: list[Override] = Field(default_factory=list, max_length=MAX_OVERRIDES)


class ScenarioInputs(ScenarioIn):
    n_paths: int = Field(1000, ge=100, le=5000)


class BacktestInputs(Body):
    market: Market
    tenor: int = Field(ge=1, le=6)
    threshold: float = Field(ge=0.25, le=3.0)


class ScenarioRun(Body):
    kind: Literal["scenario"]
    inputs: ScenarioInputs


class BacktestRun(Body):
    kind: Literal["backtest"]
    inputs: BacktestInputs


RunIn = Annotated[ScenarioRun | BacktestRun, Field(discriminator="kind")]
RUN_KINDS = ("scenario", "backtest")  # pydantic puts the kind of the run into the error's place


def overrides_of(body: StackIn | ScenarioIn) -> tuple[PlantOverride, ...]:
    return tuple(PlantOverride(**o.model_dump()) for o in body.overrides)


def scenario_of(body: ScenarioIn) -> Scenario:
    return Scenario(get_market(body.market).code, body.shift_c, body.start, body.end,
                    overrides_of(body))


def stored_inputs(body: ScenarioRun | BacktestRun) -> dict:
    """The inputs as the run record keeps them, and as the task reads them."""
    i = body.inputs
    market = get_market(i.market).code
    if body.kind == "backtest":
        return {"market": market, "tenor": i.tenor, "threshold": i.threshold}
    return {"market": market, "shift_c": i.shift_c,
            "start": i.start.isoformat() if i.start else None,
            "end": i.end.isoformat() if i.end else None,
            "n_paths": i.n_paths, "overrides": [o.model_dump() for o in i.overrides]}


# ---- the live stream


def summary(record: dict) -> dict:
    """The record without its result, which can be large. `has_result` says if there is one.

    The run list and the run events carry this. GET /api/runs/{id} gives the whole record."""
    rest = {key: value for key, value in record.items() if key != "result"}
    return {**rest, "has_result": record.get("result") is not None}


class RunList:
    """The newest runs, as summaries, for the runs rail.

    The record of an ended run does not change, so it is read once and its summary kept. A list
    then costs one listing of the names, and a read of each run that is new or still active."""

    def __init__(self, store: TextStore) -> None:
        self._store = store
        self._ended: dict[str, dict] = {}  # run id -> its summary
        self._lock = threading.Lock()  # requests run in threads

    def newest(self, limit: int) -> list[dict]:
        with self._lock:
            known = dict(self._ended)
        found, ended = [], {}
        for run_id in list_run_ids(self._store):
            if len(found) == limit:
                break
            item = known.get(run_id)
            if item is None:
                record = load_run(run_id, self._store)  # a bad or vanished record is skipped
                if record is None:
                    continue
                item = summary(record)
                if item.get("status") in TERMINAL:
                    ended[run_id] = item
            found.append(item)
        with self._lock:
            self._ended.update(ended)
            for run_id in sorted(self._ended)[:-KEEP_ENDED]:  # the oldest: no page lists them
                del self._ended[run_id]
        return found


def reason(problem: dict) -> str:
    """'inputs.shift_c: Input should be ...'. The place drops 'body' and the kind of the run."""
    place = list(problem["loc"][1:])
    if place[:1] and place[0] in RUN_KINDS:
        place = place[1:]
    field = ".".join(str(part) for part in place)
    return f"{field}: {problem['msg']}" if field else problem["msg"]


def frame(event: str, data: dict) -> str:
    """One server-sent event. The JSON has no line breaks, so one data line holds it."""
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


class Broadcaster:
    """Gives each message to every subscriber, each with a bounded queue of its own.

    Call it from the event loop only: an asyncio queue is not thread-safe."""

    def __init__(self, size: int = 100) -> None:
        self._size = size
        self._queues: set[asyncio.Queue] = set()

    @property
    def subscribers(self) -> int:
        return len(self._queues)

    def subscribe(self) -> asyncio.Queue:
        queue = asyncio.Queue(self._size)
        self._queues.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._queues.discard(queue)

    def publish(self, message: str) -> None:
        for queue in self._queues:
            if queue.full():
                queue.get_nowait()  # a slow page loses its oldest message, and nobody waits
            queue.put_nowait(message)


async def forever(seconds: float, work: Callable[[], Awaitable[None]]) -> None:
    """Do the work again and again. A failure is logged and does not end the loop."""
    while True:
        try:
            await work()
        except Exception:
            logger.exception("A background step failed. Trying again in %s s.", seconds)
        await asyncio.sleep(seconds)


# ---- the page, and the forecast that the previews share


T = TypeVar("T")


class Cached(Generic[T]):
    """The value of `read`, read again once it is `seconds` old. Threads may share it."""

    def __init__(self, read: Callable[[], T], seconds: float,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self._read, self._seconds, self._clock = read, seconds, clock
        self._lock = threading.Lock()
        self._value: T | None = None
        self._read_at: float | None = None

    def __call__(self) -> T:
        with self._lock:  # the first caller reads, the others wait for its value
            now = self._clock()
            if self._read_at is None or now - self._read_at >= self._seconds:
                self._value, self._read_at = self._read(), now
            return self._value


def page_html() -> str:
    """The page, with the forecast credit and the ECMWF notice in its footer. Its {{base}} and
    {{year}} are filled for each request. The Curves page then puts the year of the forecast run
    in place of {{year}}."""
    page = (STATIC / "index.html").read_text(encoding="utf-8")
    return page.replace("{{credit}}", html.escape(CREDIT)).replace("{{notice}}", ECMWF_NOTICE)


def forecast_or_sample(store: TextStore) -> Forecast:
    """The newest forecast, or the bundled sample when the store cannot be read (logged)."""
    try:
        return newest_forecast(store)
    except Exception as error:  # the previews still work, on the sample
        logger.warning("Could not read the newest forecast: %s. Using the sample.",
                       type(error).__name__)
        return Forecast.from_json(SAMPLE_FORECAST.read_text(encoding="utf-8"))


def sweep_size() -> str | None:
    """The variable sweep_size, or None when it cannot be read."""
    try:
        return get_sweep_size()
    except Exception as error:  # its text could hold a command line
        logger.warning("Could not read the variable sweep_size: %s", type(error).__name__)
        return None


# ---- the viewer and the environment


def environment() -> str:
    return os.environ.get("DATATAILR_JOB_ENVIRONMENT", "dev").lower()


def header_name(request: Request) -> str | None:
    """The name in the x-datatailr-user header, for the top bar. The platform put it there."""
    try:
        name = json.loads(request.headers.get("x-datatailr-user", "null")).get("name")
    except (ValueError, AttributeError):
        return None
    return name if isinstance(name, str) and name else None


def viewer_name(request: Request) -> str | None:
    """The user the platform knows, for the run record. Any failure leaves the name empty."""
    try:
        from datatailr import User  # a call to the platform: only a run start needs it

        user = User.from_request(request)
        return user.name if user else None
    except Exception as error:
        logger.warning("Could not look up the user: %s", type(error).__name__)
        return None


def real_runner(store: TextStore) -> Runner:
    """The local runner for a laptop (MERIT_DESK_RUNNER=local), else the platform's."""
    from merit_desk.desk_app.runner import LocalRunner, PlatformRunner

    if os.environ.get("MERIT_DESK_RUNNER") == "local":
        return LocalRunner(store)
    return PlatformRunner(environment())


# ---- the app


def create_app(store: TextStore | None = None, runner: Runner | None = None,
               prices: Callable[[], dict] | None = None,
               curves: Callable[[str], dict] | None = None, *, price_interval: float = 1.0,
               poll_interval: float = 3.0, heartbeat: float = 15.0) -> FastAPI:
    """The app. What is None becomes the real thing when the app starts, not before.

    The app works while it runs: uvicorn starts it, and a test uses `with TestClient(app)`."""
    desk = SimpleNamespace()  # store, runner, manager, prices and curves: set at startup
    bus = Broadcaster()

    async def read_prices() -> None:
        if not bus.subscribers:  # no page is open: the Market Feed is not asked
            return
        try:
            data = await asyncio.to_thread(desk.prices)
        except client.ServiceUnavailable as error:  # the stream stays open with the service's text
            data = {"error": str(error)}
        except Exception:  # any other text could hold a path or a command line
            logger.exception("Could not read the prices of the Market Feed.")
            data = {"error": FEED_DOWN}
        bus.publish(frame("prices", data))

    async def publish_changes() -> None:
        for record in await asyncio.to_thread(desk.manager.poll):
            bus.publish(frame("run", summary(record)))

    async def watch_runs() -> None:
        await asyncio.to_thread(desk.manager.resume)  # once: take up the runs of an old app
        await forever(poll_interval, publish_changes)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        from merit_desk.desk_app.manager import RunManager

        desk.store = default_store() if store is None else store
        desk.runner = real_runner(desk.store) if runner is None else runner
        desk.manager = RunManager(desk.store, desk.runner, resume_pending=True)  # see watch_runs
        desk.runs = RunList(desk.store)
        desk.prices = client.get_latest_prices if prices is None else prices
        desk.curves = client.get_curve if curves is None else curves
        desk.forecast = Cached(lambda: forecast_or_sample(desk.store), FORECAST_SECONDS)
        loops = [asyncio.create_task(forever(price_interval, read_prices)),
                 asyncio.create_task(watch_runs())]
        try:
            yield
        finally:
            for loop in loops:
                loop.cancel()
            await asyncio.gather(*loops, return_exceptions=True)
            desk.runner.shutdown()

    app = FastAPI(title="Merit Desk", lifespan=lifespan)
    app.state.bus = bus

    @app.exception_handler(RequestValidationError)
    def invalid_request(request: Request, error: RequestValidationError) -> JSONResponse:
        # FastAPI's own reply repeats the input, and JSON cannot hold it when it is NaN
        problems = error.errors()
        if any(problem["type"] == "json_invalid" for problem in problems):
            return JSONResponse({"detail": NOT_JSON}, status_code=422)
        return JSONResponse({"detail": "; ".join(map(reason, problems))}, status_code=422)

    @app.exception_handler(ScenarioError)
    def scenario_refused(request: Request, error: ScenarioError) -> JSONResponse:
        return JSONResponse({"detail": str(error)}, status_code=422)

    @app.exception_handler(UnknownMarketError)
    def unknown_market(request: Request, error: UnknownMarketError) -> JSONResponse:
        return JSONResponse({"detail": str(error)}, status_code=404)

    page = page_html()  # read once: the page does not change while the app runs

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request) -> str:
        # the platform serves the app under /job/<env>/<name>: the page's links start there
        root = html.escape(request.scope.get("root_path", ""))
        return page.replace("{{base}}", root + "/").replace("{{year}}", str(london_today().year))

    app.mount("/static", StaticFiles(directory=STATIC), name="static")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/api/context")
    def context(request: Request) -> dict:
        return {"user": header_name(request), "environment": environment()}

    @app.get("/api/curves")
    async def get_curves() -> dict:
        # the three calls run side by side, and a failure comes back as a value, not a raise
        results = await asyncio.gather(*(asyncio.to_thread(desk.curves, m.code) for m in MARKETS),
                                       return_exceptions=True)
        found, errors = [], {}
        for market, result in zip(MARKETS, results):
            if isinstance(result, client.ServiceUnavailable):
                errors[market.code] = str(result)
            elif isinstance(result, BaseException):
                raise result
            else:
                found.append(result)
        return {"curves": found, "errors": errors}

    @app.post("/api/stack/preview")
    def stack_preview(body: StackIn) -> dict:
        return stack_view(body.market, body.temp_c, overrides_of(body))

    @app.post("/api/scenario/preview")
    def scenario_preview(body: ScenarioIn) -> dict:
        forecast, today = desk.forecast(), london_today()
        scenario = scenario_of(body)
        base = build_curve(scenario.market, forecast, today)
        shifted = scenario_curve(scenario, forecast, today)
        return {"months": base.months, "base": base.prices, "scenario": shifted.prices}

    @app.get("/api/sweep/{market}")
    def sweep(market: Annotated[str, Path(max_length=8)]) -> dict:
        code = get_market(market).code
        sweep_summary = latest_backtest_summary(desk.store)
        if sweep_summary is None:
            raise HTTPException(404, NO_SWEEP)
        results = [r for r in sweep_summary["results"] if r["market"] == code]
        cells = {(r["tenor"], r["threshold"]): r["total_pnl_eur"] for r in results}
        tenors = sorted({tenor for tenor, _ in cells})
        thresholds = sorted({threshold for _, threshold in cells})
        return {"as_of": sweep_summary["as_of"], "size": sweep_summary["size"], "tenors": tenors,
                "thresholds": thresholds,
                "pnl": [[cells.get((tenor, th)) for th in thresholds] for tenor in tenors]}

    def checked_inputs(body: ScenarioRun | BacktestRun) -> dict:
        """The stored inputs. A scenario is priced once first, so that a window or a stack that
        cannot work is refused here, and no job starts for it."""
        inputs = stored_inputs(body)
        if body.kind == "scenario":
            scenario_curve(scenario_of(body.inputs), desk.forecast(), london_today())
        return inputs

    @app.post("/api/runs", status_code=202)
    async def start_run(body: RunIn, request: Request) -> dict:
        from merit_desk.desk_app.manager import TooManyRuns

        inputs = await asyncio.to_thread(checked_inputs, body)
        started_by = await asyncio.to_thread(viewer_name, request)
        try:
            record = await asyncio.to_thread(desk.manager.start, body.kind, inputs, started_by)
        except TooManyRuns as error:
            raise HTTPException(429, str(error)) from None
        bus.publish(frame("run", summary(record)))  # the other pages show the new run at once
        return record

    @app.get("/api/runs")
    def get_runs(limit: int = Query(50, ge=1, le=200)) -> list[dict]:
        return desk.runs.newest(limit)

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str) -> dict:
        if not RUN_ID.fullmatch(run_id):  # the blob name is built from it, and it is not echoed
            raise HTTPException(404, NO_SUCH_RUN)
        record = load_run(run_id, desk.store)
        if record is None:
            raise HTTPException(404, f"There is no run '{run_id}'.")
        return record

    def last_run(name: str) -> dict:
        try:
            return desk.runner.desk_job_state(name)
        except Exception as error:  # the platform's own words can hold a command line and paths
            logger.warning("Could not read the last run of %s: %s", name, type(error).__name__)
            return {"last_state": None, "last_time": None, "error": NO_LAST_RUN}

    @app.get("/api/jobs")
    def get_jobs() -> list[dict]:
        from merit_desk.desk_app.manager import DESK_JOBS

        jobs = [{"key": key, **job, **last_run(job["name"])} for key, job in DESK_JOBS.items()]
        for job in jobs:
            if job["key"] == "strategy-sweep":  # the grid is built from it at the next deploy
                job["sweep_size"] = sweep_size()
        return jobs

    @app.post("/api/jobs/{job}/start", status_code=202)
    async def start_job(job: str, request: Request) -> dict:
        from merit_desk.desk_app.manager import DESK_JOBS
        from merit_desk.desk_app.runner import RunRefused

        if job not in DESK_JOBS:
            known = ", ".join(DESK_JOBS)
            raise HTTPException(404, f"Unknown desk job '{job}'. Use one of: {known}.")
        started_by = await asyncio.to_thread(viewer_name, request)
        try:
            record = await asyncio.to_thread(desk.manager.start_desk_job, job, started_by)
        except RunRefused as refusal:
            if refusal.record is not None:  # every page shows the failed start at once
                bus.publish(frame("run", summary(refusal.record)))
            raise HTTPException(403, str(refusal)) from None
        bus.publish(frame("run", summary(record)))
        return record

    @app.get("/api/events")
    async def events() -> StreamingResponse:
        async def messages():
            queue = bus.subscribe()  # inside the generator, so that its finally always follows
            try:
                while True:
                    try:
                        yield await asyncio.wait_for(queue.get(), heartbeat)
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"  # keeps proxies from closing a quiet stream
            finally:
                bus.unsubscribe(queue)

        return StreamingResponse(messages(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    return app


app = create_app()  # the platform serves this one. It builds nothing real until it starts
