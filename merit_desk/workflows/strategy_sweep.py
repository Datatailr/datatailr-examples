"""The strategy sweep: one backtest task per grid point, all in parallel, then rank them."""

from pathlib import Path

from datatailr import Schedule, task, workflow

from merit_desk.common.backtest import run_backtest, sweep_grid, task_name
from merit_desk.common.dates import london_today
from merit_desk.common.settings import get_sweep_size
from merit_desk.common.store import default_store, save_backtest_summary

TOP_N = 10
REQUIREMENTS = str(Path(__file__).with_name("requirements.txt"))  # works from any folder


@task(memory="1g", cpu=1)  # generous for a small backtest, so a full sweep spreads over many nodes
def backtest(market: str, tenor: int, threshold: float) -> dict:
    return run_backtest(market, tenor, threshold)


@task(memory="512m")  # pandas alone takes about 96 MB, close to the 100 MB default
def summarize(results: list) -> dict:
    top = sorted(results, key=lambda r: r["total_pnl_eur"], reverse=True)[:TOP_N]
    summary = {"as_of": london_today().isoformat(), "size": len(results),
               "top": top, "results": results}
    save_backtest_summary(summary, default_store())
    return summary


@workflow(
    name="Merit Desk Strategy Sweep",
    # The platform rejects Sunday schedules in this release (it writes Sunday as day 7), so Saturday
    schedule=Schedule(at_hours=[2], at_minutes=[0], weekdays=["Sat"], timezone="Europe/London"),
    budget="Research",
    python_version="3.12",
    python_requirements=REQUIREMENTS,
)
def strategy_sweep():
    # bt_DE_M2_t15 = Germany, two months ahead, threshold 1.5 °C
    results = [backtest(m, t, th).alias(task_name(m, t, th))
               for m, t, th in sweep_grid(get_sweep_size())]
    summarize(results)
