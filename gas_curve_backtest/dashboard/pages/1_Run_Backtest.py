"""Configure and launch a backtest — laptop or Datatailr platform."""

from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import streamlit as st

from gas_curve_backtest.dashboard import _storage as storage
from gas_curve_backtest.local_run import run_locally


def _run_locally_threaded(params: dict, status_box: dict) -> None:
    def _cb(msg: str, progress: float) -> None:
        status_box["msg"] = msg
        status_box["progress"] = progress

    try:
        out = run_locally(progress_cb=_cb, **params)
        status_box["done"] = True
        status_box["result"] = out
    except Exception as e:
        status_box["done"] = True
        status_box["error"] = str(e)


def _launch_on_datatailr(params: dict) -> dict:
    """Deploy a fresh parent workflow that triggers the dynamic child."""
    from gas_curve_backtest.workflows.parent_workflow import (
        make_run_id,
        parent_backtest_workflow,
    )

    rid = make_run_id()
    params = {k: v for k, v in params.items() if k != "use_datatailr_blob"}
    parent_backtest_workflow(rid, **params)
    return {"run_id": rid}


def main() -> None:
    st.set_page_config(page_title="Run Backtest", layout="wide", page_icon=":rocket:")
    st.title("Run a Backtest")
    st.caption(
        "The same Python kernels run on your laptop and on Datatailr; "
        "the only difference is the scheduling boundary."
    )

    with st.form("backtest_params"):
        c1, c2, c3 = st.columns(3)
        n_days = c1.slider("Trading days", 200, 1500, 750, step=50)
        n_tenors = c2.slider("Tenors (M+1, ...)", 4, 12, 8)
        n_regimes = c3.slider("Regimes (KMeans)", 2, 8, 4)

        c4, c5, c6 = st.columns(3)
        sig_steps = c4.slider("Signal threshold grid", 5, 21, 11)
        pivot_steps = c5.slider("Asymmetry-pivot grid", 3, 15, 5)
        bootstrap = c6.slider("Bootstrap samples per cell", 1, 256, 64)

        cells_estimate = n_regimes * n_tenors * sig_steps * pivot_steps
        st.info(
            f"Approximate fan-out: **{cells_estimate:,} backtest cells** "
            f"× {bootstrap} bootstrap paths each."
        )

        submit_local = st.form_submit_button("Run on laptop", use_container_width=True)
        submit_remote = st.form_submit_button(
            "Run on Datatailr (deploy parent + child)", type="primary", use_container_width=True
        )

    params = dict(
        n_days=n_days,
        n_tenors=n_tenors,
        n_regimes=n_regimes,
        grid_signal_steps=sig_steps,
        grid_pivot_steps=pivot_steps,
        bootstrap_samples=bootstrap,
    )

    if submit_local:
        st.session_state["local_status"] = {"msg": "starting", "progress": 0.0}
        thread = threading.Thread(
            target=_run_locally_threaded,
            args=(params, st.session_state["local_status"]),
            daemon=True,
        )
        thread.start()
        st.session_state["local_thread_started"] = time.time()

    if "local_status" in st.session_state:
        box = st.session_state["local_status"]
        st.markdown("### Laptop run")
        prog = st.progress(min(1.0, max(0.0, box.get("progress", 0.0))))
        msg_ph = st.empty()
        msg_ph.info(box.get("msg", "..."))
        if box.get("done"):
            elapsed = time.time() - st.session_state["local_thread_started"]
            st.success(f"Completed in {elapsed:.1f}s")
            if box.get("error"):
                st.error(box["error"])
            else:
                result = box.get("result", {})
                rid = result.get("run_id")
                if rid:
                    st.session_state["active_run_id"] = rid
                    st.write(f"run_id: `{rid}`")
                    st.json(result.get("timings", {}))
        else:
            time.sleep(0.4)
            st.rerun()

    if submit_remote:
        if os.environ.get("DATATAILR_JOB_TYPE") in (None, "", "workstation"):
            st.error(
                "Deploy from a Datatailr-connected environment "
                "(`dt login` first), or run `python -m gas_curve_backtest.workflows.parent_workflow`."
            )
        else:
            with st.spinner("Deploying parent workflow on Datatailr..."):
                try:
                    out = _launch_on_datatailr(params)
                    rid = out.get("run_id")
                    st.session_state["active_run_id"] = rid
                    st.success(f"Launched! run_id: `{rid}`")
                    st.info(
                        "The parent workflow will detect regimes and "
                        "dynamically deploy a child workflow with one task per cell. "
                        "Open *Live Progress* to watch."
                    )
                except Exception as e:
                    st.error(f"Deployment failed: {e}")


main()
