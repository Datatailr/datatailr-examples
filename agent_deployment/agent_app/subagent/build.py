"""Build a uniquely-named sub-agent ``@workflow`` at runtime (§8, §10, §12).

The coordinator calls :func:`build_subagent_workflow` to obtain a fresh
``@workflow``-decorated function whose display name embeds the ``subagent_id``,
then calls it to launch a run (build-then-call, exactly like
``gas_curve_backtest.build_regime_workflow``). Each concurrent sub-agent gets
its own uniquely-named workflow definition so parallel launches never clobber
one another's version history.

The image is built on first launch and cached for subsequent runs; it installs
Node/``pi``/``fd``/``rg`` plus ``git``/``openssh-client``/``gh`` via the shared
``build_script_pre.sh`` (§5, §12).
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from datatailr import Resources, workflow

from agent_app.subagent.run_subagent import run_subagent

# Both files live in the shipped ``agent_app`` package, so these paths resolve
# both at deploy time (local) and at build-then-call time (inside the App
# container, under the site-packages install path).
_AGENT_APP_DIR = Path(__file__).resolve().parent.parent
_BUILD_SCRIPT = str(_AGENT_APP_DIR / "build_script_pre.sh")
_REQUIREMENTS = str(_AGENT_APP_DIR / "requirements.txt")


def build_subagent_workflow(
    subagent_id: str,
    name: str,
    *,
    wall_clock: str = "45m",
    turn_timeout_s: int = 600,
    model: Optional[str] = None,
    thinking: Optional[str] = None,
    memory: str = "2g",
    cpu: float = 1,
):
    """Return a fresh ``@workflow`` function for one sub-agent.

    ``name`` must be the exact display name produced by
    ``orchestration.workflow_name`` and persisted by the coordinator so the
    handle can be reopened for polling/harvesting later.
    """
    env_vars = {
        # Hard per-turn ceiling read by pi_runner at import time (§11 point 1).
        "PI_TIMEOUT_SECONDS": str(int(turn_timeout_s)),
    }
    if model:
        env_vars["AGENT_MODEL"] = model
    if thinking:
        env_vars["AGENT_THINKING"] = thinking

    @workflow(
        name=name,
        python_requirements=_REQUIREMENTS,
        resources=Resources(memory=memory, cpu=cpu),
        build_script_pre=_BUILD_SCRIPT,
        env_vars=env_vars,
        # Wall-clock stop condition: Datatailr marks the run `failed_after`
        # and the coordinator records a timeout (§11 point 3).
        fail_after=wall_clock,
    )
    def subagent_workflow():
        run_subagent(subagent_id).alias("run_subagent")

    return subagent_workflow
