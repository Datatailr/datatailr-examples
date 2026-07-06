"""Deploy the main agent as a Datatailr FastAPI App ("SWE Main Agent").

This single app runs the `pi` coding agent in-container and serves:
- an interactive xterm.js terminal wired to a live `pi` PTY over a WebSocket
- a JSON HTTP API (`/chat`, `/chat/stream`) for programmatic access
- an activity dashboard + a sub-agent panel sourced from the run registry
- the orchestration API (`/subagents`) and the coordinator that spawns, tracks,
  and harvests sub-agent workflows (specification §6, §7)

The agent runtime lives in the app (not a separate service) because Datatailr's
internal service-to-service routing does not forward WebSocket upgrades, whereas
the public app ingress does -- so the terminal's WebSocket must terminate here.
The coordinator builds and launches each sub-agent workflow at runtime
(build-then-call), so the sub-agent code ships as a subpackage of `agent_app`.

Run from this directory (with the project venv active so the `dt` CLI is on
PATH):

    python deploy_app.py
"""

from pathlib import Path

import agent_app.app as entrypoint
from datatailr import App, Resources

_here = Path(__file__).parent
requirements_file = _here / "agent_app" / "requirements.txt"
build_script_pre_file = _here / "agent_app" / "build_script_pre.sh"

app = App(
    name="SWE Main Agent",
    entrypoint=entrypoint,
    framework="fastapi",
    resources=Resources(memory="2g", cpu=1),
    python_requirements=str(requirements_file),
    # Installs Node.js + the pi CLI, git/openssh-client/gh, and the
    # spawn_subagent helper into the app image.
    build_script_pre=str(build_script_pre_file),
)

if __name__ == "__main__":
    app.run()
