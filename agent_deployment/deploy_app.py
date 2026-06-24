"""Deploy the self-contained pi agent as a Datatailr FastAPI App.

This single app runs the `pi` coding agent in-container and serves:
- an interactive xterm.js terminal wired to a live `pi` PTY over a WebSocket
- a JSON HTTP API (`/chat`, `/chat/stream`) for programmatic access
- an activity dashboard sourced from the on-disk `~/.pi` session store

The agent runtime lives in the app (not a separate service) because Datatailr's
internal service-to-service routing does not forward WebSocket upgrades, whereas
the public app ingress does -- so the terminal's WebSocket must terminate here.

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
    name="Pi Agent UI",
    entrypoint=entrypoint,
    framework="fastapi",
    resources=Resources(memory="2g", cpu=1),
    python_requirements=str(requirements_file),
    # Installs Node.js + the pi CLI into the app image.
    build_script_pre=str(build_script_pre_file),
)

if __name__ == "__main__":
    app.run()
