"""Deploy the pi agent GUI as a Datatailr Flask App.

Run from this directory (with the project venv active so the `dt` CLI is on
PATH), after the service is deployed:

    python deploy_app.py
"""

import agent_app.app as entrypoint
from datatailr import App, Resources
from pathlib import Path

requirements_file = Path(__file__).parent / "agent_app" / "requirements.txt"

app = App(
    name="Pi Agent UI",
    entrypoint=entrypoint,
    framework="flask",
    resources=Resources(memory="1g", cpu=1),
    python_requirements=str(requirements_file),
    # Streaming (SSE) responses keep a worker busy for the whole agent turn.
    # Use threaded workers with no request timeout so long streams aren't killed,
    # and allow other requests (sessions/stats) to be served concurrently.
    env_vars={
        "GUNICORN_CMD_ARGS": "--worker-class gthread --workers 2 --threads 8 --timeout 0",
    },
)

if __name__ == "__main__":
    app.run()
