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
)

if __name__ == "__main__":
    app.run()
