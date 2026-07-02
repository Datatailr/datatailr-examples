"""Deploy the optional "SWE Agent Callback" Service (specification §10, §15).

This service is a low-latency wake-up only; the system works poll-only without
it. Ship it if harvest latency matters. A sub-agent can then POST its completion
to ``http://swe-agent-callback/notify`` and the coordinator harvests promptly.

Deploy order: deploy this before the main agent so the internal hostname exists.
Run from this directory with the project venv active so the `dt` CLI is on PATH:

    python deploy_callback.py
"""

from pathlib import Path

from agent_callback.app import main
from datatailr import Resources, Service

_here = Path(__file__).parent
requirements_file = _here / "agent_callback" / "requirements.txt"

service = Service(
    name="SWE Agent Callback",
    entrypoint=main,
    resources=Resources(memory="256m", cpu=0.25),
    python_requirements=str(requirements_file),
)

if __name__ == "__main__":
    service.run()
