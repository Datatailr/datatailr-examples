"""Deploy the pi agent backend as a Datatailr Service.

Run from this directory (with the project venv active so the `dt` CLI is on
PATH):

    python deploy_service.py

Prerequisite: create an OpenAI API key secret named `openai_api_key` in the
Datatailr Secrets Manager UI. The service reads it at runtime.
"""

from pathlib import Path
from agent_service.service import main
from datatailr import Resources, Service

requirements_file = Path(__file__).parent / "agent_service" / "requirements.txt"
build_script_pre_file = Path(__file__).parent / "agent_service" / "build_script_pre.sh"

service = Service(
    name="Pi Agent Service",
    entrypoint=main,
    resources=Resources(memory="2g", cpu=1),
    python_requirements=str(requirements_file),
    build_script_pre=str(build_script_pre_file),
)

if __name__ == "__main__":
    service.run()
