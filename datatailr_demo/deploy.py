# *************************************************************************
#
#  Copyright (c) 2026 - Datatailr Inc.
#  All Rights Reserved.
#
#  This file is part of Datatailr and subject to the terms and conditions
#  defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  of this file, in parts or full, via any medium is strictly prohibited.
# *************************************************************************
import sys
from datatailr.logging import CYAN
from datatailr import workflow, App, Service, ExcelAddin, Resources
from data_pipelines.data_processing import func_no_args


@workflow(name="Simple Data Pipeline with One Task")
def data_pipeline_with_one_task():
    func_no_args()



def simple_workflow():
    from data_pipelines.deploy import simple_data_pipeline
    return simple_data_pipeline


def simple_app(framework: str = "streamlit"):
    name = f"Simple {framework.capitalize()} App"
    resources = Resources(memory="2g", cpu=1)

    if framework == "streamlit":
        import dashboards.streamlit.app as entrypoint

        python_requirements = ["streamlit", "pandas"]
    elif framework == "dash":
        import dashboards.dash.app as entrypoint

        python_requirements = ["dash", "numpy", "pandas", "plotly", "gunicorn"]
    elif framework == "flask":
        import dashboards.flask.app as entrypoint

        python_requirements = ["flask", "gunicorn"]
    elif framework == "panel":
        import dashboards.panel as entrypoint

        python_requirements = ["panel"]
    elif framework == "fastapi":
        import dashboards.fastapi.app as entrypoint

        python_requirements = ["fastapi", "uvicorn", "jinja2", "python-multipart"]
    else:
        raise ValueError(f"Unsupported framework '{framework}' for app deployment.")

    return App(
        name=name,
        entrypoint=entrypoint,
        framework=framework,
        resources=resources,
        app_section='Demo Apps',
        python_requirements=python_requirements,
    )


def simple_service():
    from services.flask_service import main

    service = Service(
        name="Simple Service",
        entrypoint=main,
        python_requirements=["flask"],
    )
    return service


def simple_excel_addin():
    from excel_addins.addin import main as addin_main

    addin = ExcelAddin(
        name="Simple Excel Addin",
        entrypoint=addin_main,
        resources=Resources(memory="4g", cpu=1),
        python_version="3.10",
        python_requirements=["numpy", "pandas", "requests"],
    )
    return addin

def notebook(voila=True):
    from pathlib import Path
    from datatailr import App

    path_to_notebook = Path(__file__).parent / "notebooks" / "demo_notebook.ipynb"
    assert path_to_notebook.exists(), f"Notebook not found: {path_to_notebook}"

    python_requirements=["jupyter", "pandas", "perspective-python", "jupyterlab_widgets", "pyarrow", "networkx", "ipycytoscape", "bqplot"]
    if voila:
        python_requirements += ['voila']
    framework = "voila" if voila else "jupyter"
    notebook = App(
        name=f"{framework.capitalize()} Notebook",
        entrypoint=str(path_to_notebook),
        framework=framework,
        resources=Resources(memory="2g", cpu=1),
        python_requirements=python_requirements
        )
    return notebook

def deploy_pipeline():
    wf = simple_workflow()
    print(CYAN("Deploying workflow..."))
    wf()


def deploy_app(framework: str = "streamlit"):
    if framework not in ["streamlit", "dash", "flask", "panel", "fastapi", "jupyter", "voila"]:
        raise ValueError(f"Unsupported framework '{framework}' for app deployment.")

    if framework == "jupyter":
        app = notebook(voila=False)
    elif framework == "voila":
        app = notebook(voila=True)
    else:
        app = simple_app(framework=framework)
    print(CYAN("Deploying app..."))
    app.run()


def deploy_service():
    """
    The service will be deployed and accessible at curl dev.simple-service/job/dev/simple-service/
    """
    service = simple_service()
    print(CYAN("Deploying service..."))
    service.run()


def deploy_excel_addin():
    addin = simple_excel_addin()
    print(CYAN("Deploying excel add-in..."))
    addin.run()


def deploy_dag_generator(num_tasks: int):
    from data_pipelines.dag_generator import generate_dag

    print(CYAN(f"Generating and deploying DAG with {num_tasks} tasks..."))
    wf = generate_dag(num_tasks)
    wf()


def deploy_all():
    deploy_pipeline()
    deploy_app()
    deploy_app('voila')
    deploy_service()
    deploy_excel_addin()


if __name__ == "__main__":
    usage = """
    python deploy.py <command>
    Commands:
    - all: Deploy all components
    - workflow: Deploy the workflow
    - app: Deploy the app
    - service: Deploy the service
    - excel: Deploy the Excel add-in
    """
    if len(sys.argv) < 2:
        print(usage)
        sys.exit(1)
    command = sys.argv[1]
    if command == "all":
        deploy_all()
    elif command == "workflow":
        deploy_pipeline()
    elif command == "app":
        framework = sys.argv[2] if len(sys.argv) > 2 else "streamlit"
        deploy_app(framework)
    elif command == "service":
        deploy_service()
    elif command == "excel-addin":
        deploy_excel_addin()
    else:
        print(f"Unknown command: {command}")
        print(usage)
        sys.exit(1)
    
