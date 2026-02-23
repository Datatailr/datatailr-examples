# *************************************************************************
#
#  Copyright (c) 2026 - Datatailr Inc.
#  All Rights Reserved.
#
#  This file is part of Datatailr and subject to the terms and conditions
#  defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  of this file, in parts or full, via any medium is strictly prohibited.
# *************************************************************************
from datatailr.logging import CYAN


from datatailr import workflow, App, Service, ExcelAddin, Resources
from data_pipelines.data_processing import func_no_args


@workflow(name="Simple Data Pipeline with One Task")
def data_pipeline_with_one_task():
    func_no_args()



def simple_workflow():
    from data_pipelines.data_processing import (
        get_data,
        process_data,
        get_number,
        add,
        get_number_from_service,
    )

    @workflow(name="Simple Data Pipeline", python_requirements=["requests"])
    def simple_data_pipeline():
        data = get_data()
        process_data(data)

        a = get_number().alias("a")
        b = get_number().alias("b")
        add(a, b).alias("Add a and b")
        add(a, 18).alias("Add a and 18")
        rand_low = get_number_from_service(0, 10).alias("Random 0-10")
        rand_high = get_number_from_service(90, 100).alias("Random 90-100")
        random = get_number_from_service(rand_low, rand_high).alias(
            "Random between previous two"
        )

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


def deploy_pipeline():
    wf = simple_workflow()
    print(CYAN("Deploying workflow..."))
    wf()


def deploy_app(framework: str = "streamlit"):
    if framework not in ["streamlit", "dash", "flask", "panel", "fastapi"]:
        raise ValueError(f"Unsupported framework '{framework}' for app deployment.")
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
    deploy_service()
    deploy_excel_addin()


if __name__ == "__main__":
    deploy_all()
