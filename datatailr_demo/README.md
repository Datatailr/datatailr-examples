# Datatailr Demo

This folder is a deployment-ready demo package that showcases multiple job types on the Datatailr platform.
Each component can be deployed independently and used as a starting point for real projects.

## What this demo includes

1. **Simple Service**  
   A Flask-based service that accepts requests and returns responses.
2. **Simple Workflow**  
   A workflow/pipeline example for fetching and processing data.
3. **Excel Add-in**  
   A hosted Excel add-in with Python-powered functionality.
4. **App Collection (multiple frameworks)**  
   Example apps in `streamlit`, `dash`, `flask`, `panel`, and `fastapi`.

## Folder highlights

- `deploy.py`: Entry points for deploying each demo component.
- `services/`: Service example(s).
- `data_pipelines/`: Workflow and DAG examples.
- `excel_addins/`: Excel add-in implementation.
- `dashboards/`: App examples by framework.

## Prerequisites

- Python environment with Datatailr SDK available.
- Access/authentication configured for your Datatailr workspace.
- Run commands from this `datatailr_demo` directory.

## Deployment commands

Deploy everything at once (workflow + default app + service + Excel add-in):

```bash
python deploy.py
```

Deploy only the workflow:

```bash
python -c "from deploy import deploy_pipeline; deploy_pipeline()"
```

Deploy only the service:

```bash
python -c "from deploy import deploy_service; deploy_service()"
```

Deploy only the Excel add-in:

```bash
python -c "from deploy import deploy_excel_addin; deploy_excel_addin()"
```

Deploy one app framework:

```bash
python -c "from deploy import deploy_app; deploy_app('streamlit')"
python -c "from deploy import deploy_app; deploy_app('dash')"
python -c "from deploy import deploy_app; deploy_app('flask')"
python -c "from deploy import deploy_app; deploy_app('panel')"
python -c "from deploy import deploy_app; deploy_app('fastapi')"
```

Deploy a generated DAG workflow with `N` tasks:

```bash
python -c "from deploy import deploy_dag_generator; deploy_dag_generator(100)"
```

## Notes

- Resource settings and Python requirements are declared in `deploy.py`.
- Deployed names use generic demo identifiers.
- Service URL routing follows the naming convention documented in `deploy_service()`.
