from stock_price_processing.price_server.server import main
from datatailr import Service, App, Resources

import stock_price_processing.dashboard.monitoring as dashboard_entrypoint


service = Service(
    name="price_server",
    entrypoint=main,
    resources=Resources(memory="1g", cpu=1),
    python_requirements="stock_price_processing/requirements.txt",
)

dashboard = App(
    name="Exchange Monitor",
    entrypoint=dashboard_entrypoint,
    framework="flask",
    resources=Resources(memory="1g", cpu=1),
    python_requirements=["flask", "gunicorn"],
)

service.run()
dashboard.run()
