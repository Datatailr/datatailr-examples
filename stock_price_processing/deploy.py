from stock_price_processing.price_server.server import main as price_server_main
from stock_price_processing.price_processing.server import main as price_processing_main
from datatailr import Service, App, Resources

import stock_price_processing.dashboard.monitoring as dashboard_entrypoint


price_server = Service(
    name="price_server",
    entrypoint=price_server_main,
    resources=Resources(memory="1g", cpu=1),
    python_requirements="stock_price_processing/requirements.txt",
)

price_processing = Service(
    name="price_processing",
    entrypoint=price_processing_main,
    resources=Resources(memory="2g", cpu=1),
    python_requirements="stock_price_processing/requirements.txt",
)

dashboard = App(
    name="Exchange Monitor",
    entrypoint=dashboard_entrypoint,
    framework="flask",
    resources=Resources(memory="1g", cpu=1),
    python_requirements=["flask", "gunicorn", "requests"],
)

price_server.run()
price_processing.run()
dashboard.run()
