"""Stock price example: price server, CSP processor, unified Flask dashboard.

Processor cockpit uses http://price-processor unless PRICE_PROCESSOR_URL is set.
"""
from stock_price_processing.price_server.server import main as price_server_main
from stock_price_processing.price_processor.server import main as price_processing_main
from stock_price_processing.data_collector.service import main as data_collector_main
from datatailr import Service, App, Resources

import stock_price_processing.price_server_dashboard.monitoring as dashboard_entrypoint
import stock_price_processing.lake_query.dashboard as lake_dashboard_entrypoint
import stock_price_processing.price_processor.dashboard as processor_dashboard_entrypoint
from stock_price_processing.compaction_workflow.deploy import hourly_compaction_workflow


price_server = Service(
    name="Price Server",
    entrypoint=price_server_main,
    resources=Resources(memory="1g", cpu=1),
    python_requirements="stock_price_processing/requirements.txt",
)

price_processing = Service(
    name="Price Processor",
    entrypoint=price_processing_main,
    resources=Resources(memory="1g", cpu=1),
    python_requirements="stock_price_processing/requirements.txt",
)

data_collector = Service(
    name="Stock data collector",
    entrypoint=data_collector_main,
    resources=Resources(memory="2g", cpu=1),
    python_requirements="stock_price_processing/requirements.txt",
)

dashboard = App(
    name="Price Server Dashboard",
    entrypoint=dashboard_entrypoint,
    framework="flask",
    resources=Resources(memory="4g", cpu=1),
    python_requirements=["flask", "gunicorn", "requests"],
)

lake_dashboard = App(
    name="Lake Query Dashboard",
    entrypoint=lake_dashboard_entrypoint,
    framework="flask",
    resources=Resources(memory="4g", cpu=1),
    python_requirements=["flask", "gunicorn", "requests", "duckdb", "pyarrow", "pandas"],
)

processor_dashboard = App(
    name="Price Processor Dashboard",
    entrypoint=processor_dashboard_entrypoint,
    framework="flask",
    resources=Resources(memory="4g", cpu=1),
    python_requirements=["flask", "gunicorn", "requests"],
)

price_server.run()
price_processing.run()
data_collector.run()
dashboard.run()
lake_dashboard.run()
processor_dashboard.run()
hourly_compaction_workflow()
