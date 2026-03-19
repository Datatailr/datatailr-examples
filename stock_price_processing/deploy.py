from stock_price_processing.price_server.server import main
from datatailr import Service, Resources

service = Service(
    name="price_server",
    entrypoint=main,
    resources=Resources(memory="1g", cpu=1),
    python_requirements='stock_price_processing/requirements.txt',
)

service.run()