from datatailr import Service 
from live_trading_system.tests.pong import main

service = Service(
    name="pong",
    entrypoint=main,
    python_requirements='pyzmq',
)
service.run()