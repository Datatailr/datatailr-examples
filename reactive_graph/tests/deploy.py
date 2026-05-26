from datatailr import Service 
from reactive_graph.tests.pong import main

service = Service(
    name="pong",
    entrypoint=main,
    python_requirements='pyzmq',
)
service.run()