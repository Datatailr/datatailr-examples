from websockets.sync.client import connect


host, port = "localhost", 8080

with connect(f"ws://{host}:{port}/ws") as websocket:
    while True:
        message = websocket.recv()
        print(f"Received: {message}")