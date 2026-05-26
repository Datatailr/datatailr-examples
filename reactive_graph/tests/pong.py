import zmq


def main(port: int):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{port}")

    while True:
        message = socket.recv()
        print("Received request: %s" % message)
        socket.send(b"World")

if __name__ == "__main__":
    import os
    main(int(os.environ.get("PORT", 1024)))