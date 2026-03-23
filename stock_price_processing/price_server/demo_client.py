import requests


host, port = "localhost", 8080

with requests.get(f"http://{host}:{port}/stream", stream=True) as resp:
    for line in resp.iter_lines(decode_unicode=True):
        if line.startswith("data:"):
            print(f"Received: {line[5:].strip()}")
