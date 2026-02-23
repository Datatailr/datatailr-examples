import numpy as np
from datatailr.excel import Addin, Queue
import time
import datetime
import requests
from datatailr.logging import DatatailrLogger


logger = DatatailrLogger(__name__).get_logger()

addin = Addin("Demo Addin", "A simple example Excel add-in deployed on Datatailr")


@addin.expose(
    description="Adds 2 numbers", help="To add 2 numbers give them to the function"
)
def add(a: float, b: float) -> float:
    return a + b


@addin.expose(
    description="Subtracts 2 numbers",
    help="To subtract 2 numbers give them to the function",
)
def sub(a: float, b: float) -> float:
    return a - b


@addin.expose(
    description="Gets a random number from Data Service",
    help="Provide min and max values",
)
def random(min_val: int, max_val: int) -> int:
    data_service_url = "http://simple-service-<>USERNAME<>"
    r = requests.get(
        f"{data_service_url}/random",
        params={"min": int(min_val), "max": int(max_val)},
        timeout=2,
    )
    if r.status_code == 200:
        rand_num = r.json().get("random_number", None)
        logger.info(f"Received random number {rand_num} from data service")
        return rand_num
    else:
        logger.error("Data service error on /random.")
        return -1


@addin.expose(
    description="Streams a matrix of random numbers",
    help="Provide the dimensions of the random matrix",
    streaming=True,
)
def random_matrix(queue: Queue, m: int, n: int) -> list:
    X = np.random.rand(m, n)
    num_cells_to_change = min(m, n)
    while True:
        random_rows = np.random.choice(m, num_cells_to_change, replace=False)
        random_cols = np.random.choice(n, num_cells_to_change, replace=False)
        for row, col in zip(random_rows, random_cols):
            X[row, col] = np.random.rand()
        queue.push(X.tolist())
        time.sleep(0.1)


@addin.expose(
    description="Stream a random price process",
    help="Provide the initial price",
    streaming=True,
)
def stream_price(queue: Queue, p: float) -> list:
    now = datetime.datetime.now()
    N = 1000
    μ, σ = 0.1, 0.2
    freq = 1
    time.sleep(np.random.exponential(freq))
    scale = N * 365 * 24 * 60 * 60 / freq
    returns = np.random.normal(loc=μ / scale, scale=σ / np.sqrt(scale), size=N)
    prices = (p * np.exp(returns.cumsum())).tolist()
    times = [
        (now + datetime.timedelta(seconds=t)).strftime("%x %X") for t in range(1000)
    ]
    while True:
        ret = np.random.normal(loc=μ / scale, scale=σ / np.sqrt(scale), size=1)
        new_price = prices[-1] * (1 + ret[0])
        new_time = (
            datetime.datetime.strptime(times[-1], "%x %X")
            + datetime.timedelta(seconds=1)
        ).strftime("%x %X")
        prices = prices[1:] + [new_price]
        times = times[1:] + [new_time]
        queue.push([[t, p] for t, p in zip(times, prices)])
        time.sleep(0.1)


def main(port=8080, ws_port=8000):
    addin.run(port, ws_port)


if __name__ == "__main__":
    main()
