"""Demand training: build the training data, train a neural net on a GPU, save the model."""

from pathlib import Path

import numpy as np
from datatailr import task, workflow

from merit_desk.common.demand_model import evaluate_torch_model, features, train_torch_model
from merit_desk.common.history import synthetic_history
from merit_desk.common.markets import get_market
from merit_desk.common.store import env_prefix, save_model

MARKET = "DE"
MODEL_NAME = f"demand-{MARKET}.pt"
REQUIREMENTS = str(Path(__file__).with_name("training-requirements.txt"))  # works from any folder


@task(memory="512m")  # pandas alone takes about 96 MB, close to the 100 MB default
def prepare_data(market: str) -> dict:
    X, y = features(synthetic_history(get_market(market)))
    return {"market": market, "X": X.tolist(), "y": y.tolist()}  # plain lists travel between jobs


@task(vram="16g", memory="16g", cpu=4)  # a GPU with 16 GB, and the RAM and cores to feed it
def train_demand_model(data: dict) -> bytes:
    X, y = np.array(data["X"]), np.array(data["y"])
    model = train_torch_model(X, y)
    print(f"In-sample RMSE: {evaluate_torch_model(model, X, y):.2f} GW")
    return model


@task(memory="512m")  # pandas alone takes about 96 MB, close to the 100 MB default
def save_demand_model(model: bytes) -> str:
    save_model(MODEL_NAME, model)
    return f"{env_prefix()}models/{MODEL_NAME}"


@workflow(
    name="Merit Desk Demand Training",
    budget="Research",
    python_version="3.12",
    python_requirements=REQUIREMENTS,
)
def demand_training():
    data = prepare_data(MARKET)
    model = train_demand_model(data)
    save_demand_model(model)
