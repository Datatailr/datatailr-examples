"""Demand models: features from the weather and the calendar, a neural net, a grid search."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from sklearn.model_selection import GridSearchCV

N_FEATURES = 11  # temperature, its square, 7 weekdays, and the month as a point on a circle
HIDDEN = 64
LEARNING_RATE = 0.01


def features(history: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """X and y for demand: X is the weather and the calendar, y is the demand in GW."""
    temp = history["temp_c"].to_numpy()
    weekday = np.eye(7)[history["day"].dt.day_of_week.to_numpy()]  # Monday first
    month = history["day"].dt.month.to_numpy() - 1
    angle = 2 * np.pi * month / 12  # on a circle, December sits next to January
    X = np.column_stack([temp, temp**2, weekday, np.sin(angle), np.cos(angle)])
    return X, history["demand_gw"].to_numpy()


def _network():
    from torch import nn

    return nn.Sequential(nn.Linear(N_FEATURES, HIDDEN), nn.ReLU(),
                         nn.Linear(HIDDEN, HIDDEN), nn.ReLU(), nn.Linear(HIDDEN, 1))


def train_torch_model(X: np.ndarray, y: np.ndarray, epochs: int = 200, seed: int = 7) -> bytes:
    """Train a small neural net, on the GPU when there is one. Returns it as bytes.

    The bytes hold the weights and the scaling that evaluate_torch_model needs. Every epoch is
    one step on the whole data set, so the same seed gives the same model on a CPU.
    """
    import torch

    torch.manual_seed(seed)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Training on {device}.")  # print, not logging: the root log level is WARNING
    x, t = torch.tensor(X, dtype=torch.float32), torch.tensor(y, dtype=torch.float32)
    # The net learns small, centred numbers.
    scaling = {"x_mean": x.mean(0), "x_std": x.std(0).clamp_min(1e-6),
               "y_mean": t.mean(), "y_std": t.std().clamp_min(1e-6)}
    x = ((x - scaling["x_mean"]) / scaling["x_std"]).to(device)
    t = ((t - scaling["y_mean"]) / scaling["y_std"]).unsqueeze(1).to(device)

    net = _network().to(device)
    optimizer = torch.optim.Adam(net.parameters(), lr=LEARNING_RATE)
    for _ in range(epochs):
        optimizer.zero_grad()
        torch.nn.functional.mse_loss(net(x), t).backward()
        optimizer.step()

    state = {name: weights.cpu() for name, weights in net.state_dict().items()}
    buffer = io.BytesIO()
    torch.save({"state_dict": state, "scaling": scaling}, buffer)
    return buffer.getvalue()


def evaluate_torch_model(model: bytes, X: np.ndarray, y: np.ndarray) -> float:
    """The root mean squared error of a trained model, in GW."""
    import torch

    saved = torch.load(io.BytesIO(model), weights_only=True)  # tensors only, no pickled code
    scaling = saved["scaling"]
    net = _network()
    net.load_state_dict(saved["state_dict"])
    with torch.no_grad():
        x = (torch.tensor(X, dtype=torch.float32) - scaling["x_mean"]) / scaling["x_std"]
        predicted = net(x).squeeze(1) * scaling["y_std"] + scaling["y_mean"]
    return float(np.sqrt(np.mean((predicted.numpy() - y) ** 2)))


def sklearn_search() -> GridSearchCV:
    """A grid search of 18 gradient boosting models, five folds each: 90 fits to spread out."""
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.model_selection import GridSearchCV

    grid = {"n_estimators": [100, 200, 400], "max_depth": [2, 3, 4], "learning_rate": [0.03, 0.1]}
    # n_jobs stays unset: the Datatailr backend sizes the pool, and a -1 would mean 4 workers.
    return GridSearchCV(GradientBoostingRegressor(random_state=7), grid, cv=5)
