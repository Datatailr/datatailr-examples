import numpy as np
import pandas as pd
import pytest
from sklearn.model_selection import ParameterGrid

from merit_desk.common.demand_model import (
    evaluate_torch_model,
    features,
    sklearn_search,
    train_torch_model,
)
from merit_desk.common.history import synthetic_history
from merit_desk.common.markets import get_market


def de_history(years: int) -> pd.DataFrame:
    return synthetic_history(get_market("DE"), years=years)


def test_feature_shape():
    X, y = features(de_history(1))
    assert X.shape[1] == 11 and len(X) == len(y)


def test_features_hold_temperature_weekday_and_month():
    history = pd.DataFrame({
        "day": pd.to_datetime(["2021-01-04", "2021-04-04"]),  # a Monday, then a Sunday in April
        "temp_c": [10.0, -2.0],
        "demand_gw": [55.0, 61.0],
    })
    X, y = features(history)
    monday = [10.0, 100.0, 1, 0, 0, 0, 0, 0, 0, 0.0, 1.0]  # sin and cos of January (angle 0)
    sunday = [-2.0, 4.0, 0, 0, 0, 0, 0, 0, 1, 1.0, 0.0]  # April is a quarter of the way round
    np.testing.assert_allclose(X, [monday, sunday], atol=1e-12)
    np.testing.assert_array_equal(y, [55.0, 61.0])


def test_torch_training_runs_on_cpu():
    pytest.importorskip("torch")
    X, y = features(de_history(1))
    model = train_torch_model(X, y, epochs=5)
    assert model and np.isfinite(evaluate_torch_model(model, X, y))


def test_torch_training_says_where_it_ran(capsys):
    torch = pytest.importorskip("torch")
    X, y = features(de_history(1))
    train_torch_model(X, y, epochs=1)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    assert capsys.readouterr().out == f"Training on {device}.\n"


def test_torch_training_copes_with_a_demand_that_never_changes():
    pytest.importorskip("torch")
    X, y = features(de_history(1))
    flat = np.full(len(y), 50.0)
    model = train_torch_model(X, flat, epochs=5)
    assert np.isfinite(evaluate_torch_model(model, X, flat))


def test_torch_training_is_deterministic_for_a_seed():
    pytest.importorskip("torch")
    X, y = features(de_history(1))
    runs = [train_torch_model(X, y, epochs=5, seed=s) for s in (1, 1, 2)]
    rmse = [evaluate_torch_model(model, X, y) for model in runs]
    assert rmse[0] == rmse[1] != rmse[2]


def test_torch_model_learns_demand_from_five_years():
    pytest.importorskip("torch")
    X, y = features(de_history(5))
    rmse = evaluate_torch_model(train_torch_model(X, y), X, y)
    assert rmse < 1.5 < np.std(y)  # the demand noise is 1 GW, so 1.5 is close to the best possible


def test_search_has_18_candidates():
    assert len(ParameterGrid(sklearn_search().param_grid)) == 18


def test_search_is_five_fold():
    search = sklearn_search()
    assert search.cv == 5  # 18 candidates x 5 folds = 90 fits
    assert search.param_grid == {"n_estimators": [100, 200, 400], "max_depth": [2, 3, 4],
                                 "learning_rate": [0.03, 0.1]}


def test_search_leaves_n_jobs_unset_so_the_notebook_sizes_the_pool():
    # n_jobs=-1 gives the Datatailr backend 4 workers, not the 16 of parallel_backend(n_jobs=16)
    assert sklearn_search().n_jobs is None
