import pytest

from merit_desk.admin import setup_demo

ARGS = ["--research", "500", "--trading-desk", "300"]


def test_setup_creates_the_groups_budgets_and_variables(cluster, capsys):
    setup_demo.main(ARGS)
    assert cluster.groups == {"research", "trading_desk", "platform"}
    assert cluster.budgets == {"Research": (500.0, True), "Trading": (300.0, False)}
    assert cluster.variables == {"markets": "GB,DE,FR", "sweep_size": "small"}
    out = capsys.readouterr().out
    assert "desk_webhook_url" in out and "Secret Keys Manager" in out
    assert "current environment only" in out
    # A promotion copies the tasks as built in dev, so pre and prod variables do not steer it.
    assert "promoted from dev keeps the values that its dev deploy read" in out


def test_setup_can_run_again(cluster, capsys):
    setup_demo.main(ARGS)
    capsys.readouterr()
    setup_demo.main(["--research", "999", "--trading-desk", "999"])
    assert cluster.group_adds == 3
    assert cluster.budgets == {"Research": (500.0, True), "Trading": (300.0, False)}
    out = capsys.readouterr().out
    assert out.count("already there") == 5  # three groups and two budgets
    assert "created" not in out


def test_setup_keeps_a_variable_that_has_a_value(cluster, capsys):
    cluster.variables.update({"markets": "GB,DE", "sweep_size": "full"})
    setup_demo.main(ARGS)
    assert cluster.variables == {"markets": "GB,DE", "sweep_size": "full"}
    out = capsys.readouterr().out
    assert "kept markets=GB,DE (already set)" in out
    assert "kept sweep_size=full (already set)" in out
    assert "set markets=" not in out


def test_setup_sets_a_variable_that_is_empty_or_missing(cluster, capsys):
    cluster.variables["markets"] = ""  # empty, and sweep_size is missing: its get raises
    setup_demo.main(ARGS)
    assert cluster.variables == {"markets": "GB,DE,FR", "sweep_size": "small"}
    out = capsys.readouterr().out
    assert "set markets=GB,DE,FR" in out and "set sweep_size=small" in out
    assert "kept" not in out


@pytest.mark.parametrize("amount", ["0", "-5", "nan", "inf", "abc"])
def test_setup_rejects_a_budget_amount_that_is_not_a_number_above_zero(cluster, capsys, amount):
    with pytest.raises(SystemExit) as stop:
        setup_demo.main(["--research", amount, "--trading-desk", "300"])
    assert stop.value.code == 2  # argparse error
    assert "--research" in capsys.readouterr().err
    assert not cluster.groups and not cluster.budgets and not cluster.variables
