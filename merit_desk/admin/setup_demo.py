"""Set up a Datatailr cluster for the Merit Desk demo. An administrator runs it once.

It creates the groups and the budgets that the jobs need, and it sets the two variables that
the workflows read. You can run it again. A group or a budget that exists stays as it is,
and so does a variable that already has a value.

Run it from the repository root:

    python merit_desk/admin/setup_demo.py --research 500 --trading-desk 300

The amounts are in USD.
"""

import argparse
import math

from datatailr import KV, Budget, Group

GROUPS = ["research", "trading_desk", "platform"]  # group names cannot contain spaces
VARIABLES = {"markets": "GB,DE,FR", "sweep_size": "small"}
SECRET = "desk_webhook_url"


def ensure_group(name: str) -> None:
    if Group.exists(name):
        print(f"Group {name}: already there")
    elif Group.add(name) is None:
        raise SystemExit(f"Group {name}: could not be created")
    else:
        print(f"Group {name}: created")


def budget_exists(name: str) -> bool:
    try:
        Budget.get(name)  # it raises when the budget does not exist, and there is no Budget.exists
    except Exception:  # any lookup error counts as missing, and Budget.add then reports a real one
        return False
    return True


def ensure_budget(name: str, amount: float, prevent_overflow: bool = False) -> None:
    if budget_exists(name):
        print(f"Budget {name}: already there (its amount is unchanged)")
        return
    Budget.add(name, amount, prevent_overflow=prevent_overflow)
    flag = ", prevent_overflow on" if prevent_overflow else ""
    print(f"Budget {name}: created with {amount:g}{flag}")


def ensure_variable(key: str, default: str) -> None:
    try:
        value = KV().get(key)
    except Exception:  # the platform can raise for a variable that does not exist
        value = None
    if value in (None, ""):
        KV().put(key, default)
        print(f"set {key}={default}")
    else:
        print(f"kept {key}={value} (already set)")


def usd_amount(text: str) -> float:
    """A budget amount: a finite number above 0."""
    amount = float(text)  # argparse reports a ValueError as an invalid value
    if not math.isfinite(amount) or amount <= 0:
        raise argparse.ArgumentTypeError(f"'{text}' is not a number above 0")
    return amount


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--research", type=usd_amount, required=True,
                        help="amount of the budget 'Research', in USD")
    parser.add_argument("--trading-desk", type=usd_amount, required=True,
                        help="amount of the budget 'Trading', in USD")
    args = parser.parse_args(argv)

    for name in GROUPS:
        ensure_group(name)
    ensure_budget("Research", args.research, prevent_overflow=True)
    ensure_budget("Trading", args.trading_desk)
    for key, default in VARIABLES.items():
        ensure_variable(key, default)
    print("It wrote the variables of the current environment only. Pre and prod have their own:")
    print("set them the same way, from that environment.")
    print(f"Next: create the secret {SECRET} in the Secret Keys Manager, in each environment.")
    print("The SDK cannot create secrets. Without it, the daily curve skips the chat message.")


if __name__ == "__main__":
    main()
