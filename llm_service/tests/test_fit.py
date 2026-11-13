import pytest

from llm_service.gateway.fit import WEIGHTS_MARGIN, fit, needed_from, parse_memory_mib

MIB = 2**20


@pytest.mark.parametrize(
    "value,expected",
    [
        ("16g", 16384),
        ("16G", 16384),
        ("16gi", 16384),
        ("16Gi", 16384),
        ("15360m", 15360),
        ("15360Mi", 15360),
        ("1t", 1024 * 1024),
        ("16384", 16384),
        (" 16g ", 16384),
        ("1.5g", 1536),
    ],
)
def test_parse_memory_mib_forms(value, expected):
    assert parse_memory_mib(value) == expected


@pytest.mark.parametrize("value", ["", None, "lots", "0g", "-1g", "0", "g", "1x", "nang", "infg", "1e400g"])
def test_parse_memory_mib_bad_values_give_none(value):
    assert parse_memory_mib(value) is None


def test_weights_margin_is_one_point_two():
    assert WEIGHTS_MARGIN == 1.2


def test_needed_from_catalog_wins_over_the_size():
    entry = {"model": "org/a", "revision": "main", "note": "", "gpu_memory_mib": 6000}
    assert needed_from(entry, 10 * MIB) == (6000, "catalog")


def test_needed_from_size_with_margin_rounded_up():
    assert needed_from(None, 10 * MIB) == (12, "weights")
    assert needed_from({"gpu_memory_mib": None}, 10 * MIB + 1) == (13, "weights")
    assert needed_from(None, 1) == (1, "weights")


def test_needed_from_unknown():
    assert needed_from(None, None) == (None, "unknown")
    assert needed_from({"gpu_memory_mib": None}, None) == (None, "unknown")


def test_fit_yes_no_and_equal():
    assert fit(100, "catalog", 200, "nvidia-smi") == {
        "fits": "yes",
        "needed_mib": 100,
        "available_mib": 200,
        "basis": "catalog",
        "available_source": "nvidia-smi",
    }
    assert fit(300, "weights", 200, "container")["fits"] == "no"
    assert fit(200, "catalog", 200, "container")["fits"] == "yes"


def test_fit_unknown_when_a_number_is_missing():
    assert fit(None, "unknown", 200, "container")["fits"] == "unknown"
    assert fit(100, "catalog", None, None)["fits"] == "unknown"
    assert fit(None, "unknown", None, None) == {
        "fits": "unknown",
        "needed_mib": None,
        "available_mib": None,
        "basis": "unknown",
        "available_source": None,
    }
