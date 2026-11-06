from datetime import date

import pytest

from merit_desk.common.climate import normal_temp
from merit_desk.common.markets import MARKET_BY_CODE, MARKETS, UnknownMarketError, get_market


def test_three_markets_in_order():
    assert [m.code for m in MARKETS] == ["GB", "DE", "FR"]


def test_get_market_is_case_insensitive():
    assert get_market(" de ").code == "DE"


def test_get_market_unknown_code_message():
    with pytest.raises(UnknownMarketError, match=r"Unknown market 'NL'\. Use one of: GB, DE, FR\."):
        get_market("NL")


def test_normal_temp_coldest_and_warmest():
    gb = MARKET_BY_CODE["GB"]
    assert normal_temp(gb, date(2027, 1, 20)) == pytest.approx(4.0)
    assert normal_temp(gb, date(2027, 7, 21)) == pytest.approx(17.0, abs=0.05)
