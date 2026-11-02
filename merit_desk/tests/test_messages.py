import pytest

from merit_desk.common.messages import clean_platform_text

DEFAULT = "The platform refused the request."


@pytest.mark.parametrize("text, expected", [
    ("Command failed: dt blob get file:///tmp/x.json\nNo such blob", "No such blob"),
    ("Command failed: dt job run file:///tmp/x.json", DEFAULT),
    ("Command timed out after 60s: dt job run file:///tmp/x.json",
     "The platform did not answer in time."),
    ("Cannot read file:///tmp/tmpab12cd.json now.", "Cannot read now."),
    ("Budget 'Trading desk' is used up.", "Budget 'Trading desk' is used up."),
    ("The command failed: no route", "The command failed: no route"),  # not the SDK's form
    ("", DEFAULT),
    (None, DEFAULT),
])
def test_only_the_words_of_the_platform_stay(text, expected):
    assert clean_platform_text(text, DEFAULT) == expected


def test_a_long_text_is_cut_to_300_characters_or_a_shorter_limit():
    assert clean_platform_text("x" * 300, DEFAULT) == "x" * 300
    cut = clean_platform_text("word " * 100, DEFAULT)
    assert len(cut) <= 300 and cut.endswith("...")
    assert len(clean_platform_text("x" * 300, DEFAULT, limit=50)) == 50
