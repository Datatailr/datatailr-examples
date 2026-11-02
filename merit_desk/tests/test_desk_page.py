import html
import re
import warnings
from html.parser import HTMLParser
from pathlib import Path

import pytest

with warnings.catch_warnings():
    # starlette 1.10 prefers httpx2 and warns about httpx: switch when it drops the httpx fallback
    warnings.filterwarnings("ignore", message=r"Using `httpx` with `starlette\.testclient`")
    from fastapi.testclient import TestClient

from merit_desk.common.weather import CREDIT
from merit_desk.desk_app.app import create_app

STATIC = Path(__file__).parents[1] / "desk_app" / "static"
MENU = ["Curves", "Scenario", "Merit order", "Backtest", "Jobs"]
ECMWF_TERMS = "https://apps.ecmwf.int/datasets/licences/general/"
API_PATH = re.compile(r"""["'`](/api/[^"'`?\s]*)""")  # a quoted path, without its query


class Tags(HTMLParser):
    """The tags of a page with their attributes, and the text of the links in its menu."""

    def __init__(self, text: str):
        super().__init__()
        self.tags, self.menu, self._in_nav, self._link = [], [], False, None
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))
        self._in_nav = self._in_nav or tag == "nav"
        if self._in_nav and tag == "a":
            self._link = ""

    def handle_endtag(self, tag):
        if tag == "nav":
            self._in_nav = False
        if tag == "a" and self._link is not None:
            self.menu.append(self._link.strip())
            self._link = None

    def handle_data(self, data):
        if self._link is not None:
            self._link += data


@pytest.fixture(scope="module")
def client():
    """The page and its files need no started app: nothing real is built for them."""
    return TestClient(create_app())


@pytest.fixture(scope="module")
def page(client) -> str:
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    return response.text


def loaded_files(page: str) -> list[str]:
    """What the page loads: the scripts and the linked files, such as the stylesheet."""
    tags = Tags(page).tags
    return ([a["src"] for t, a in tags if t == "script" and "src" in a]
            + [a["href"] for t, a in tags if t == "link" and "href" in a])


def page_scripts() -> dict[str, str]:
    """Our own scripts, wherever they are under static/, but not the bundled Chart.js."""
    paths = [p for p in STATIC.rglob("*.js") if "vendor" not in p.parts]
    return {p.name: p.read_text(encoding="utf-8") for p in paths}


def script_api_paths() -> set[str]:
    """Each /api/ path the scripts call. A template part, such as ${id}, is one path parameter."""
    return {re.sub(r"\$\{[^}]*\}", "x", path)
            for text in page_scripts().values() for path in API_PATH.findall(text)}


def api_routes(app) -> list:
    return [r for r in app.routes if getattr(r, "path", "").startswith("/api/")]


def test_the_page_has_its_title_the_menu_the_price_label_and_the_forecast_credit(page):
    assert re.search(r"<title>\s*Merit Desk\s*</title>", page)
    assert Tags(page).menu == MENU
    assert "Simulated prices" in page
    assert html.escape(CREDIT) in page
    assert "© European Centre for Medium-Range Weather Forecasts (ECMWF)." in page
    assert f'href="{ECMWF_TERMS}"' in page
    assert "{{" not in page  # every placeholder is filled


def test_every_file_the_page_loads_is_served(client, page):
    files = loaded_files(page)
    assert "static/vendor/chart.umd.js" in files and len(files) >= 4
    for name in files:
        response = client.get(f"/{name}")
        assert response.status_code == 200, name
        assert response.content, name


def test_the_page_loads_nothing_from_another_site(page):
    for name in loaded_files(page):
        assert not re.match(r"(https?:)?//", name), name
    own = {"app.css": (STATIC / "app.css").read_text(encoding="utf-8"), **page_scripts()}
    for name, text in own.items():
        assert "http://" not in text and "https://" not in text, name


@pytest.mark.parametrize("root", ["", "/job/dev/merit-desk"])
def test_the_page_names_the_root_path_of_the_app_as_its_base(root):
    """The platform serves the app under /job/<env>/<name>, with or without a last slash. The
    page's links start from its <base>, so they work either way."""
    client = TestClient(create_app(), root_path=root)
    for address in (root or "/", f"{root}/"):
        page = client.get(address).text
        assert [a["href"] for t, a in Tags(page).tags if t == "base"] == [f"{root}/"]
    for name in loaded_files(page):
        assert client.get(f"{root}/{name}").status_code == 200, name


def test_the_root_path_in_the_base_is_escaped():
    page = TestClient(create_app(), root_path='/job/dev/a"b<c').get("/").text
    assert '<base href="/job/dev/a&quot;b&lt;c/">' in page


def test_chart_js_is_the_bundled_4_4_7_with_its_licence():
    assert (STATIC / "vendor" / "chart.umd.js").read_text(encoding="utf-8").startswith(
        "/*!\n * Chart.js v4.4.7\n")
    assert "MIT License" in (STATIC / "vendor" / "LICENSE-chartjs.md").read_text(encoding="utf-8")


def test_every_api_path_in_the_scripts_is_a_route_of_the_app():
    routes, paths = api_routes(create_app()), script_api_paths()
    assert paths
    for path in paths:
        assert any(route.path_regex.match(path) for route in routes), path


def test_the_page_uses_every_api_route_of_the_app():
    paths = script_api_paths()
    for route in api_routes(create_app()):
        assert any(route.path_regex.match(path) for path in paths), route.path


def created_inputs() -> list[str]:
    """The properties of each input that the scripts create, such as el('input', {...})."""
    return [props for text in page_scripts().values()
            for props in re.findall(r"el\('input', \{(.*?)\}\)", text, re.S)]


def test_every_input_the_scripts_create_has_an_accessible_name():
    """The market check boxes and the plant fields are made in script. A reader that does not
    follow a wrapping label names a check box by its value, "on", so each input says its own."""
    inputs = created_inputs()
    assert len(inputs) >= 2  # the market check box and the plant field
    for props in inputs:
        assert "'aria-label'" in props, props


def test_a_check_box_is_named_by_its_market_and_a_plant_field_by_its_plant_and_unit():
    pages = page_scripts()["pages.js"]
    assert re.search(r"'aria-label': `[^`]*\$\{code\}[^`]*`", pages)
    assert re.search(r"'aria-label': `[^`]*\$\{plant\.name\}[^`]*`", pages)
    assert "capacity in GW" in pages and "cost in EUR/MWh" in pages
