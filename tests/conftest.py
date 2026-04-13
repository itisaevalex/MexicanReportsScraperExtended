"""
Pytest configuration and shared fixtures for the CNBV scraper test suite.

All fixtures are scope-appropriate:
  - ``fixture_dir``      — module-scoped path to fixtures/
  - ``tmp_db``           — function-scoped in-memory EncCache
  - ``initial_page_html``— module-scoped HTML for the landing page
  - ``grid_html``        — module-scoped HTML for a grid with filings
  - ``dx_cb_enc``        — DX callback response containing an enc value
  - ``dx_cb_no_enc``     — DX callback response with the c0: error
  - ``dx_grid_response`` — DX GridView pagination callback response
  - ``asp_delta``        — ASP.NET UpdatePanel delta response
  - ``detalle_html``     — Detalle.aspx page HTML
"""

from __future__ import annotations

import os
import sys

import pytest

# Make the task1 package importable from within the tests sub-package.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from db import EncCache  # noqa: E402


FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def fixture_dir() -> str:
    """Return the absolute path to the fixtures directory."""
    return FIXTURE_DIR


# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_db(tmp_path) -> EncCache:
    """Return a fresh EncCache backed by a temporary SQLite file."""
    cache = EncCache(db_path=str(tmp_path / "test_enc_cache.db"))
    yield cache
    cache.close()


# ---------------------------------------------------------------------------
# HTML / response body fixtures
# ---------------------------------------------------------------------------


def _read_fixture(name: str) -> str:
    path = os.path.join(FIXTURE_DIR, name)
    with open(path, encoding="utf-8") as fh:
        return fh.read()


@pytest.fixture(scope="module")
def initial_page_html() -> str:
    return _read_fixture("initial_page.html")


@pytest.fixture(scope="module")
def grid_html() -> str:
    return _read_fixture("grid_with_filings.html")


@pytest.fixture(scope="module")
def dx_cb_enc() -> str:
    return _read_fixture("dx_callback_with_enc.txt")


@pytest.fixture(scope="module")
def dx_cb_no_enc() -> str:
    return _read_fixture("dx_callback_no_enc.txt")


@pytest.fixture(scope="module")
def dx_grid_response() -> str:
    return _read_fixture("dx_grid_callback_response.txt")


@pytest.fixture(scope="module")
def asp_delta() -> str:
    return _read_fixture("asp_delta_response.txt")


@pytest.fixture(scope="module")
def detalle_html() -> str:
    return _read_fixture("detalle_page.html")
