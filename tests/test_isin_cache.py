"""Unit and integration tests for isin_cache.py.

Covers:
  - _fetch_companies_page: success, HTTP error, bad JSON shape
  - _fetch_all_companies: single-page and multi-page pagination
  - _fetch_equity_isin: ACCION match, no match, HTTP error
  - _load_cache: file missing, valid JSON, bad JSON
  - _save_cache: writes valid JSON, survives I/O error
  - build_isin_map: end-to-end with mocked HTTP
  - load_isin_map: cache hit, cache miss (downloads), failure fallback
"""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch, call

import pytest
import requests

from isin_cache import (
    _DEFAULT_CACHE_PATH,
    _fetch_all_companies,
    _fetch_companies_page,
    _fetch_equity_isin,
    _load_cache,
    _save_cache,
    build_isin_map,
    load_isin_map,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_response(payload: object, status: int = 200) -> MagicMock:
    """Build a mock requests.Response that returns *payload* as JSON."""
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    if status >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(
            response=resp, request=MagicMock()
        )
    return resp


def _make_session() -> MagicMock:
    return MagicMock(spec=requests.Session)


# ---------------------------------------------------------------------------
# _fetch_companies_page
# ---------------------------------------------------------------------------


class TestFetchCompaniesPage:
    def test_returns_content_list_on_success(self):
        session = _make_session()
        companies = [{"id": 1, "clave": "FEMSA", "nombre": "FEMSA Corp"}]
        session.get.return_value = _mock_response({"content": companies})

        result = _fetch_companies_page(session, page=0)

        assert result == companies

    def test_returns_empty_on_http_error(self):
        session = _make_session()
        session.get.return_value = _mock_response({}, status=500)

        result = _fetch_companies_page(session, page=0)

        assert result == []

    def test_returns_empty_on_request_exception(self):
        session = _make_session()
        session.get.side_effect = requests.ConnectionError("timeout")

        result = _fetch_companies_page(session, page=0)

        assert result == []

    def test_returns_empty_when_content_missing(self):
        session = _make_session()
        session.get.return_value = _mock_response({"other": "data"})

        result = _fetch_companies_page(session, page=0)

        assert result == []

    def test_returns_empty_when_response_is_list_not_dict(self):
        session = _make_session()
        session.get.return_value = _mock_response([{"id": 1}])

        result = _fetch_companies_page(session, page=0)

        assert result == []

    def test_returns_empty_when_content_is_not_list(self):
        session = _make_session()
        session.get.return_value = _mock_response({"content": "not-a-list"})

        result = _fetch_companies_page(session, page=0)

        assert result == []

    def test_passes_correct_page_param(self):
        session = _make_session()
        session.get.return_value = _mock_response({"content": []})

        _fetch_companies_page(session, page=3)

        call_kwargs = session.get.call_args
        assert call_kwargs[1]["params"]["page"] == 3


# ---------------------------------------------------------------------------
# _fetch_all_companies
# ---------------------------------------------------------------------------


class TestFetchAllCompanies:
    def test_single_page_under_page_size(self):
        session = _make_session()
        companies = [{"id": i, "clave": f"T{i}"} for i in range(5)]
        session.get.return_value = _mock_response({"content": companies})

        result = _fetch_all_companies(session)

        assert result == companies
        assert session.get.call_count == 1

    def test_paginates_when_full_page_returned(self):
        """If first page has exactly PAGE_SIZE=200 items, fetch a second page."""
        from isin_cache import _PAGE_SIZE

        page0 = [{"id": i, "clave": f"T{i}"} for i in range(_PAGE_SIZE)]
        page1 = [{"id": 999, "clave": "LAST"}]

        session = _make_session()
        session.get.side_effect = [
            _mock_response({"content": page0}),
            _mock_response({"content": page1}),
        ]

        result = _fetch_all_companies(session)

        assert len(result) == _PAGE_SIZE + 1
        assert session.get.call_count == 2

    def test_stops_on_empty_page(self):
        session = _make_session()
        session.get.return_value = _mock_response({"content": []})

        result = _fetch_all_companies(session)

        assert result == []
        assert session.get.call_count == 1

    def test_stops_on_fetch_failure(self):
        """A fetch error returns [] which breaks pagination."""
        session = _make_session()
        session.get.side_effect = requests.Timeout("timed out")

        result = _fetch_all_companies(session)

        assert result == []


# ---------------------------------------------------------------------------
# _fetch_equity_isin
# ---------------------------------------------------------------------------


class TestFetchEquityIsin:
    def test_returns_isin_for_accion_type(self):
        session = _make_session()
        emisiones = [
            {"isin": "MX01AC100006", "tipoValor": "ACCION", "serie": "*"},
        ]
        session.get.return_value = _mock_response({"content": emisiones})

        result = _fetch_equity_isin(session, biva_id=10, clave="AC")

        assert result == "MX01AC100006"

    def test_returns_none_when_no_accion_type(self):
        session = _make_session()
        emisiones = [
            {"isin": "MX0999ABC001", "tipoValor": "BONO", "serie": "A"},
        ]
        session.get.return_value = _mock_response({"content": emisiones})

        result = _fetch_equity_isin(session, biva_id=10, clave="AC")

        assert result is None

    def test_returns_none_on_empty_content(self):
        session = _make_session()
        session.get.return_value = _mock_response({"content": []})

        result = _fetch_equity_isin(session, biva_id=10, clave="AC")

        assert result is None

    def test_returns_none_on_http_error(self):
        session = _make_session()
        session.get.return_value = _mock_response({}, status=404)

        result = _fetch_equity_isin(session, biva_id=10, clave="AC")

        assert result is None

    def test_returns_none_on_connection_error(self):
        session = _make_session()
        session.get.side_effect = requests.ConnectionError("gone")

        result = _fetch_equity_isin(session, biva_id=10, clave="AC")

        assert result is None

    def test_accion_match_is_case_insensitive(self):
        """tipoValor 'accion' (lowercase) should still match."""
        session = _make_session()
        emisiones = [{"isin": "MX01AC100006", "tipoValor": "accion", "serie": "*"}]
        session.get.return_value = _mock_response({"content": emisiones})

        result = _fetch_equity_isin(session, biva_id=10, clave="AC")

        assert result == "MX01AC100006"

    def test_picks_first_accion_when_multiple(self):
        session = _make_session()
        emisiones = [
            {"isin": "MX01FIRST001", "tipoValor": "ACCION", "serie": "*"},
            {"isin": "MX01SECOND02", "tipoValor": "ACCION", "serie": "B"},
        ]
        session.get.return_value = _mock_response({"content": emisiones})

        result = _fetch_equity_isin(session, biva_id=10, clave="AC")

        assert result == "MX01FIRST001"

    def test_skips_empty_isin(self):
        """Rows with empty isin should not be returned."""
        session = _make_session()
        emisiones = [
            {"isin": "", "tipoValor": "ACCION", "serie": "*"},
            {"isin": "MX01VALID001", "tipoValor": "ACCION", "serie": "*"},
        ]
        session.get.return_value = _mock_response({"content": emisiones})

        result = _fetch_equity_isin(session, biva_id=10, clave="AC")

        assert result == "MX01VALID001"


# ---------------------------------------------------------------------------
# _load_cache / _save_cache
# ---------------------------------------------------------------------------


class TestLoadCache:
    def test_returns_none_when_file_missing(self, tmp_path):
        result = _load_cache(str(tmp_path / "no_such_file.json"))
        assert result is None

    def test_returns_dict_for_valid_cache_file(self, tmp_path):
        cache = {"FEMSA": "MX01FE100003", "AC": "MX01AC100006"}
        path = str(tmp_path / "cache.json")
        with open(path, "w") as fh:
            json.dump(cache, fh)

        result = _load_cache(path)

        assert result == cache

    def test_returns_none_for_invalid_json(self, tmp_path):
        path = str(tmp_path / "bad.json")
        with open(path, "w") as fh:
            fh.write("not-json{{{")

        result = _load_cache(path)

        assert result is None

    def test_returns_none_when_content_is_not_dict(self, tmp_path):
        path = str(tmp_path / "list.json")
        with open(path, "w") as fh:
            json.dump(["FEMSA", "AC"], fh)

        result = _load_cache(path)

        assert result is None


class TestSaveCache:
    def test_writes_valid_json_file(self, tmp_path):
        cache = {"FEMSA": "MX01FE100003"}
        path = str(tmp_path / "cache.json")

        _save_cache(cache, path)

        with open(path) as fh:
            loaded = json.load(fh)
        assert loaded == cache

    def test_survives_io_error_without_raising(self):
        """_save_cache must not raise even if the path is unwritable."""
        bad_path = "/proc/this/cannot/be/written"
        _save_cache({"A": "B"}, bad_path)  # should not raise

    def test_writes_empty_dict(self, tmp_path):
        path = str(tmp_path / "empty.json")
        _save_cache({}, path)

        with open(path) as fh:
            loaded = json.load(fh)
        assert loaded == {}


# ---------------------------------------------------------------------------
# build_isin_map
# ---------------------------------------------------------------------------


class TestBuildIsinMap:
    def test_returns_resolved_entries(self):
        session = _make_session()
        companies = [
            {"id": 1, "clave": "FEMSA", "nombre": "FEMSA Corp"},
            {"id": 2, "clave": "AC", "nombre": "AC Corp"},
            {"id": 3, "clave": "BIMBO", "nombre": "Bimbo"},
        ]
        # page 0 returns 3 companies (< PAGE_SIZE), no pagination needed
        emisiones_femsa = [{"isin": "MX01FE100003", "tipoValor": "ACCION", "serie": "*"}]
        emisiones_ac = [{"isin": "MX01AC100006", "tipoValor": "ACCION", "serie": "*"}]
        emisiones_bimbo = [{"isin": "", "tipoValor": "BONO", "serie": "A"}]

        responses = [
            _mock_response({"content": companies}),   # company list page 0
            _mock_response({"content": emisiones_femsa}),  # FEMSA
            _mock_response({"content": emisiones_ac}),     # AC
            _mock_response({"content": emisiones_bimbo}),  # BIMBO — no equity ISIN
        ]
        session.get.side_effect = responses

        with patch("isin_cache.time.sleep"):
            result = build_isin_map(session)

        assert result == {"FEMSA": "MX01FE100003", "AC": "MX01AC100006"}
        assert "BIMBO" not in result

    def test_returns_empty_when_company_list_empty(self):
        session = _make_session()
        session.get.return_value = _mock_response({"content": []})

        result = build_isin_map(session)

        assert result == {}

    def test_skips_company_with_missing_id(self):
        session = _make_session()
        companies = [
            {"clave": "NOID"},           # missing 'id' key
            {"id": 2, "clave": "AC"},
        ]
        emisiones_ac = [{"isin": "MX01AC100006", "tipoValor": "ACCION", "serie": "*"}]

        session.get.side_effect = [
            _mock_response({"content": companies}),
            _mock_response({"content": emisiones_ac}),
        ]

        with patch("isin_cache.time.sleep"):
            result = build_isin_map(session)

        assert "AC" in result
        assert "NOID" not in result

    def test_skips_company_with_empty_clave(self):
        session = _make_session()
        companies = [
            {"id": 1, "clave": ""},      # empty clave
            {"id": 2, "clave": "AC"},
        ]
        emisiones_ac = [{"isin": "MX01AC100006", "tipoValor": "ACCION", "serie": "*"}]

        session.get.side_effect = [
            _mock_response({"content": companies}),
            _mock_response({"content": emisiones_ac}),
        ]

        with patch("isin_cache.time.sleep"):
            result = build_isin_map(session)

        assert "AC" in result
        assert "" not in result

    def test_sleeps_between_requests(self):
        """Ensures rate-limit courtesy delay between per-company calls."""
        session = _make_session()
        companies = [
            {"id": 1, "clave": "A"},
            {"id": 2, "clave": "B"},
        ]
        session.get.side_effect = [
            _mock_response({"content": companies}),
            _mock_response({"content": []}),
            _mock_response({"content": []}),
        ]

        with patch("isin_cache.time.sleep") as mock_sleep:
            build_isin_map(session)

        # There are 2 companies; delay is between consecutive calls (n-1 = 1).
        assert mock_sleep.call_count == 1

    def test_no_sleep_for_single_company(self):
        session = _make_session()
        companies = [{"id": 1, "clave": "SOLO"}]
        session.get.side_effect = [
            _mock_response({"content": companies}),
            _mock_response({"content": []}),
        ]

        with patch("isin_cache.time.sleep") as mock_sleep:
            build_isin_map(session)

        assert mock_sleep.call_count == 0


# ---------------------------------------------------------------------------
# load_isin_map
# ---------------------------------------------------------------------------


class TestLoadIsinMap:
    def test_returns_cached_dict_on_cache_hit(self, tmp_path):
        cache = {"FEMSA": "MX01FE100003", "WALMEX": "MX01WA100004"}
        cache_path = str(tmp_path / "cache.json")
        with open(cache_path, "w") as fh:
            json.dump(cache, fh)

        session = _make_session()
        result = load_isin_map(session, cache_path=cache_path)

        assert result == cache
        # No HTTP call should have been made when using the cache
        session.get.assert_not_called()

    def test_downloads_when_no_cache_file(self, tmp_path):
        cache_path = str(tmp_path / "new_cache.json")
        isin_map = {"FEMSA": "MX01FE100003"}

        session = _make_session()
        with patch("isin_cache.build_isin_map", return_value=isin_map) as mock_build:
            result = load_isin_map(session, cache_path=cache_path)

        assert result == isin_map
        mock_build.assert_called_once_with(session)

    def test_persists_downloaded_map_to_cache(self, tmp_path):
        cache_path = str(tmp_path / "new_cache.json")
        isin_map = {"AC": "MX01AC100006"}

        session = _make_session()
        with patch("isin_cache.build_isin_map", return_value=isin_map):
            load_isin_map(session, cache_path=cache_path)

        assert os.path.exists(cache_path)
        with open(cache_path) as fh:
            saved = json.load(fh)
        assert saved == isin_map

    def test_returns_empty_on_build_failure(self, tmp_path):
        cache_path = str(tmp_path / "no_cache.json")
        session = _make_session()

        with patch("isin_cache.build_isin_map", side_effect=Exception("network down")):
            result = load_isin_map(session, cache_path=cache_path)

        assert result == {}

    def test_force_refresh_skips_existing_cache(self, tmp_path):
        cache = {"OLD": "MX00OLD00001"}
        cache_path = str(tmp_path / "cache.json")
        with open(cache_path, "w") as fh:
            json.dump(cache, fh)

        new_map = {"NEW": "MX00NEW00002"}
        session = _make_session()

        with patch("isin_cache.build_isin_map", return_value=new_map) as mock_build:
            result = load_isin_map(session, cache_path=cache_path, force_refresh=True)

        assert result == new_map
        mock_build.assert_called_once()

    def test_does_not_persist_empty_map(self, tmp_path):
        """When build_isin_map returns {}, do not write an empty cache file."""
        cache_path = str(tmp_path / "empty_cache.json")
        session = _make_session()

        with patch("isin_cache.build_isin_map", return_value={}):
            load_isin_map(session, cache_path=cache_path)

        # Cache file should NOT be created for an empty result
        assert not os.path.exists(cache_path)

    def test_uses_default_cache_path_constant(self):
        """Ensure the default cache path constant is what we advertise."""
        assert _DEFAULT_CACHE_PATH == "_biva_isin_cache.json"
