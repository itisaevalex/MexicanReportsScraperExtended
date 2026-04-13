"""
Unit tests for http_utils.py

Tests cover:
  - to_dx_epoch_ms
  - make_session
  - safe_get / safe_post (mock HTTP)
  - build_gv_callback_param
  - get_filing_enc (cache-first + server fallback)
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import requests

from db import EncCache
from http_utils import (
    build_gv_callback_param,
    get_filing_enc,
    make_session,
    safe_get,
    safe_post,
    to_dx_epoch_ms,
)


# ---------------------------------------------------------------------------
# to_dx_epoch_ms
# ---------------------------------------------------------------------------


class TestToDxEpochMs:
    def test_unix_epoch_is_zero(self):
        epoch = datetime(1970, 1, 1)
        assert to_dx_epoch_ms(epoch) == "0"

    def test_known_date(self):
        # 2026-01-01 00:00:00 = 1767225600 seconds from epoch
        dt = datetime(2026, 1, 1)
        ms = to_dx_epoch_ms(dt)
        assert ms == str(int(1767225600 * 1000))

    def test_returns_string(self):
        assert isinstance(to_dx_epoch_ms(datetime.now()), str)

    def test_is_integer_string(self):
        result = to_dx_epoch_ms(datetime(2025, 6, 15))
        assert result.isdigit()


# ---------------------------------------------------------------------------
# make_session
# ---------------------------------------------------------------------------


class TestMakeSession:
    def test_returns_requests_session(self):
        s = make_session()
        assert isinstance(s, requests.Session)

    def test_ssl_verification_disabled(self):
        s = make_session()
        assert s.verify is False

    def test_each_call_returns_new_instance(self):
        s1 = make_session()
        s2 = make_session()
        assert s1 is not s2


# ---------------------------------------------------------------------------
# safe_get
# ---------------------------------------------------------------------------


class TestSafeGet:
    def test_returns_response_on_success(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        result = safe_get(mock_session, "http://example.com")
        assert result is mock_response
        mock_session.get.assert_called_once()

    def test_passes_merged_headers(self):
        mock_response = MagicMock()
        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        safe_get(mock_session, "http://x.com", headers={"X-Custom": "yes"})
        call_kwargs = mock_session.get.call_args[1]
        assert "X-Custom" in call_kwargs["headers"]
        assert "User-Agent" in call_kwargs["headers"]  # from BROWSER_HEADERS

    def test_retries_on_connection_error(self):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.get.side_effect = [
            requests.ConnectionError("fail"),
            mock_response,
        ]

        result = safe_get(mock_session, "http://x.com", retries=1)
        assert result is mock_response
        assert mock_session.get.call_count == 2

    def test_raises_after_exhausted_retries(self):
        mock_session = MagicMock()
        mock_session.get.side_effect = requests.ConnectionError("always fails")

        with pytest.raises(requests.ConnectionError):
            safe_get(mock_session, "http://x.com", retries=1)


# ---------------------------------------------------------------------------
# safe_post
# ---------------------------------------------------------------------------


class TestSafePost:
    def test_returns_response_on_success(self):
        mock_response = MagicMock()
        mock_session = MagicMock()
        mock_session.post.return_value = mock_response

        result = safe_post(mock_session, "http://x.com", {"key": "val"})
        assert result is mock_response

    def test_passes_form_data(self):
        mock_response = MagicMock()
        mock_session = MagicMock()
        mock_session.post.return_value = mock_response

        safe_post(mock_session, "http://x.com", {"field": "value"})
        call_kwargs = mock_session.post.call_args[1]
        assert call_kwargs["data"] == {"field": "value"}

    def test_retries_on_connection_error(self):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_session.post.side_effect = [
            requests.ConnectionError("fail"),
            mock_response,
        ]

        result = safe_post(mock_session, "http://x.com", {}, retries=1)
        assert result is mock_response
        assert mock_session.post.call_count == 2


# ---------------------------------------------------------------------------
# build_gv_callback_param
# ---------------------------------------------------------------------------


class TestBuildGvCallbackParam:
    def test_starts_with_c0_prefix(self):
        param = build_gv_callback_param(["100", "101", "102"], page_index=1)
        assert param.startswith("c0:")

    def test_contains_page_index(self):
        param = build_gv_callback_param(["100"], page_index=3)
        assert "PN3" in param

    def test_contains_kv_block(self):
        param = build_gv_callback_param(["100", "200"], page_index=1)
        assert "KV|" in param
        assert "'100'" in param
        assert "'200'" in param

    def test_contains_gb_block(self):
        param = build_gv_callback_param(["100"], page_index=1, page_size=20)
        assert "GB|20;" in param

    def test_pageronclick_format(self):
        param = build_gv_callback_param(["100"], page_index=2)
        assert "PAGERONCLICK3" in param
        assert "12|PAGERONCLICK3|PN2;" in param

    def test_empty_keys_list(self):
        param = build_gv_callback_param([], page_index=1)
        assert param.startswith("c0:")
        assert "KV|2;[];" in param

    def test_known_format(self):
        """Regression: exact format verified from Playwright capture."""
        keys = ["453816", "453815"]
        param = build_gv_callback_param(keys, page_index=1)
        kv_array = "['453816','453815']"
        assert f"KV|{len(kv_array)};{kv_array};" in param


# ---------------------------------------------------------------------------
# get_filing_enc
# ---------------------------------------------------------------------------


class TestGetFilingEnc:
    def test_cache_hit_skips_server_call(self, tmp_db: EncCache):
        tmp_db.put(453816, "CACHED_ENC_VALUE")
        mock_session = MagicMock()

        result = get_filing_enc(mock_session, {}, "453816", tmp_db)

        assert result == "CACHED_ENC_VALUE"
        mock_session.post.assert_not_called()

    def test_cache_miss_calls_server(self, tmp_db: EncCache, dx_cb_enc: str):
        mock_response = MagicMock()
        mock_response.text = dx_cb_enc
        mock_session = MagicMock()
        mock_session.post.return_value = mock_response

        result = get_filing_enc(mock_session, {"__VIEWSTATE": "VS"}, "453816", tmp_db)

        mock_session.post.assert_called_once()
        assert result is not None
        # Verify the result was cached
        assert tmp_db.get(453816) == result

    def test_server_call_uses_c0_prefix(self, tmp_db: EncCache, dx_cb_enc: str):
        """Critical: __CALLBACKPARAM must be 'c0:<key>'."""
        mock_response = MagicMock()
        mock_response.text = dx_cb_enc
        mock_session = MagicMock()
        mock_session.post.return_value = mock_response

        get_filing_enc(mock_session, {}, "453816", tmp_db)

        call_kwargs = mock_session.post.call_args[1]
        posted_data = call_kwargs["data"]
        assert posted_data["__CALLBACKPARAM"] == "c0:453816"

    def test_returns_none_when_server_returns_no_enc(self, tmp_db: EncCache, dx_cb_no_enc: str):
        mock_response = MagicMock()
        mock_response.text = dx_cb_no_enc
        mock_session = MagicMock()
        mock_session.post.return_value = mock_response

        result = get_filing_enc(mock_session, {}, "99999", tmp_db)

        assert result is None
        # Nothing should be cached for a miss
        assert tmp_db.get(99999) is None
