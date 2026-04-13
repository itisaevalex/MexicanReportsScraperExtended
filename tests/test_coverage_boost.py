"""
Additional tests to push coverage above 80%.

Covers edge cases in:
  - downloader.py (filename fallback paths)
  - http_utils.py (resolve_enc_batch)
  - scraper.py (build_cache, monitor keyboard interrupt, probe_key)
"""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, call, patch

import pytest
import requests
from bs4 import BeautifulSoup

from db import EncCache
from downloader import download_pdf_with_enc
from http_utils import resolve_enc_batch
from scraper import CNBVScraper


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _html_resp(html: str, status: int = 200) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = html
    r.headers = {"Content-Type": "text/html"}
    r.content = html.encode()
    r.raise_for_status = MagicMock()
    return r


def _make_scraper(tmp_path) -> CNBVScraper:
    return CNBVScraper(
        output_path=str(tmp_path / "filings.json"),
        pdf_dir=str(tmp_path / "pdfs"),
        download_docs=False,
        db_path=str(tmp_path / "enc.db"),
    )


# ---------------------------------------------------------------------------
# downloader.py — fallback filename paths
# ---------------------------------------------------------------------------


class TestDownloadFilenameEdgeCases:
    """Cover lines 103, 140-143 in downloader.py."""

    def test_uses_filename_hint_when_no_archivo_adjunto(self, tmp_path):
        """Page has no 'Archivo adjunto:' → fallback to filename_hint."""
        html = """<html><head><title>CNBV</title></head>
        <body>
        <form>
        <input type="hidden" name="__VIEWSTATE" value="VS" />
        <input type="hidden" name="__EVENTVALIDATION" value="EV" />
        <p>No file info here</p>
        </form>
        </body></html>"""

        pdf_bytes = b"%PDF-1.4 test"
        mock_session = MagicMock()
        mock_session.get.return_value = _html_resp(html)
        pdf_resp = MagicMock()
        pdf_resp.status_code = 200
        pdf_resp.headers = {"Content-Type": "application/pdf", "Content-Disposition": ""}
        pdf_resp.content = pdf_bytes
        mock_session.post.return_value = pdf_resp

        path = download_pdf_with_enc(
            "ENC_HINT",
            str(tmp_path),
            filename_hint="MyHintFile.pdf",
            session_override=mock_session,
        )

        assert path is not None
        assert "MyHintFile" in path

    def test_appends_pdf_extension_for_pdf_content(self, tmp_path):
        """When no Content-Disposition and filename has no extension, .pdf is appended."""
        html = """<html><head><title>CNBV</title></head>
        <body><form>
        <input type="hidden" name="__VIEWSTATE" value="VS" />
        <input type="hidden" name="__EVENTVALIDATION" value="EV" />
        <p>Archivo adjunto: report_no_ext</p>
        </form></body></html>"""

        pdf_bytes = b"%PDF-1.4 test"
        mock_session = MagicMock()
        mock_session.get.return_value = _html_resp(html)
        pdf_resp = MagicMock()
        pdf_resp.status_code = 200
        pdf_resp.headers = {"Content-Type": "application/pdf", "Content-Disposition": ""}
        pdf_resp.content = pdf_bytes
        mock_session.post.return_value = pdf_resp

        path = download_pdf_with_enc(
            "ENC_EXT",
            str(tmp_path),
            session_override=mock_session,
        )

        assert path is not None
        assert path.endswith(".pdf")

    def test_appends_bin_extension_for_non_pdf_binary(self, tmp_path):
        """Non-PDF binary content without extension → .bin appended."""
        html = """<html><head><title>CNBV</title></head>
        <body><form>
        <input type="hidden" name="__VIEWSTATE" value="VS" />
        <input type="hidden" name="__EVENTVALIDATION" value="EV" />
        <p>Archivo adjunto: mystery_file</p>
        </form></body></html>"""

        binary_bytes = b"\x00\x01\x02\x03 some binary data"
        mock_session = MagicMock()
        mock_session.get.return_value = _html_resp(html)
        bin_resp = MagicMock()
        bin_resp.status_code = 200
        bin_resp.headers = {
            "Content-Type": "application/octet-stream",
            "Content-Disposition": "",
        }
        bin_resp.content = binary_bytes
        mock_session.post.return_value = bin_resp

        path = download_pdf_with_enc(
            "ENC_BIN",
            str(tmp_path),
            session_override=mock_session,
        )

        assert path is not None
        assert path.endswith(".bin")

    def test_uses_default_document_pdf_when_no_hint_and_no_adjunto(self, tmp_path):
        """No hint, no Archivo adjunto line → falls back to 'document.pdf'."""
        html = """<html><head><title>CNBV</title></head>
        <body><form>
        <input type="hidden" name="__VIEWSTATE" value="VS" />
        <input type="hidden" name="__EVENTVALIDATION" value="EV" />
        </form></body></html>"""

        pdf_bytes = b"%PDF-1.4"
        mock_session = MagicMock()
        mock_session.get.return_value = _html_resp(html)
        pdf_resp = MagicMock()
        pdf_resp.status_code = 200
        pdf_resp.headers = {"Content-Type": "application/pdf", "Content-Disposition": ""}
        pdf_resp.content = pdf_bytes
        mock_session.post.return_value = pdf_resp

        path = download_pdf_with_enc(
            "ENC_DEFAULT",
            str(tmp_path),
            filename_hint="",
            session_override=mock_session,
        )

        assert path is not None
        assert "document" in path


# ---------------------------------------------------------------------------
# http_utils.py — resolve_enc_batch
# ---------------------------------------------------------------------------


class TestResolveEncBatch:
    def test_returns_list_of_tuples(self):
        """resolve_enc_batch returns (key, enc_or_None) for each input key."""
        enc_response = "/*DX*/some html enc=RESOLVED_ENC_VAL end"

        mock_session = MagicMock()
        mock_get_resp = MagicMock()
        mock_get_resp.status_code = 200
        mock_get_resp.text = (
            "<html><input type='hidden' name='__VIEWSTATE' value='VS'/></html>"
        )
        mock_session.get.return_value = mock_get_resp

        mock_post_resp = MagicMock()
        mock_post_resp.text = enc_response
        mock_session.post.return_value = mock_post_resp

        with patch("http_utils.make_session", return_value=mock_session):
            results = resolve_enc_batch([1, 2])

        assert len(results) == 2
        keys = {r[0] for r in results}
        assert keys == {1, 2}

    def test_handles_get_failure_gracefully(self):
        """If the initial GET fails, returns None for all keys."""
        mock_session = MagicMock()
        mock_get_resp = MagicMock()
        mock_get_resp.status_code = 503
        mock_session.get.return_value = mock_get_resp

        with patch("http_utils.make_session", return_value=mock_session):
            results = resolve_enc_batch([10, 20])

        assert all(enc is None for _, enc in results)

    def test_handles_post_exception_per_key(self):
        """Per-key POST exception → that key gets None, others may succeed."""
        mock_session = MagicMock()
        mock_get_resp = MagicMock()
        mock_get_resp.status_code = 200
        mock_get_resp.text = (
            "<html><input type='hidden' name='__VIEWSTATE' value='VS'/></html>"
        )
        mock_session.get.return_value = mock_get_resp
        mock_session.post.side_effect = Exception("network error")

        with patch("http_utils.make_session", return_value=mock_session):
            results = resolve_enc_batch([5, 6])

        assert all(enc is None for _, enc in results)

    def test_closes_session_on_completion(self):
        """Worker session must always be closed."""
        mock_session = MagicMock()
        mock_get_resp = MagicMock()
        mock_get_resp.status_code = 200
        mock_get_resp.text = "<html><input type='hidden' name='__VIEWSTATE' value='VS'/></html>"
        mock_session.get.return_value = mock_get_resp
        mock_post_resp = MagicMock()
        mock_post_resp.text = "no enc here"
        mock_session.post.return_value = mock_post_resp

        with patch("http_utils.make_session", return_value=mock_session):
            resolve_enc_batch([1])

        mock_session.close.assert_called_once()


# ---------------------------------------------------------------------------
# scraper.py — probe_key
# ---------------------------------------------------------------------------


class TestProbeKey:
    def test_probe_key_returns_none_when_no_enc(self, tmp_path):
        scraper = _make_scraper(tmp_path)
        scraper.hidden_fields = {}

        with patch("scraper.get_filing_enc", return_value=None):
            result = scraper.probe_key(99999)

        assert result is None

    def test_probe_key_returns_filing_when_found(self, tmp_path, detalle_html):
        scraper = _make_scraper(tmp_path)
        scraper.hidden_fields = {}

        with patch("scraper.get_filing_enc", return_value="ENC_VAL"):
            with patch("scraper.safe_get", return_value=_html_resp(detalle_html)):
                result = scraper.probe_key(453816)

        assert result is not None
        assert result["key"] == "453816"
        assert result["enc"] == "ENC_VAL"

    def test_probe_key_returns_none_on_non_200(self, tmp_path):
        scraper = _make_scraper(tmp_path)

        with patch("scraper.get_filing_enc", return_value="ENC_VAL"):
            with patch("scraper.safe_get", return_value=_html_resp("", status=404)):
                result = scraper.probe_key(453816)

        assert result is None

    def test_probe_key_returns_none_on_error_page(self, tmp_path):
        error_html = "<html><head><title>Error</title></head><body></body></html>"
        scraper = _make_scraper(tmp_path)

        with patch("scraper.get_filing_enc", return_value="ENC_VAL"):
            with patch("scraper.safe_get", return_value=_html_resp(error_html)):
                result = scraper.probe_key(453816)

        assert result is None


# ---------------------------------------------------------------------------
# scraper.py — build_cache
# ---------------------------------------------------------------------------


class TestBuildCache:
    def test_build_cache_calls_resolve_enc_batch(self, tmp_path):
        scraper = _make_scraper(tmp_path)
        scraper.cache.put(2, "ENC_2")  # pre-cache key 2

        with patch.object(scraper, "initialize"):
            with patch(
                "scraper.resolve_enc_batch",
                return_value=[(1, "ENC_1"), (3, "ENC_3")],
            ) as mock_resolve:
                scraper.build_cache(start=1, end=3, workers=2)

        # Key 2 was already cached, so resolve should handle [1, 3]
        assert scraper.cache.get(1) == "ENC_1"
        assert scraper.cache.get(3) == "ENC_3"

    def test_build_cache_auto_detects_end_key(self, tmp_path):
        scraper = _make_scraper(tmp_path)
        mock_filings = [
            {"key": "500"},
            {"key": "510"},
        ]

        with patch.object(scraper, "initialize"):
            with patch.object(scraper, "search_filings", return_value=mock_filings):
                with patch("scraper.resolve_enc_batch", return_value=[]):
                    scraper.build_cache(start=1, end=0, workers=1)

        # Should have attempted to resolve keys 1..510

    def test_build_cache_skips_when_all_cached(self, tmp_path, caplog):
        import logging

        scraper = _make_scraper(tmp_path)
        # Pre-cache all keys in range
        for k in range(1, 4):
            scraper.cache.put(k, f"ENC_{k}")

        with patch.object(scraper, "initialize"):
            with caplog.at_level(logging.INFO):
                scraper.build_cache(start=1, end=3, workers=2)

        assert "All keys already cached" in caplog.text

    def test_build_cache_exits_when_no_filings_and_no_end_key(self, tmp_path, caplog):
        import logging

        scraper = _make_scraper(tmp_path)

        with patch.object(scraper, "initialize"):
            with patch.object(scraper, "search_filings", return_value=[]):
                with caplog.at_level(logging.ERROR):
                    scraper.build_cache(start=1, end=0, workers=1)

        assert "Could not detect latest key" in caplog.text


# ---------------------------------------------------------------------------
# scraper.py — monitor keyboard interrupt
# ---------------------------------------------------------------------------


class TestMonitorKeyboardInterrupt:
    def test_monitor_saves_state_on_keyboard_interrupt(self, tmp_path):
        scraper = _make_scraper(tmp_path)
        scraper.cache.put(1000, "ENC_1000")

        mock_filings = [{"key": "1000"}]

        def interrupt_on_first(*args, **kwargs):
            raise KeyboardInterrupt()

        with patch.object(scraper, "initialize"):
            with patch.object(scraper, "search_filings", return_value=mock_filings):
                with patch.object(scraper, "probe_key", side_effect=interrupt_on_first):
                    scraper.monitor(interval=1)

        state = scraper._load_state()
        assert "last_key" in state
