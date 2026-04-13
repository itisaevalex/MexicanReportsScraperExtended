"""
Unit tests for downloader.py

Tests cover:
  - download_pdf_with_enc (mock HTTP)
  - attempt_pdf_download
  - download_batch_parallel (mock HTTP)
  - _download_worker

All HTTP calls are mocked — no real network requests are made.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, call, patch

import pytest
import requests

from db import EncCache
from downloader import (
    _download_worker,
    attempt_pdf_download,
    download_batch_parallel,
    download_pdf_with_enc,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_html_response(html: str, status: int = 200) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = html
    r.headers = {"Content-Type": "text/html"}
    r.content = html.encode()
    return r


def _make_pdf_response(content: bytes = b"%PDF-1.4 fake") -> MagicMock:
    r = MagicMock()
    r.status_code = 200
    r.text = ""
    r.headers = {
        "Content-Type": "application/pdf",
        "Content-Disposition": 'attachment; filename="document.pdf"',
    }
    r.content = content
    return r


# ---------------------------------------------------------------------------
# download_pdf_with_enc
# ---------------------------------------------------------------------------


class TestDownloadPdfWithEnc:
    def test_successful_download(self, tmp_path, detalle_html: str):
        pdf_bytes = b"%PDF-1.4 test document"
        mock_session = MagicMock()
        mock_session.get.return_value = _make_html_response(detalle_html)
        mock_session.post.return_value = _make_pdf_response(pdf_bytes)

        with patch("downloader.make_session", return_value=mock_session):
            path = download_pdf_with_enc(
                "ABC123",
                str(tmp_path),
                session_override=mock_session,
            )

        assert path is not None
        assert os.path.exists(path)
        with open(path, "rb") as fh:
            assert fh.read() == pdf_bytes

    def test_returns_none_on_non_200_get(self, tmp_path):
        mock_session = MagicMock()
        mock_session.get.return_value = _make_html_response("", status=404)

        path = download_pdf_with_enc(
            "ENC_BAD",
            str(tmp_path),
            session_override=mock_session,
        )

        assert path is None

    def test_returns_none_when_response_is_html_not_file(self, tmp_path, detalle_html: str):
        mock_session = MagicMock()
        mock_session.get.return_value = _make_html_response(detalle_html)
        # Server returns HTML instead of a file
        html_resp = _make_html_response("<html>error</html>")
        html_resp.headers = {"Content-Type": "text/html"}
        html_resp.content = b"<html>error</html>"
        mock_session.post.return_value = html_resp

        path = download_pdf_with_enc(
            "ENC_FAIL",
            str(tmp_path),
            session_override=mock_session,
        )

        assert path is None

    def test_returns_none_on_error_page(self, tmp_path):
        error_html = "<html><head><title>Error - página</title></head><body>Error</body></html>"
        mock_session = MagicMock()
        mock_session.get.return_value = _make_html_response(error_html)

        path = download_pdf_with_enc(
            "ENC_ERR",
            str(tmp_path),
            session_override=mock_session,
        )

        assert path is None

    def test_uses_content_disposition_filename(self, tmp_path, detalle_html: str):
        pdf_bytes = b"%PDF-1.4 test"
        mock_session = MagicMock()
        mock_session.get.return_value = _make_html_response(detalle_html)
        pdf_resp = _make_pdf_response(pdf_bytes)
        pdf_resp.headers["Content-Disposition"] = 'attachment; filename="MyReport_2025.pdf"'
        mock_session.post.return_value = pdf_resp

        path = download_pdf_with_enc(
            "ENC_CD",
            str(tmp_path),
            session_override=mock_session,
        )

        assert path is not None
        assert "MyReport_2025" in path

    def test_creates_pdf_dir_if_missing(self, tmp_path, detalle_html: str):
        new_dir = str(tmp_path / "deep" / "nested" / "dir")
        mock_session = MagicMock()
        mock_session.get.return_value = _make_html_response(detalle_html)
        mock_session.post.return_value = _make_pdf_response()

        path = download_pdf_with_enc(
            "ENC_DIR",
            new_dir,
            session_override=mock_session,
        )

        assert os.path.isdir(new_dir)

    def test_posts_correct_eventtarget(self, tmp_path, detalle_html: str):
        mock_session = MagicMock()
        mock_session.get.return_value = _make_html_response(detalle_html)
        mock_session.post.return_value = _make_pdf_response()

        download_pdf_with_enc(
            "ENC_ET",
            str(tmp_path),
            session_override=mock_session,
        )

        post_call_kwargs = mock_session.post.call_args[1]
        posted_data = post_call_kwargs["data"]
        assert posted_data["__EVENTTARGET"] == "DataViewContenido$DescargaArchivo"


# ---------------------------------------------------------------------------
# attempt_pdf_download
# ---------------------------------------------------------------------------


class TestAttemptPdfDownload:
    def test_returns_none_when_no_key(self, tmp_db: EncCache, tmp_path):
        filing = {"emisora": "FEMSA", "asunto": "Annual", "key": None}
        mock_session = MagicMock()
        result = attempt_pdf_download(
            filing, mock_session, {}, tmp_db, str(tmp_path)
        )
        assert result is None

    def test_calls_get_filing_enc_and_downloads(
        self, tmp_db: EncCache, tmp_path, detalle_html: str
    ):
        filing = {"emisora": "BIMBO", "asunto": "Report", "key": "453815"}
        tmp_db.put(453815, "CACHED_ENC")

        mock_session = MagicMock()
        mock_session.post.return_value = _make_pdf_response()
        mock_session.get.return_value = _make_html_response(detalle_html)

        with patch("downloader.get_filing_enc", return_value="CACHED_ENC") as mock_enc:
            with patch(
                "downloader.download_pdf_with_enc",
                return_value=str(tmp_path / "file.pdf"),
            ) as mock_dl:
                result = attempt_pdf_download(
                    filing, mock_session, {}, tmp_db, str(tmp_path)
                )

        mock_enc.assert_called_once()
        mock_dl.assert_called_once()
        assert result == str(tmp_path / "file.pdf")

    def test_returns_none_when_enc_unavailable(self, tmp_db: EncCache, tmp_path):
        filing = {"emisora": "CEMEX", "asunto": "Report", "key": "99999"}

        with patch("downloader.get_filing_enc", return_value=None):
            result = attempt_pdf_download(
                filing, MagicMock(), {}, tmp_db, str(tmp_path)
            )

        assert result is None


# ---------------------------------------------------------------------------
# download_batch_parallel
# ---------------------------------------------------------------------------


class TestDownloadBatchParallel:
    def test_resolves_enc_for_all_filings(self, tmp_db: EncCache, tmp_path):
        filings = [
            {"emisora": "F1", "asunto": "A1", "key": "1"},
            {"emisora": "F2", "asunto": "A2", "key": "2"},
        ]

        def fake_enc(session, hidden, key, cache):
            return f"ENC_{key}"

        with patch("downloader.get_filing_enc", side_effect=fake_enc):
            with patch("downloader._download_worker", return_value="/tmp/file.pdf"):
                download_batch_parallel(
                    filings,
                    MagicMock(),
                    {},
                    tmp_db,
                    str(tmp_path),
                    workers=2,
                )

        # Both filings should have pdf_path set
        assert all(f.get("pdf_path") is not None for f in filings)

    def test_skips_filings_without_key(self, tmp_db: EncCache, tmp_path):
        filings = [
            {"emisora": "F1", "asunto": "A1", "key": None},
        ]

        enc_calls = []

        def fake_enc(session, hidden, key, cache):
            enc_calls.append(key)
            return "ENC"

        with patch("downloader.get_filing_enc", side_effect=fake_enc):
            with patch("downloader._download_worker", return_value="/tmp/f.pdf"):
                download_batch_parallel(
                    filings, MagicMock(), {}, tmp_db, str(tmp_path)
                )

        assert enc_calls == []

    def test_handles_failed_enc_resolution(self, tmp_db: EncCache, tmp_path):
        filings = [
            {"emisora": "F1", "asunto": "A1", "key": "1"},
        ]

        with patch("downloader.get_filing_enc", return_value=None):
            # Should not raise even when enc is not available
            download_batch_parallel(
                filings, MagicMock(), {}, tmp_db, str(tmp_path)
            )


# ---------------------------------------------------------------------------
# _download_worker
# ---------------------------------------------------------------------------


class TestDownloadWorker:
    def test_creates_fresh_session(self, tmp_path):
        with patch("downloader.make_session") as mock_factory:
            mock_session = MagicMock()
            mock_factory.return_value = mock_session
            with patch("downloader.download_pdf_with_enc", return_value="/tmp/f.pdf"):
                _download_worker("ENC_VAL", "hint.pdf", str(tmp_path))

        mock_factory.assert_called_once()

    def test_closes_session_after_download(self, tmp_path):
        with patch("downloader.make_session") as mock_factory:
            mock_session = MagicMock()
            mock_factory.return_value = mock_session
            with patch("downloader.download_pdf_with_enc", return_value="/tmp/f.pdf"):
                _download_worker("ENC_VAL", "hint.pdf", str(tmp_path))

        mock_session.close.assert_called_once()

    def test_closes_session_even_on_exception(self, tmp_path):
        with patch("downloader.make_session") as mock_factory:
            mock_session = MagicMock()
            mock_factory.return_value = mock_session
            with patch(
                "downloader.download_pdf_with_enc",
                side_effect=Exception("boom"),
            ):
                try:
                    _download_worker("ENC_VAL", "hint.pdf", str(tmp_path))
                except Exception:
                    pass

        mock_session.close.assert_called_once()

    def test_returns_none_on_download_failure(self, tmp_path):
        with patch("downloader.make_session", return_value=MagicMock()):
            with patch("downloader.download_pdf_with_enc", return_value=None):
                result = _download_worker("ENC_FAIL", "hint.pdf", str(tmp_path))

        assert result is None
