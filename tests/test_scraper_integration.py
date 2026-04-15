"""
Integration-level tests for scraper.py orchestration paths.

All HTTP calls are mocked.  These tests exercise:
  - CNBVScraper.initialize
  - CNBVScraper.search_filings (first page, pagination)
  - CNBVScraper._download_page_filings
  - CNBVScraper.run (full pipeline)
  - cmd_crawl / cmd_monitor / cmd_export / cmd_stats subcommand handlers
  - _setup_logging
"""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, call, patch

import pytest
import requests

from scraper import (
    CNBVScraper,
    _setup_logging,
    build_parser,
    cmd_crawl,
    cmd_export,
    cmd_monitor,
    cmd_stats,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _html_response(html: str, status: int = 200) -> MagicMock:
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
# _setup_logging
# ---------------------------------------------------------------------------


class TestSetupLogging:
    def test_no_log_file(self):
        # Should not raise
        _setup_logging(log_file=None)

    def test_with_log_file(self, tmp_path):
        log_path = str(tmp_path / "test.log")
        _setup_logging(log_file=log_path)
        import logging

        logging.getLogger().info("test message")
        # File should exist after a log write
        assert os.path.exists(log_path)


# ---------------------------------------------------------------------------
# CNBVScraper.initialize
# ---------------------------------------------------------------------------


class TestInitialize:
    def test_sets_hidden_fields(self, tmp_path, initial_page_html):
        scraper = _make_scraper(tmp_path)

        with patch("scraper.safe_get", return_value=_html_response(initial_page_html)):
            scraper.initialize()

        assert "__VIEWSTATE" in scraper.hidden_fields
        assert scraper.hidden_fields["__VIEWSTATE"] == "FAKE_VS_VALUE_123abc"

    def test_extracts_eventvalidation(self, tmp_path, initial_page_html):
        scraper = _make_scraper(tmp_path)

        with patch("scraper.safe_get", return_value=_html_response(initial_page_html)):
            scraper.initialize()

        assert scraper.hidden_fields.get("__EVENTVALIDATION") == "FAKE_EV_VALUE_456def"


# ---------------------------------------------------------------------------
# CNBVScraper.search_filings
# ---------------------------------------------------------------------------


class TestSearchFilings:
    def test_parses_filings_from_first_page(self, tmp_path, asp_delta):
        scraper = _make_scraper(tmp_path)
        scraper.hidden_fields = {"__VIEWSTATE": "VS", "__EVENTVALIDATION": "EV"}

        delta_resp = MagicMock()
        delta_resp.status_code = 200
        delta_resp.text = asp_delta
        delta_resp.raise_for_status = MagicMock()

        with patch("scraper.safe_post", return_value=delta_resp):
            filings = scraper.search_filings(period="2", max_pages=1)

        assert len(filings) == 2
        assert filings[0]["emisora"] == "FEMSA"
        assert filings[1]["emisora"] == "BIMBO"

    def test_hidden_fields_updated_after_search(self, tmp_path, asp_delta):
        scraper = _make_scraper(tmp_path)
        scraper.hidden_fields = {"__VIEWSTATE": "OLD_VS"}

        delta_resp = MagicMock()
        delta_resp.status_code = 200
        delta_resp.text = asp_delta
        delta_resp.raise_for_status = MagicMock()

        with patch("scraper.safe_post", return_value=delta_resp):
            scraper.search_filings()

        assert scraper.hidden_fields["__VIEWSTATE"] == "NEW_VS_VALUE_789"

    def test_pagination_calls_grid_callback(self, tmp_path, asp_delta, dx_grid_response):
        scraper = _make_scraper(tmp_path)
        scraper.hidden_fields = {"__VIEWSTATE": "VS", "__EVENTVALIDATION": "EV"}

        delta_resp = MagicMock()
        delta_resp.text = asp_delta
        delta_resp.raise_for_status = MagicMock()

        page2_resp = MagicMock()
        page2_resp.text = dx_grid_response

        with patch("scraper.safe_post", side_effect=[delta_resp, page2_resp]):
            filings = scraper.search_filings(period="2", max_pages=2)

        # First page (2 filings from delta) + second page (2 filings from DX response)
        assert len(filings) == 4

    def test_search_stops_on_empty_page(self, tmp_path, asp_delta):
        scraper = _make_scraper(tmp_path)
        scraper.hidden_fields = {"__VIEWSTATE": "VS"}

        delta_resp = MagicMock()
        delta_resp.text = asp_delta
        delta_resp.raise_for_status = MagicMock()

        empty_resp = MagicMock()
        empty_resp.text = "/*DX*/({'result':'<table></table>'})"

        with patch("scraper.safe_post", side_effect=[delta_resp, empty_resp]):
            filings = scraper.search_filings(period="2", max_pages=3)

        # Only page 1 filings (pagination stopped on empty page 2)
        assert len(filings) == 2


# ---------------------------------------------------------------------------
# CNBVScraper._download_page_filings
# ---------------------------------------------------------------------------


class TestDownloadPageFilings:
    def test_sequential_download(self, tmp_path):
        scraper = _make_scraper(tmp_path)
        scraper.download_docs = True
        scraper.parallel_workers = 1
        filings = [
            {"fecha": "01/01/2026", "emisora": "F1", "asunto": "A1", "key": "1"},
            {"fecha": "02/01/2026", "emisora": "F2", "asunto": "A2", "key": "2"},
        ]

        with patch("scraper.attempt_pdf_download", return_value="/tmp/f.pdf") as mock_dl:
            scraper._download_page_filings(filings)

        assert mock_dl.call_count == 2

    def test_parallel_download(self, tmp_path):
        scraper = _make_scraper(tmp_path)
        scraper.download_docs = True
        scraper.parallel_workers = 3
        filings = [{"fecha": "01/01/2026", "emisora": "F1", "asunto": "A1", "key": "1"}]

        with patch("scraper.download_batch_parallel") as mock_batch:
            scraper._download_page_filings(filings)

        mock_batch.assert_called_once()


# ---------------------------------------------------------------------------
# CNBVScraper.run (full pipeline)
# ---------------------------------------------------------------------------


class TestRun:
    def test_run_saves_output_json(self, tmp_path, asp_delta):
        output = str(tmp_path / "filings.json")
        scraper = CNBVScraper(
            output_path=output,
            pdf_dir=str(tmp_path / "pdfs"),
            download_docs=False,
            db_path=str(tmp_path / "enc.db"),
        )

        sample_filings = [
            {"fecha": "01/01/2026", "emisora": "FEMSA", "asunto": "Informe Anual", "key": "1"},
        ]

        with patch.object(scraper, "initialize"):
            with patch.object(scraper, "search_filings", return_value=sample_filings):
                scraper.run()

        assert os.path.exists(output)
        with open(output) as fh:
            data = json.load(fh)
        assert data["metadata"]["total_filings"] == 1

    def test_run_calls_sys_exit_on_no_filings(self, tmp_path):
        scraper = CNBVScraper(
            output_path=str(tmp_path / "f.json"),
            download_docs=False,
            db_path=str(tmp_path / "enc.db"),
        )

        with patch.object(scraper, "initialize"):
            with patch.object(scraper, "search_filings", return_value=[]):
                with pytest.raises(SystemExit):
                    scraper.run()

    def test_run_enriches_all_filing_types(self, tmp_path):
        output = str(tmp_path / "filings.json")
        scraper = CNBVScraper(
            output_path=output,
            download_docs=False,
            db_path=str(tmp_path / "enc.db"),
        )
        filings = [
            {"fecha": "01/01/2026", "emisora": "A", "asunto": "Informe Anual", "key": "1"},
            {"fecha": "01/01/2026", "emisora": "B", "asunto": "Otro documento", "key": "2"},
        ]
        with patch.object(scraper, "initialize"):
            with patch.object(scraper, "search_filings", return_value=filings):
                scraper.run()

        with open(output) as fh:
            data = json.load(fh)
        types = {f["filing_type"] for f in data["filings"]}
        assert "annual_report" in types
        assert "other" in types


# ---------------------------------------------------------------------------
# Subcommand handler tests
# ---------------------------------------------------------------------------


class TestCmdCrawl:
    def test_cmd_crawl_calls_run(self, tmp_path):
        parser = build_parser()
        args = parser.parse_args([
            "crawl",
            "--output", str(tmp_path / "out.json"),
            "--db", str(tmp_path / "enc.db"),
            "--no-download",
        ])

        with patch("scraper.CNBVScraper") as MockScraper:
            mock_instance = MagicMock()
            MockScraper.return_value = mock_instance
            result = cmd_crawl(args)

        mock_instance.run.assert_called_once()
        assert result == 0

    def test_cmd_crawl_all_pages_flag(self, tmp_path):
        parser = build_parser()
        args = parser.parse_args([
            "crawl",
            "--max-pages", "-1",
            "--output", str(tmp_path / "out.json"),
            "--db", str(tmp_path / "enc.db"),
        ])

        with patch("scraper.CNBVScraper") as MockScraper:
            MockScraper.return_value = MagicMock()
            cmd_crawl(args)

        # max_pages -1 should become 99999
        call_kwargs = MockScraper.call_args[1]
        assert call_kwargs["max_pages"] == 99999


class TestCmdMonitor:
    def test_cmd_monitor_calls_monitor(self, tmp_path):
        parser = build_parser()
        args = parser.parse_args([
            "monitor",
            "--output", str(tmp_path / "out.json"),
            "--db", str(tmp_path / "enc.db"),
        ])

        with patch("scraper.CNBVScraper") as MockScraper:
            mock_instance = MagicMock()
            MockScraper.return_value = mock_instance
            cmd_monitor(args)

        mock_instance.monitor.assert_called_once_with(interval=300)

    def test_cmd_monitor_writes_state_file_when_start_key_given(self, tmp_path):
        parser = build_parser()
        output = str(tmp_path / "out.json")
        args = parser.parse_args([
            "monitor",
            "--start-key", "453884",
            "--output", output,
            "--db", str(tmp_path / "enc.db"),
        ])

        with patch("scraper.CNBVScraper") as MockScraper:
            MockScraper.return_value = MagicMock()
            cmd_monitor(args)

        state_file = os.path.join(tmp_path, ".monitor_state.json")
        assert os.path.exists(state_file)
        with open(state_file) as fh:
            state = json.load(fh)
        assert state["last_key"] == 453884


class TestCmdExport:
    def test_cmd_export_calls_export(self, tmp_path):
        parser = build_parser()
        args = parser.parse_args([
            "export",
            "--output", str(tmp_path / "out.json"),
            "--db", str(tmp_path / "enc.db"),
        ])

        with patch("scraper.CNBVScraper") as MockScraper:
            mock_instance = MagicMock()
            MockScraper.return_value = mock_instance
            cmd_export(args)

        mock_instance.export.assert_called_once()


class TestCmdStats:
    def test_cmd_stats_calls_stats(self, tmp_path):
        parser = build_parser()
        args = parser.parse_args([
            "stats",
            "--output", str(tmp_path / "out.json"),
            "--db", str(tmp_path / "enc.db"),
        ])

        with patch("scraper.CNBVScraper") as MockScraper:
            mock_instance = MagicMock()
            MockScraper.return_value = mock_instance
            cmd_stats(args)

        mock_instance.stats.assert_called_once()


# ---------------------------------------------------------------------------
# ISIN resolution wiring in CNBVScraper
# ---------------------------------------------------------------------------


class TestIsinWiring:
    """Tests that ISIN lookup is correctly wired into the scraper pipeline."""

    def test_isin_map_default_is_empty(self, tmp_path):
        scraper = CNBVScraper(
            output_path=str(tmp_path / "f.json"),
            db_path=str(tmp_path / "enc.db"),
        )
        assert scraper.isin_map == {}

    def test_with_isin_false_does_not_call_load_isin_map(self, tmp_path):
        """run() must NOT call _load_isin_map when with_isin=False (default)."""
        scraper = CNBVScraper(
            output_path=str(tmp_path / "f.json"),
            db_path=str(tmp_path / "enc.db"),
            download_docs=False,
            with_isin=False,
        )
        sample_filings = [
            {"fecha": "01/01/2026", "emisora": "FEMSA", "asunto": "Annual", "key": "1"},
        ]
        with patch.object(scraper, "initialize"):
            with patch.object(scraper, "search_filings", return_value=sample_filings):
                with patch.object(scraper, "_load_isin_map") as mock_load:
                    scraper.run()

        mock_load.assert_not_called()

    def test_with_isin_true_calls_load_isin_map(self, tmp_path):
        """run() must call _load_isin_map when with_isin=True."""
        scraper = CNBVScraper(
            output_path=str(tmp_path / "f.json"),
            db_path=str(tmp_path / "enc.db"),
            download_docs=False,
            with_isin=True,
        )
        sample_filings = [
            {"fecha": "01/01/2026", "emisora": "FEMSA", "asunto": "Annual", "key": "1"},
        ]
        with patch.object(scraper, "initialize"):
            with patch.object(scraper, "search_filings", return_value=sample_filings):
                with patch.object(scraper, "_load_isin_map") as mock_load:
                    scraper.run()

        mock_load.assert_called_once()

    def test_isin_populated_from_isin_map(self, tmp_path):
        """upsert_filing must receive the ISIN for a known emisora."""
        from db import FilingsDB

        filings_db_path = str(tmp_path / "filings.db")
        scraper = CNBVScraper(
            output_path=str(tmp_path / "f.json"),
            db_path=str(tmp_path / "enc.db"),
            filings_db_path=filings_db_path,
            download_docs=False,
            with_isin=True,
        )
        scraper.isin_map = {"FEMSA": "MX01FE100003"}

        sample_filings = [
            {"fecha": "01/01/2026", "emisora": "FEMSA", "asunto": "Annual", "key": "42"},
        ]
        with patch.object(scraper, "initialize"):
            with patch.object(scraper, "search_filings", return_value=sample_filings):
                with patch.object(scraper, "_load_isin_map"):
                    scraper.run()

        db = FilingsDB(filings_db_path)
        row = db.get_filing("cnbv_42")
        assert row is not None
        assert row["isin"] == "MX01FE100003"
        db.close()

    def test_isin_none_when_emisora_not_in_map(self, tmp_path):
        """Filings for unknown emisoras should get isin=None."""
        from db import FilingsDB

        filings_db_path = str(tmp_path / "filings.db")
        scraper = CNBVScraper(
            output_path=str(tmp_path / "f.json"),
            db_path=str(tmp_path / "enc.db"),
            filings_db_path=filings_db_path,
            download_docs=False,
            with_isin=True,
        )
        # BIMBO is not in the map
        scraper.isin_map = {"FEMSA": "MX01FE100003"}

        sample_filings = [
            {"fecha": "01/01/2026", "emisora": "BIMBO", "asunto": "Trimestral", "key": "99"},
        ]
        with patch.object(scraper, "initialize"):
            with patch.object(scraper, "search_filings", return_value=sample_filings):
                with patch.object(scraper, "_load_isin_map"):
                    scraper.run()

        db = FilingsDB(filings_db_path)
        row = db.get_filing("cnbv_99")
        assert row is not None
        assert row["isin"] is None
        db.close()

    def test_isin_none_when_with_isin_false(self, tmp_path):
        """Without --with-isin, isin_map stays empty so all ISINs are None."""
        from db import FilingsDB

        filings_db_path = str(tmp_path / "filings.db")
        scraper = CNBVScraper(
            output_path=str(tmp_path / "f.json"),
            db_path=str(tmp_path / "enc.db"),
            filings_db_path=filings_db_path,
            download_docs=False,
            with_isin=False,
        )

        sample_filings = [
            {"fecha": "01/01/2026", "emisora": "FEMSA", "asunto": "Annual", "key": "7"},
        ]
        with patch.object(scraper, "initialize"):
            with patch.object(scraper, "search_filings", return_value=sample_filings):
                scraper.run()

        db = FilingsDB(filings_db_path)
        row = db.get_filing("cnbv_7")
        assert row is not None
        assert row["isin"] is None
        db.close()

    def test_load_isin_map_logs_warning_on_failure(self, tmp_path, caplog):
        """_load_isin_map must log a warning and leave isin_map empty on failure."""
        import logging

        scraper = CNBVScraper(
            output_path=str(tmp_path / "f.json"),
            db_path=str(tmp_path / "enc.db"),
            with_isin=True,
        )
        with patch("scraper.load_isin_map", side_effect=Exception("API down")):
            with caplog.at_level(logging.WARNING):
                scraper._load_isin_map()

        assert scraper.isin_map == {}
        assert "BIVA" in caplog.text

    def test_load_isin_map_logs_warning_when_empty(self, tmp_path, caplog):
        """When load_isin_map returns {}, _load_isin_map must log a warning."""
        import logging

        scraper = CNBVScraper(
            output_path=str(tmp_path / "f.json"),
            db_path=str(tmp_path / "enc.db"),
            with_isin=True,
        )
        with patch("scraper.load_isin_map", return_value={}):
            with caplog.at_level(logging.WARNING):
                scraper._load_isin_map()

        assert scraper.isin_map == {}
        assert "BIVA" in caplog.text

    def test_load_isin_map_sets_isin_map_on_success(self, tmp_path):
        """_load_isin_map must populate self.isin_map when load succeeds."""
        scraper = CNBVScraper(
            output_path=str(tmp_path / "f.json"),
            db_path=str(tmp_path / "enc.db"),
            with_isin=True,
        )
        expected = {"FEMSA": "MX01FE100003", "AC": "MX01AC100006"}
        with patch("scraper.load_isin_map", return_value=expected):
            scraper._load_isin_map()

        assert scraper.isin_map == expected


class TestCmdCrawlIsinFlags:
    """Tests that --with-isin and --isin-cache CLI flags are forwarded correctly."""

    def test_with_isin_flag_parsed(self):
        parser = build_parser()
        args = parser.parse_args(["crawl", "--with-isin"])
        assert args.with_isin is True

    def test_with_isin_flag_default_false(self):
        parser = build_parser()
        args = parser.parse_args(["crawl"])
        assert args.with_isin is False

    def test_isin_cache_flag_parsed(self):
        parser = build_parser()
        args = parser.parse_args(["crawl", "--isin-cache", "/tmp/my_cache.json"])
        assert args.isin_cache == "/tmp/my_cache.json"

    def test_isin_cache_flag_default(self):
        parser = build_parser()
        args = parser.parse_args(["crawl"])
        assert args.isin_cache == "_biva_isin_cache.json"

    def test_cmd_crawl_passes_with_isin_to_scraper(self, tmp_path):
        parser = build_parser()
        args = parser.parse_args([
            "crawl",
            "--with-isin",
            "--output", str(tmp_path / "out.json"),
            "--db", str(tmp_path / "enc.db"),
            "--no-download",
        ])

        with patch("scraper.CNBVScraper") as MockScraper:
            MockScraper.return_value = MagicMock()
            cmd_crawl(args)

        call_kwargs = MockScraper.call_args[1]
        assert call_kwargs["with_isin"] is True

    def test_cmd_crawl_passes_isin_cache_to_scraper(self, tmp_path):
        parser = build_parser()
        args = parser.parse_args([
            "crawl",
            "--isin-cache", "/tmp/biva.json",
            "--output", str(tmp_path / "out.json"),
            "--db", str(tmp_path / "enc.db"),
            "--no-download",
        ])

        with patch("scraper.CNBVScraper") as MockScraper:
            MockScraper.return_value = MagicMock()
            cmd_crawl(args)

        call_kwargs = MockScraper.call_args[1]
        assert call_kwargs["isin_cache_path"] == "/tmp/biva.json"
