"""
Integration tests for scraper.py

Tests cover:
  - CLI argument parsing (build_parser)
  - Subcommand routing
  - CNBVScraper.stats
  - CNBVScraper.export
  - CNBVScraper._load_state / _save_state
  - CNBVScraper._append_filing
  - classify_filing_type enrichment in run()
"""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from scraper import CNBVScraper, build_parser


# ---------------------------------------------------------------------------
# CLI argument parser tests
# ---------------------------------------------------------------------------


class TestBuildParser:
    def test_crawl_subcommand_parsed(self):
        parser = build_parser()
        args = parser.parse_args(["crawl"])
        assert args.command == "crawl"

    def test_monitor_subcommand_parsed(self):
        parser = build_parser()
        args = parser.parse_args(["monitor"])
        assert args.command == "monitor"

    def test_export_subcommand_parsed(self):
        parser = build_parser()
        args = parser.parse_args(["export"])
        assert args.command == "export"

    def test_stats_subcommand_parsed(self):
        parser = build_parser()
        args = parser.parse_args(["stats"])
        assert args.command == "stats"

    def test_crawl_defaults(self):
        parser = build_parser()
        args = parser.parse_args(["crawl"])
        assert args.max_pages == 0
        assert args.period == "2"
        assert args.no_download is False
        assert args.parallel == 1
        assert args.incremental is False
        assert args.resume is False

    def test_crawl_max_pages_flag(self):
        parser = build_parser()
        args = parser.parse_args(["crawl", "--max-pages", "10"])
        assert args.max_pages == 10

    def test_crawl_no_download_flag(self):
        parser = build_parser()
        args = parser.parse_args(["crawl", "--no-download"])
        assert args.no_download is True

    def test_crawl_period_flag(self):
        parser = build_parser()
        args = parser.parse_args(["crawl", "--period", "4"])
        assert args.period == "4"

    def test_crawl_parallel_flag(self):
        parser = build_parser()
        args = parser.parse_args(["crawl", "--parallel", "5"])
        assert args.parallel == 5

    def test_crawl_incremental_flag(self):
        parser = build_parser()
        args = parser.parse_args(["crawl", "--incremental"])
        assert args.incremental is True

    def test_crawl_resume_flag(self):
        parser = build_parser()
        args = parser.parse_args(["crawl", "--resume"])
        assert args.resume is True

    def test_monitor_interval_flag(self):
        parser = build_parser()
        args = parser.parse_args(["monitor", "--interval", "60"])
        assert args.interval == 60

    def test_monitor_start_key_flag(self):
        parser = build_parser()
        args = parser.parse_args(["monitor", "--start-key", "453884"])
        assert args.start_key == 453884

    def test_log_file_flag_on_crawl(self):
        parser = build_parser()
        args = parser.parse_args(["crawl", "--log-file", "/tmp/scraper.log"])
        assert args.log_file == "/tmp/scraper.log"

    def test_output_flag(self):
        parser = build_parser()
        args = parser.parse_args(["crawl", "--output", "my_filings.json"])
        assert args.output == "my_filings.json"

    def test_db_flag(self):
        parser = build_parser()
        args = parser.parse_args(["crawl", "--db", "custom.db"])
        assert args.db == "custom.db"

    def test_missing_subcommand_raises(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_period_invalid_choice_raises(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["crawl", "--period", "99"])


# ---------------------------------------------------------------------------
# CNBVScraper._load_state / _save_state
# ---------------------------------------------------------------------------


class TestMonitorState:
    def test_load_state_returns_zero_when_no_file(self, tmp_path):
        scraper = CNBVScraper(
            output_path=str(tmp_path / "filings.json"),
            db_path=str(tmp_path / "enc.db"),
        )
        state = scraper._load_state()
        assert state["last_key"] == 0

    def test_save_and_load_state(self, tmp_path):
        scraper = CNBVScraper(
            output_path=str(tmp_path / "filings.json"),
            db_path=str(tmp_path / "enc.db"),
        )
        scraper._save_state({"last_key": 42000})
        loaded = scraper._load_state()
        assert loaded["last_key"] == 42000

    def test_load_state_uses_max_of_cache_and_file(self, tmp_path):
        scraper = CNBVScraper(
            output_path=str(tmp_path / "filings.json"),
            db_path=str(tmp_path / "enc.db"),
        )
        # Put a higher key in the cache
        scraper.cache.put(99999, "ENC")
        scraper._save_state({"last_key": 50000})
        state = scraper._load_state()
        assert state["last_key"] == 99999


# ---------------------------------------------------------------------------
# CNBVScraper._append_filing
# ---------------------------------------------------------------------------


class TestAppendFiling:
    def test_creates_new_file(self, tmp_path):
        output = str(tmp_path / "out.json")
        scraper = CNBVScraper(
            output_path=output,
            db_path=str(tmp_path / "enc.db"),
        )
        filing = {"key": "1", "emisora": "FEMSA", "asunto": "Annual"}
        scraper._append_filing(filing)

        assert os.path.exists(output)
        with open(output) as fh:
            data = json.load(fh)
        assert len(data["filings"]) == 1
        assert data["filings"][0]["emisora"] == "FEMSA"

    def test_appends_to_existing_file(self, tmp_path):
        output = str(tmp_path / "out.json")
        scraper = CNBVScraper(
            output_path=output,
            db_path=str(tmp_path / "enc.db"),
        )
        scraper._append_filing({"key": "1", "emisora": "F1", "asunto": "A1"})
        scraper._append_filing({"key": "2", "emisora": "F2", "asunto": "A2"})

        with open(output) as fh:
            data = json.load(fh)
        assert len(data["filings"]) == 2

    def test_updates_total_count_in_metadata(self, tmp_path):
        output = str(tmp_path / "out.json")
        scraper = CNBVScraper(
            output_path=output,
            db_path=str(tmp_path / "enc.db"),
        )
        scraper._append_filing({"key": "1", "emisora": "X", "asunto": "Y"})
        scraper._append_filing({"key": "2", "emisora": "X", "asunto": "Z"})

        with open(output) as fh:
            data = json.load(fh)
        assert data["metadata"]["total_filings"] == 2


# ---------------------------------------------------------------------------
# CNBVScraper.stats
# ---------------------------------------------------------------------------


class TestStats:
    def test_stats_runs_without_output_file(self, tmp_path, capsys):
        scraper = CNBVScraper(
            output_path=str(tmp_path / "no_file.json"),
            db_path=str(tmp_path / "enc.db"),
        )
        scraper.stats()
        captured = capsys.readouterr()
        assert "Statistics" in captured.out
        assert "Enc cache entries" in captured.out

    def test_stats_shows_cache_count(self, tmp_path, capsys):
        scraper = CNBVScraper(
            output_path=str(tmp_path / "f.json"),
            db_path=str(tmp_path / "enc.db"),
        )
        scraper.cache.put(1, "ENC1")
        scraper.cache.put(2, "ENC2")
        scraper.stats()
        captured = capsys.readouterr()
        assert "2" in captured.out

    def test_stats_shows_filings_from_json(self, tmp_path, capsys):
        from datetime import datetime
        from db import FilingsDB

        output = str(tmp_path / "filings.json")
        filings_db_path = str(tmp_path / "filings_cache.db")

        # Populate FilingsDB so stats() can read from it.
        db = FilingsDB(filings_db_path)
        today_str = datetime.now().strftime("%d/%m/%Y")
        for i in range(15):
            db.upsert_filing(
                filing_id=f"cnbv_{i}",
                filing_date=today_str,
                downloaded=(i < 10),
                download_path=f"/tmp/f{i}.pdf" if i < 10 else "",
            )
        db.close()

        scraper = CNBVScraper(
            output_path=output,
            db_path=str(tmp_path / "enc.db"),
            filings_db_path=filings_db_path,
        )
        scraper.stats()
        captured = capsys.readouterr()
        assert "15" in captured.out
        assert "10" in captured.out


# ---------------------------------------------------------------------------
# CNBVScraper.export
# ---------------------------------------------------------------------------


class TestExport:
    def test_export_copies_file(self, tmp_path):
        source = str(tmp_path / "filings.json")
        dest = str(tmp_path / "exported.json")
        data = {"metadata": {"total_filings": 3}, "filings": [{"key": "1"}]}
        with open(source, "w") as fh:
            json.dump(data, fh)

        scraper = CNBVScraper(
            output_path=source,
            db_path=str(tmp_path / "enc.db"),
        )
        scraper.export(output_path=dest)

        assert os.path.exists(dest)
        with open(dest) as fh:
            exported = json.load(fh)
        assert exported["metadata"]["total_filings"] == 3

    def test_export_warns_when_no_file(self, tmp_path, caplog):
        import logging

        scraper = CNBVScraper(
            output_path=str(tmp_path / "nonexistent.json"),
            db_path=str(tmp_path / "enc.db"),
        )
        with caplog.at_level(logging.WARNING):
            scraper.export()
        assert "No filings file" in caplog.text


# ---------------------------------------------------------------------------
# Filing type enrichment in run()
# ---------------------------------------------------------------------------


class TestFilingTypeEnrichment:
    def test_run_enriches_filing_type(self, tmp_path):
        """After run(), each filing should have a filing_type field."""
        output = str(tmp_path / "filings.json")
        scraper = CNBVScraper(
            output_path=output,
            pdf_dir=str(tmp_path / "pdfs"),
            download_docs=False,
            db_path=str(tmp_path / "enc.db"),
        )

        sample_filings = [
            {"fecha": "01/01/2026", "emisora": "FEMSA", "asunto": "Informe Anual 2025", "key": "1"},
            {"fecha": "02/01/2026", "emisora": "BIMBO", "asunto": "Prospecto", "key": "2"},
        ]

        with patch.object(scraper, "initialize"):
            with patch.object(scraper, "search_filings", return_value=sample_filings):
                scraper.run()

        with open(output) as fh:
            data = json.load(fh)

        filings = data["filings"]
        assert filings[0]["filing_type"] == "annual_report"
        assert filings[1]["filing_type"] == "prospectus"
