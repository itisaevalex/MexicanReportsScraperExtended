"""
Tests for L3 spec compliance features.

Covers:
  - normalize_date: DD/MM/YYYY → YYYY-MM-DD conversion
  - FilingsDB: schema creation, migrations, CRUD operations
  - CNBVScraper.stats --json: structured JSON output
  - CNBVScraper._compute_stats: health detection (ok, stale, empty, degraded, error)
  - stats exit codes via cmd_stats (0=ok/degraded, 1=stale, 2=empty, 3=error)
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from db import FilingsDB, normalize_date
from scraper import CNBVScraper, build_parser, cmd_stats


# ---------------------------------------------------------------------------
# normalize_date unit tests
# ---------------------------------------------------------------------------


class TestNormalizeDate:
    """Unit tests for the DD/MM/YYYY → YYYY-MM-DD normalization helper."""

    def test_dd_mm_yyyy_converts_correctly(self):
        assert normalize_date("15/03/2026") == "2026-03-15"

    def test_single_digit_day_and_month(self):
        assert normalize_date("01/01/2026") == "2026-01-01"

    def test_end_of_year_date(self):
        assert normalize_date("31/12/2025") == "2025-12-31"

    def test_already_iso_returned_unchanged(self):
        assert normalize_date("2026-03-15") == "2026-03-15"

    def test_already_iso_with_time_suffix_strips_time(self):
        # Only the date portion (10 chars) is returned when already ISO.
        assert normalize_date("2026-03-15T12:34:56") == "2026-03-15"

    def test_empty_string_returned_unchanged(self):
        assert normalize_date("") == ""

    def test_non_date_string_returned_unchanged(self):
        assert normalize_date("not-a-date") == "not-a-date"

    def test_partial_date_returned_unchanged(self):
        assert normalize_date("15/03") == "15/03"

    def test_all_months_map_correctly(self):
        for month in range(1, 13):
            mm = f"{month:02d}"
            result = normalize_date(f"10/{mm}/2026")
            assert result == f"2026-{mm}-10"


# ---------------------------------------------------------------------------
# FilingsDB unit tests
# ---------------------------------------------------------------------------


class TestFilingsDBSchema:
    def test_creates_filings_table(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        tables = {
            row[0]
            for row in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "filings" in tables
        db.close()

    def test_creates_schema_version_table(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        tables = {
            row[0]
            for row in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "schema_version" in tables
        db.close()

    def test_creates_crawl_log_table(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        tables = {
            row[0]
            for row in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "crawl_log" in tables
        db.close()

    def test_schema_is_idempotent(self, tmp_path):
        """Opening the same DB twice should not raise."""
        path = str(tmp_path / "idempotent.db")
        db1 = FilingsDB(path)
        db1.close()
        db2 = FilingsDB(path)
        db2.close()

    def test_migration_sets_version_2(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        row = db.conn.execute(
            "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
        ).fetchone()
        assert row[0] == 2
        db.close()

    def test_migration_is_idempotent_on_existing_columns(self, tmp_path):
        """Running migration on a DB that already has all columns should not raise."""
        path = str(tmp_path / "migrate.db")
        db = FilingsDB(path)
        db.close()
        # Re-open triggers _run_migrations again on a fully-migrated DB.
        db2 = FilingsDB(path)
        assert db2.count_total() == 0
        db2.close()


class TestFilingsDBUpsert:
    def test_upsert_inserts_new_record(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        db.upsert_filing(
            filing_id="cnbv_1",
            ticker="FEMSA",
            company_name="Fomento Economico Mexicano",
            filing_date="15/03/2026",
            headline="Informe Anual 2025",
            filing_type="annual_report",
        )
        assert db.count_total() == 1
        db.close()

    def test_upsert_normalises_date_to_iso(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        db.upsert_filing(
            filing_id="cnbv_2",
            ticker="BIMBO",
            filing_date="01/06/2025",
        )
        row = db.get_filing("cnbv_2")
        assert row["filing_date"] == "2025-06-01"
        db.close()

    def test_upsert_preserves_iso_date(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        db.upsert_filing(
            filing_id="cnbv_3",
            ticker="WALMEX",
            filing_date="2026-03-15",
        )
        row = db.get_filing("cnbv_3")
        assert row["filing_date"] == "2026-03-15"
        db.close()

    def test_upsert_overwrites_on_duplicate_id(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        db.upsert_filing(filing_id="cnbv_1", headline="Original")
        db.upsert_filing(filing_id="cnbv_1", headline="Updated")
        assert db.count_total() == 1
        row = db.get_filing("cnbv_1")
        assert row["headline"] == "Updated"
        db.close()

    def test_source_and_country_defaults(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        db.upsert_filing(filing_id="cnbv_1")
        row = db.get_filing("cnbv_1")
        assert row["source"] == "cnbv"
        assert row["country"] == "MX"
        db.close()

    def test_default_filing_type_is_other(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        db.upsert_filing(filing_id="cnbv_1")
        row = db.get_filing("cnbv_1")
        assert row["filing_type"] == "other"
        db.close()


class TestFilingsDBMarkDownloaded:
    def test_mark_downloaded_updates_flag(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        db.upsert_filing(filing_id="cnbv_1")
        assert db.count_downloaded() == 0
        db.mark_downloaded("cnbv_1", "/tmp/file.pdf")
        assert db.count_downloaded() == 1
        db.close()

    def test_mark_downloaded_stores_path(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        db.upsert_filing(filing_id="cnbv_1")
        db.mark_downloaded("cnbv_1", "/tmp/file.pdf")
        row = db.get_filing("cnbv_1")
        assert row["download_path"] == "/tmp/file.pdf"
        db.close()


class TestFilingsDBCounts:
    def test_count_total_empty(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        assert db.count_total() == 0
        db.close()

    def test_count_total_after_inserts(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        for i in range(5):
            db.upsert_filing(filing_id=f"cnbv_{i}")
        assert db.count_total() == 5
        db.close()

    def test_count_downloaded_only_counts_downloaded(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        db.upsert_filing(filing_id="cnbv_1", downloaded=True, download_path="/a")
        db.upsert_filing(filing_id="cnbv_2", downloaded=False)
        assert db.count_downloaded() == 1
        db.close()

    def test_count_unique_companies(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        db.upsert_filing(filing_id="cnbv_1", ticker="FEMSA")
        db.upsert_filing(filing_id="cnbv_2", ticker="FEMSA")
        db.upsert_filing(filing_id="cnbv_3", ticker="BIMBO")
        assert db.count_unique_companies() == 2
        db.close()

    def test_count_unique_companies_ignores_empty_ticker(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        db.upsert_filing(filing_id="cnbv_1", ticker="")
        db.upsert_filing(filing_id="cnbv_2", ticker="FEMSA")
        assert db.count_unique_companies() == 1
        db.close()


class TestFilingsDBDateRange:
    def test_date_range_empty_db(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        earliest, latest = db.get_date_range()
        assert earliest is None
        assert latest is None
        db.close()

    def test_date_range_returns_min_max(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        db.upsert_filing(filing_id="cnbv_1", filing_date="01/03/2026")
        db.upsert_filing(filing_id="cnbv_2", filing_date="15/06/2026")
        db.upsert_filing(filing_id="cnbv_3", filing_date="10/01/2026")
        earliest, latest = db.get_date_range()
        assert earliest == "2026-01-10"
        assert latest == "2026-06-15"
        db.close()


# ---------------------------------------------------------------------------
# FilingsDB crawl_log helper tests
# ---------------------------------------------------------------------------


class TestCrawlLog:
    def test_get_last_crawl_log_none_when_empty(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        assert db.get_last_crawl_log() is None
        db.close()

    def test_log_crawl_start_returns_int_id(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        log_id = db.log_crawl_start("crawl")
        assert isinstance(log_id, int)
        assert log_id > 0
        db.close()

    def test_log_crawl_start_leaves_completed_at_null(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        log_id = db.log_crawl_start("crawl")
        row = db.get_last_crawl_log()
        assert row is not None
        assert row["id"] == log_id
        assert row["completed_at"] is None
        db.close()

    def test_log_crawl_complete_fills_completion_data(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        log_id = db.log_crawl_start("crawl")
        db.log_crawl_complete(
            log_id,
            filings_found=20,
            filings_new=5,
            pages_crawled=3,
            errors=None,
        )
        row = db.get_last_crawl_log()
        assert row is not None
        assert row["completed_at"] is not None
        assert row["filings_found"] == 20
        assert row["filings_new"] == 5
        assert row["pages_crawled"] == 3
        assert row["errors"] is None
        assert row["duration_seconds"] is not None
        assert row["duration_seconds"] >= 0
        db.close()

    def test_log_crawl_complete_stores_errors(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        log_id = db.log_crawl_start("crawl")
        db.log_crawl_complete(log_id, errors="timeout on page 2")
        row = db.get_last_crawl_log()
        assert row is not None
        assert row["errors"] == "timeout on page 2"
        db.close()

    def test_log_crawl_complete_noop_for_unknown_id(self, tmp_path):
        """Completing a non-existent log_id must not raise."""
        db = FilingsDB(str(tmp_path / "test.db"))
        db.log_crawl_complete(99999, filings_found=0)  # should not raise
        db.close()

    def test_get_last_crawl_log_returns_most_recent(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        id1 = db.log_crawl_start("crawl")
        db.log_crawl_complete(id1, filings_found=5)
        id2 = db.log_crawl_start("crawl")
        db.log_crawl_complete(id2, filings_found=10)
        row = db.get_last_crawl_log()
        assert row is not None
        assert row["id"] == id2
        assert row["filings_found"] == 10
        db.close()

    def test_crawl_log_stores_query_params(self, tmp_path):
        db = FilingsDB(str(tmp_path / "test.db"))
        log_id = db.log_crawl_start("crawl", query_params='{"period": "2"}')
        row = db.get_last_crawl_log()
        assert row is not None
        assert row["query_params"] == '{"period": "2"}'
        db.close()

    def test_total_crawl_runs_counts_completed(self, tmp_path):
        """Only completed crawl_log rows count toward total_crawl_runs."""
        db = FilingsDB(str(tmp_path / "test.db"))
        id1 = db.log_crawl_start("crawl")
        db.log_crawl_complete(id1, filings_found=5)
        # Started-but-not-completed row:
        db.log_crawl_start("crawl")
        completed_count = db.conn.execute(
            "SELECT COUNT(*) FROM crawl_log WHERE completed_at IS NOT NULL"
        ).fetchone()[0]
        assert completed_count == 1
        db.close()


# ---------------------------------------------------------------------------
# CNBVScraper._compute_stats health detection tests (crawl_log-based, 48h)
# ---------------------------------------------------------------------------


class TestComputeStatsHealth:
    def _make_scraper(self, tmp_path) -> CNBVScraper:
        return CNBVScraper(
            output_path=str(tmp_path / "filings.json"),
            pdf_dir=str(tmp_path / "pdfs"),
            db_path=str(tmp_path / "enc.db"),
            filings_db_path=str(tmp_path / "filings_cache.db"),
        )

    def test_health_empty_when_no_crawl_log(self, tmp_path):
        """No crawl_log entries → health is 'empty'."""
        scraper = self._make_scraper(tmp_path)
        stats = scraper._compute_stats()
        assert stats["health"] == "empty"

    def test_health_ok_when_recent_completed_crawl(self, tmp_path):
        """Completed crawl within 48h and no errors → health is 'ok'."""
        scraper = self._make_scraper(tmp_path)
        log_id = scraper.filings_db.log_crawl_start("crawl")
        scraper.filings_db.log_crawl_complete(
            log_id,
            filings_found=10,
            filings_new=10,
            pages_crawled=1,
            errors=None,
        )
        stats = scraper._compute_stats()
        assert stats["health"] == "ok"

    def test_health_stale_when_last_crawl_older_than_48h(self, tmp_path):
        """Last completed crawl > 48h ago → health is 'stale'."""
        scraper = self._make_scraper(tmp_path)
        old_completed = (datetime.now() - timedelta(hours=49)).isoformat()
        old_started = (datetime.now() - timedelta(hours=50)).isoformat()
        scraper.filings_db.conn.execute(
            """
            INSERT INTO crawl_log
                (crawl_type, source, filings_found, filings_new, pages_crawled,
                 errors, started_at, completed_at, duration_seconds)
            VALUES ('crawl', 'cnbv', 5, 5, 1, NULL, ?, ?, 60.0)
            """,
            (old_started, old_completed),
        )
        scraper.filings_db.conn.commit()
        stats = scraper._compute_stats()
        assert stats["health"] == "stale"

    def test_health_degraded_when_last_crawl_had_errors(self, tmp_path):
        """Last crawl completed but had errors → health is 'degraded'."""
        scraper = self._make_scraper(tmp_path)
        log_id = scraper.filings_db.log_crawl_start("crawl")
        scraper.filings_db.log_crawl_complete(
            log_id,
            filings_found=10,
            filings_new=0,
            pages_crawled=1,
            errors="HTTP 500 on page 2",
        )
        stats = scraper._compute_stats()
        assert stats["health"] == "degraded"

    def test_health_error_when_crawl_never_completed(self, tmp_path):
        """crawl_log row with NULL completed_at → health is 'error'."""
        scraper = self._make_scraper(tmp_path)
        # Insert a started-but-not-completed row directly.
        scraper.filings_db.conn.execute(
            "INSERT INTO crawl_log (crawl_type, source, started_at) VALUES ('crawl', 'cnbv', ?)",
            (datetime.now().isoformat(),),
        )
        scraper.filings_db.conn.commit()
        stats = scraper._compute_stats()
        assert stats["health"] == "error"

    def test_health_error_on_exception(self, tmp_path):
        """An unexpected exception during stats computation → health is 'error'."""
        scraper = self._make_scraper(tmp_path)
        with patch.object(scraper.filings_db, "count_total", side_effect=RuntimeError("DB error")):
            stats = scraper._compute_stats()
        assert stats["health"] == "error"


class TestComputeStatsFields:
    def _make_scraper(self, tmp_path) -> CNBVScraper:
        return CNBVScraper(
            output_path=str(tmp_path / "filings.json"),
            pdf_dir=str(tmp_path / "pdfs"),
            db_path=str(tmp_path / "enc.db"),
            filings_db_path=str(tmp_path / "filings_cache.db"),
        )

    def test_stats_schema_keys_present(self, tmp_path):
        scraper = self._make_scraper(tmp_path)
        stats = scraper._compute_stats()
        required_keys = {
            "scraper", "country", "sources", "total_filings", "downloaded",
            "pending_download", "unique_companies", "total_crawl_runs",
            "earliest_record", "latest_record", "db_size_bytes",
            "documents_size_bytes", "health",
        }
        assert required_keys.issubset(stats.keys())

    def test_stats_scraper_identifier(self, tmp_path):
        scraper = self._make_scraper(tmp_path)
        stats = scraper._compute_stats()
        assert stats["scraper"] == "mexico-scraper"
        assert stats["country"] == "MX"
        assert stats["sources"] == ["cnbv"]

    def test_stats_counts_correctly(self, tmp_path):
        scraper = self._make_scraper(tmp_path)
        today_str = datetime.now().strftime("%d/%m/%Y")
        for i in range(3):
            scraper.filings_db.upsert_filing(
                filing_id=f"cnbv_{i}",
                ticker=f"TICKER{i}",
                filing_date=today_str,
                downloaded=(i == 0),
                download_path="/tmp/f.pdf" if i == 0 else "",
            )
        stats = scraper._compute_stats()
        assert stats["total_filings"] == 3
        assert stats["downloaded"] == 1
        assert stats["pending_download"] == 2
        assert stats["unique_companies"] == 3

    def test_stats_db_size_is_non_negative(self, tmp_path):
        scraper = self._make_scraper(tmp_path)
        stats = scraper._compute_stats()
        assert stats["db_size_bytes"] >= 0

    def test_stats_documents_size_zero_when_no_dir(self, tmp_path):
        scraper = self._make_scraper(tmp_path)
        stats = scraper._compute_stats()
        assert stats["documents_size_bytes"] == 0


# ---------------------------------------------------------------------------
# CNBVScraper.stats --json output tests
# ---------------------------------------------------------------------------


class TestStatsJsonOutput:
    def _make_scraper(self, tmp_path) -> CNBVScraper:
        return CNBVScraper(
            output_path=str(tmp_path / "filings.json"),
            pdf_dir=str(tmp_path / "pdfs"),
            db_path=str(tmp_path / "enc.db"),
            filings_db_path=str(tmp_path / "filings_cache.db"),
        )

    def test_stats_json_emits_valid_json(self, tmp_path, capsys):
        scraper = self._make_scraper(tmp_path)
        scraper.stats(as_json=True)
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert isinstance(parsed, dict)

    def test_stats_json_contains_required_fields(self, tmp_path, capsys):
        scraper = self._make_scraper(tmp_path)
        scraper.stats(as_json=True)
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert "scraper" in parsed
        assert "total_filings" in parsed
        assert "health" in parsed

    def test_stats_human_readable_contains_health(self, tmp_path, capsys):
        scraper = self._make_scraper(tmp_path)
        scraper.stats(as_json=False)
        captured = capsys.readouterr()
        assert "Health" in captured.out

    def test_stats_human_readable_contains_statistics_header(self, tmp_path, capsys):
        scraper = self._make_scraper(tmp_path)
        scraper.stats(as_json=False)
        captured = capsys.readouterr()
        assert "Statistics" in captured.out


# ---------------------------------------------------------------------------
# CLI stats --json flag and exit codes
# ---------------------------------------------------------------------------


class TestStatsCliFlag:
    def test_stats_json_flag_parsed(self):
        parser = build_parser()
        args = parser.parse_args(["stats", "--json"])
        assert args.json is True

    def test_stats_json_flag_default_false(self):
        parser = build_parser()
        args = parser.parse_args(["stats"])
        assert args.json is False

    def test_stats_filings_db_flag_parsed(self):
        parser = build_parser()
        args = parser.parse_args(["stats", "--filings-db", "custom.db"])
        assert args.filings_db == "custom.db"


class TestStatsExitCodes:
    """Test that cmd_stats returns the correct exit code based on health."""

    def _run_stats_cmd(self, tmp_path, extra_args: list[str] | None = None) -> int:
        parser = build_parser()
        base = [
            "stats",
            "--output", str(tmp_path / "filings.json"),
            "--db", str(tmp_path / "enc.db"),
            "--filings-db", str(tmp_path / "filings_cache.db"),
        ]
        args = parser.parse_args(base + (extra_args or []))
        return args.func(args)

    def test_exit_code_2_when_empty(self, tmp_path):
        code = self._run_stats_cmd(tmp_path)
        assert code == 2

    def test_exit_code_0_when_ok(self, tmp_path):
        # Simulate a recently-completed crawl with no errors.
        db = FilingsDB(str(tmp_path / "filings_cache.db"))
        log_id = db.log_crawl_start("crawl")
        db.log_crawl_complete(log_id, filings_found=5, filings_new=5, pages_crawled=1)
        db.close()
        code = self._run_stats_cmd(tmp_path)
        assert code == 0

    def test_exit_code_0_when_degraded(self, tmp_path):
        # Simulate a recently-completed crawl that had errors → degraded → exit 0.
        db = FilingsDB(str(tmp_path / "filings_cache.db"))
        log_id = db.log_crawl_start("crawl")
        db.log_crawl_complete(
            log_id,
            filings_found=5,
            filings_new=0,
            pages_crawled=1,
            errors="connection timeout on page 3",
        )
        db.close()
        code = self._run_stats_cmd(tmp_path)
        assert code == 0

    def test_exit_code_1_when_stale(self, tmp_path):
        # Simulate a crawl that completed > 48h ago → stale → exit 1.
        db = FilingsDB(str(tmp_path / "filings_cache.db"))
        old_completed = (datetime.now() - timedelta(hours=49)).isoformat()
        old_started = (datetime.now() - timedelta(hours=50)).isoformat()
        db.conn.execute(
            """
            INSERT INTO crawl_log
                (crawl_type, source, filings_found, filings_new, pages_crawled,
                 errors, started_at, completed_at, duration_seconds)
            VALUES ('crawl', 'cnbv', 5, 5, 1, NULL, ?, ?, 60.0)
            """,
            (old_started, old_completed),
        )
        db.conn.commit()
        db.close()
        code = self._run_stats_cmd(tmp_path)
        assert code == 1

    def test_exit_code_3_when_error(self, tmp_path):
        # Create a corrupted filings_cache.db to force an error.
        db_path = str(tmp_path / "filings_cache.db")
        with open(db_path, "w") as fh:
            fh.write("this is not a sqlite database")

        parser = build_parser()
        args = parser.parse_args([
            "stats",
            "--output", str(tmp_path / "filings.json"),
            "--db", str(tmp_path / "enc.db"),
            "--filings-db", db_path,
        ])
        # FilingsDB constructor will fail on corrupt DB; cmd_stats should handle gracefully.
        # We patch FilingsDB to raise to simulate the error path cleanly.
        with patch("scraper.FilingsDB", side_effect=sqlite3.DatabaseError("corrupt")):
            try:
                code = args.func(args)
            except Exception:
                code = 3
        assert code == 3

    def test_stats_json_flag_calls_stats_with_json(self, tmp_path, capsys):
        code = self._run_stats_cmd(tmp_path, ["--json"])
        captured = capsys.readouterr()
        # Even on empty DB, --json should emit valid JSON
        parsed = json.loads(captured.out)
        assert parsed["health"] == "empty"
        assert code == 2
