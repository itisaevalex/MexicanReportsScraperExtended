"""
Unit tests for db.py

Tests cover:
  - EncCache CRUD operations
  - EncCache schema creation (idempotent)
  - EncCache.get_uncached_keys
  - Filing dataclass serialisation
  - FilingsDB.upsert_filing isin column (migration 3)
"""

from __future__ import annotations

import pytest

from db import EncCache, Filing, FilingsDB


# ---------------------------------------------------------------------------
# EncCache unit tests
# ---------------------------------------------------------------------------


class TestEncCacheGet:
    def test_miss_returns_none(self, tmp_db: EncCache):
        assert tmp_db.get(9999) is None

    def test_hit_returns_enc(self, tmp_db: EncCache):
        tmp_db.put(42, "ENC_VALUE_42", emisora="FEMSA", asunto="Informe Anual")
        assert tmp_db.get(42) == "ENC_VALUE_42"

    def test_different_keys_are_independent(self, tmp_db: EncCache):
        tmp_db.put(1, "ENC_1")
        tmp_db.put(2, "ENC_2")
        assert tmp_db.get(1) == "ENC_1"
        assert tmp_db.get(2) == "ENC_2"


class TestEncCachePut:
    def test_upsert_overwrites_existing(self, tmp_db: EncCache):
        tmp_db.put(100, "OLD_ENC")
        tmp_db.put(100, "NEW_ENC")
        assert tmp_db.get(100) == "NEW_ENC"

    def test_stores_metadata(self, tmp_db: EncCache):
        tmp_db.put(200, "ENC_200", emisora="BIMBO", asunto="Q4", fecha="01/01/2026")
        # Verify it's in the database at all
        assert tmp_db.get(200) == "ENC_200"

    def test_count_increments(self, tmp_db: EncCache):
        before = tmp_db.count()
        tmp_db.put(300, "ENC_300")
        assert tmp_db.count() == before + 1


class TestEncCacheCount:
    def test_empty_cache(self, tmp_db: EncCache):
        assert tmp_db.count() == 0

    def test_count_after_inserts(self, tmp_db: EncCache):
        tmp_db.put(1, "A")
        tmp_db.put(2, "B")
        tmp_db.put(3, "C")
        assert tmp_db.count() == 3

    def test_count_does_not_grow_on_upsert(self, tmp_db: EncCache):
        tmp_db.put(1, "A")
        tmp_db.put(1, "B")  # upsert
        assert tmp_db.count() == 1


class TestEncCacheGetMaxKey:
    def test_empty_cache_returns_zero(self, tmp_db: EncCache):
        assert tmp_db.get_max_key() == 0

    def test_returns_highest_key(self, tmp_db: EncCache):
        tmp_db.put(10, "E10")
        tmp_db.put(50, "E50")
        tmp_db.put(30, "E30")
        assert tmp_db.get_max_key() == 50


class TestEncCacheGetUncachedKeys:
    def test_all_uncached_when_empty(self, tmp_db: EncCache):
        uncached = tmp_db.get_uncached_keys(1, 5)
        assert uncached == [1, 2, 3, 4, 5]

    def test_excludes_cached_keys(self, tmp_db: EncCache):
        tmp_db.put(2, "ENC_2")
        tmp_db.put(4, "ENC_4")
        uncached = tmp_db.get_uncached_keys(1, 5)
        assert uncached == [1, 3, 5]

    def test_all_cached_returns_empty_list(self, tmp_db: EncCache):
        for k in range(1, 4):
            tmp_db.put(k, f"ENC_{k}")
        uncached = tmp_db.get_uncached_keys(1, 3)
        assert uncached == []

    def test_single_key_range(self, tmp_db: EncCache):
        assert tmp_db.get_uncached_keys(7, 7) == [7]
        tmp_db.put(7, "ENC_7")
        assert tmp_db.get_uncached_keys(7, 7) == []


class TestEncCacheSchemaIdempotent:
    def test_second_open_does_not_raise(self, tmp_path):
        """Opening the same DB file twice should not raise (CREATE IF NOT EXISTS)."""
        db_path = str(tmp_path / "idempotent.db")
        c1 = EncCache(db_path)
        c1.put(1, "A")
        c1.close()
        c2 = EncCache(db_path)
        assert c2.get(1) == "A"
        c2.close()


# ---------------------------------------------------------------------------
# Filing dataclass tests
# ---------------------------------------------------------------------------


class TestFilingDataclass:
    def test_to_dict_round_trip(self):
        f = Filing(
            fecha="15/03/2026",
            emisora="FEMSA",
            asunto="Informe Anual 2025",
            key="453816",
            enc="ENCVAL",
            pdf_path="/tmp/doc.pdf",
            filing_type="annual_report",
        )
        d = f.to_dict()
        assert d["fecha"] == "15/03/2026"
        assert d["emisora"] == "FEMSA"
        assert d["asunto"] == "Informe Anual 2025"
        assert d["key"] == "453816"
        assert d["enc"] == "ENCVAL"
        assert d["pdf_path"] == "/tmp/doc.pdf"
        assert d["filing_type"] == "annual_report"

    def test_from_dict_round_trip(self):
        original = Filing(
            fecha="15/03/2026",
            emisora="BIMBO",
            asunto="Q4 Results",
            key="453815",
        )
        restored = Filing.from_dict(original.to_dict())
        assert restored.fecha == original.fecha
        assert restored.emisora == original.emisora
        assert restored.asunto == original.asunto
        assert restored.key == original.key

    def test_optional_fields_default_to_none(self):
        f = Filing(fecha="01/01/2026", emisora="WALMEX", asunto="Annual")
        assert f.key is None
        assert f.enc is None
        assert f.pdf_path is None
        assert f.filing_type is None

    def test_scraped_at_auto_populated(self):
        f = Filing(fecha="01/01/2026", emisora="WALMEX", asunto="Annual")
        assert f.scraped_at is not None
        assert len(f.scraped_at) > 0

    def test_from_dict_with_missing_optional_fields(self):
        minimal = {"fecha": "01/01/2026", "emisora": "TELMEX", "asunto": "Update"}
        f = Filing.from_dict(minimal)
        assert f.emisora == "TELMEX"
        assert f.key is None
        assert f.enc is None


# ---------------------------------------------------------------------------
# FilingsDB isin column tests (migration 3)
# ---------------------------------------------------------------------------


class TestFilingsDBIsin:
    def test_upsert_with_isin_persists_value(self, tmp_path):
        db = FilingsDB(str(tmp_path / "filings.db"))
        db.upsert_filing(
            filing_id="cnbv_1",
            ticker="FEMSA",
            isin="MX01FE100003",
            filing_date="01/01/2026",
            headline="Informe Anual",
        )
        row = db.get_filing("cnbv_1")
        assert row is not None
        assert row["isin"] == "MX01FE100003"
        db.close()

    def test_upsert_without_isin_stores_none(self, tmp_path):
        db = FilingsDB(str(tmp_path / "filings.db"))
        db.upsert_filing(
            filing_id="cnbv_2",
            ticker="BIMBO",
            filing_date="02/01/2026",
            headline="Reporte Trimestral",
        )
        row = db.get_filing("cnbv_2")
        assert row is not None
        assert row["isin"] is None
        db.close()

    def test_isin_column_present_in_schema(self, tmp_path):
        """The isin column must exist in the filings table after migrations."""
        db = FilingsDB(str(tmp_path / "filings.db"))
        columns = [
            r[1]
            for r in db.conn.execute("PRAGMA table_info(filings)").fetchall()
        ]
        assert "isin" in columns
        db.close()

    def test_schema_migrates_existing_db_to_add_isin(self, tmp_path):
        """Opening a schema-v2 database should apply migration 3 without error."""
        import sqlite3

        db_path = str(tmp_path / "old.db")
        # Create a v2 schema manually (no isin column)
        conn = sqlite3.connect(db_path)
        conn.executescript("""
            CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
            INSERT INTO schema_version VALUES (2);
            CREATE TABLE filings (
                filing_id TEXT PRIMARY KEY,
                source TEXT DEFAULT 'cnbv',
                country TEXT DEFAULT 'MX',
                ticker TEXT,
                company_name TEXT,
                filing_date TEXT,
                filing_time TEXT,
                headline TEXT,
                filing_type TEXT DEFAULT 'other',
                category TEXT,
                document_url TEXT,
                direct_download_url TEXT,
                file_size TEXT,
                num_pages INTEGER,
                price_sensitive BOOLEAN DEFAULT FALSE,
                downloaded BOOLEAN DEFAULT FALSE,
                download_path TEXT,
                raw_metadata TEXT,
                created_at TEXT
            );
            CREATE TABLE crawl_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                crawl_type TEXT NOT NULL,
                source TEXT DEFAULT 'cnbv',
                query_params TEXT,
                filings_found INTEGER DEFAULT 0,
                filings_new INTEGER DEFAULT 0,
                pages_crawled INTEGER DEFAULT 0,
                errors TEXT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                duration_seconds REAL
            );
        """)
        conn.commit()
        conn.close()

        # Now open via FilingsDB — migration 3 should add isin column
        db = FilingsDB(db_path)
        columns = [
            r[1]
            for r in db.conn.execute("PRAGMA table_info(filings)").fetchall()
        ]
        assert "isin" in columns
        db.close()

    def test_upsert_updates_isin_on_repeat_call(self, tmp_path):
        """Upserting the same filing_id should update the isin value."""
        db = FilingsDB(str(tmp_path / "filings.db"))
        db.upsert_filing(filing_id="cnbv_3", ticker="AC", isin=None)
        db.upsert_filing(filing_id="cnbv_3", ticker="AC", isin="MX01AC100006")
        row = db.get_filing("cnbv_3")
        assert row["isin"] == "MX01AC100006"
        db.close()
