"""
CNBV STIV-2 Scraper — Database Layer
=====================================

SQLite schema, dataclasses, and CRUD operations for:
  - EncCache: maps filing integer keys → encrypted enc tokens
  - Filing: structured representation of a scraped filing row
  - FilingsDB: L3 spec-compliant filings table with ISO date storage

The enc values are deterministic and permanent (AES-encrypted filing IDs
that never change). Caching them eliminates repeated callbackPanel requests
and enables instant parallel downloads on repeat runs.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class Filing:
    """Structured representation of a single CNBV filing row."""

    fecha: str
    emisora: str
    asunto: str
    key: Optional[str] = None
    enc: Optional[str] = None
    pdf_path: Optional[str] = None
    filing_type: Optional[str] = None
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        """Convert to plain dict for JSON serialisation."""
        return {
            "fecha": self.fecha,
            "emisora": self.emisora,
            "asunto": self.asunto,
            "key": self.key,
            "enc": self.enc,
            "pdf_path": self.pdf_path,
            "filing_type": self.filing_type,
            "scraped_at": self.scraped_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Filing":
        """Construct a Filing from a plain dict (e.g. loaded from JSON)."""
        return cls(
            fecha=data.get("fecha", ""),
            emisora=data.get("emisora", ""),
            asunto=data.get("asunto", ""),
            key=data.get("key"),
            enc=data.get("enc"),
            pdf_path=data.get("pdf_path"),
            filing_type=data.get("filing_type"),
            scraped_at=data.get("scraped_at", datetime.now().isoformat()),
        )


# ---------------------------------------------------------------------------
# Enc cache (SQLite)
# ---------------------------------------------------------------------------


class EncCache:
    """
    Persistent cache mapping filing keys to their encrypted enc values.

    The enc values are deterministic and permanent — a key always maps to
    the same enc. This cache eliminates repeated callbackPanel requests.

    Schema:
      filings(key INTEGER PRIMARY KEY, enc TEXT, emisora TEXT,
              asunto TEXT, fecha TEXT, resolved_at TEXT)
    """

    def __init__(self, db_path: str = "enc_cache.db") -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._create_schema()

    def _create_schema(self) -> None:
        """Create the database schema if it does not yet exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS filings (
                key INTEGER PRIMARY KEY,
                enc TEXT NOT NULL,
                emisora TEXT,
                asunto TEXT,
                fecha TEXT,
                resolved_at TEXT
            )
        """)
        self.conn.commit()

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get(self, key: int) -> Optional[str]:
        """Look up cached enc for a key. Returns None if not cached."""
        row = self.conn.execute(
            "SELECT enc FROM filings WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else None

    def get_max_key(self) -> int:
        """Return the highest cached key, or 0 if the cache is empty."""
        row = self.conn.execute("SELECT MAX(key) FROM filings").fetchone()
        return row[0] or 0

    def count(self) -> int:
        """Return the total number of cached entries."""
        row = self.conn.execute("SELECT COUNT(*) FROM filings").fetchone()
        return row[0]

    def get_uncached_keys(self, start: int, end: int) -> list[int]:
        """Return keys in [start, end] that are NOT in the cache."""
        cached = {
            r[0]
            for r in self.conn.execute(
                "SELECT key FROM filings WHERE key BETWEEN ? AND ?",
                (start, end),
            ).fetchall()
        }
        return [k for k in range(start, end + 1) if k not in cached]

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def put(
        self,
        key: int,
        enc: str,
        emisora: str = "",
        asunto: str = "",
        fecha: str = "",
    ) -> None:
        """Cache an enc value for a key (upsert)."""
        self.conn.execute(
            """INSERT OR REPLACE INTO filings
               (key, enc, emisora, asunto, fecha, resolved_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (key, enc, emisora, asunto, fecha, datetime.now().isoformat()),
        )
        self.conn.commit()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self.conn.close()


# ---------------------------------------------------------------------------
# Date normalization helper
# ---------------------------------------------------------------------------


def normalize_date(date_str: str) -> str:
    """Normalize a date string to ISO 8601 YYYY-MM-DD format.

    CNBV stores dates in DD/MM/YYYY format.  This function converts them
    to the canonical YYYY-MM-DD format required by the L3 spec.

    Args:
        date_str: Date string.  Accepted formats:
            - ``DD/MM/YYYY`` (CNBV native format)
            - ``YYYY-MM-DD`` (already normalised; returned unchanged)
            - Any other string is returned unchanged.

    Returns:
        ISO 8601 ``YYYY-MM-DD`` string when conversion succeeds; the
        original string otherwise.

    Examples:
        >>> normalize_date("15/03/2026")
        '2026-03-15'
        >>> normalize_date("2026-03-15")
        '2026-03-15'
        >>> normalize_date("not-a-date")
        'not-a-date'
    """
    if not date_str:
        return date_str
    # Already ISO
    if len(date_str) >= 10 and date_str[4] == "-" and date_str[7] == "-":
        return date_str[:10]
    # DD/MM/YYYY
    if len(date_str) >= 10 and date_str[2] == "/" and date_str[5] == "/":
        try:
            day, month, year = date_str[:10].split("/")
            return f"{year}-{month}-{day}"
        except (ValueError, IndexError):
            return date_str
    return date_str


# ---------------------------------------------------------------------------
# L3 spec-compliant filings table
# ---------------------------------------------------------------------------


class FilingsDB:
    """L3 spec-compliant persistent store for scraped filings.

    Manages the ``filings`` table with the canonical cross-country schema.
    Dates are stored in ISO 8601 (YYYY-MM-DD).  The enc token cache table
    is preserved separately in the ``EncCache`` class.

    Schema (``filings`` table):
        filing_id TEXT PRIMARY KEY
        source TEXT DEFAULT 'cnbv'
        country TEXT DEFAULT 'MX'
        ticker TEXT
        company_name TEXT
        filing_date TEXT           -- YYYY-MM-DD
        filing_time TEXT
        headline TEXT
        filing_type TEXT DEFAULT 'other'
        category TEXT
        document_url TEXT
        direct_download_url TEXT
        file_size TEXT
        num_pages INTEGER
        price_sensitive BOOLEAN DEFAULT FALSE
        downloaded BOOLEAN DEFAULT FALSE
        download_path TEXT
        raw_metadata TEXT
        created_at TEXT
    """

    _CURRENT_SCHEMA_VERSION = 1

    def __init__(self, db_path: str = "filings_cache.db") -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_schema()
        self._run_migrations()

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def _create_schema(self) -> None:
        """Create the filings table and schema_version table if absent."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS filings (
                filing_id           TEXT PRIMARY KEY,
                source              TEXT DEFAULT 'cnbv',
                country             TEXT DEFAULT 'MX',
                ticker              TEXT,
                company_name        TEXT,
                filing_date         TEXT,
                filing_time         TEXT,
                headline            TEXT,
                filing_type         TEXT DEFAULT 'other',
                category            TEXT,
                document_url        TEXT,
                direct_download_url TEXT,
                file_size           TEXT,
                num_pages           INTEGER,
                price_sensitive     BOOLEAN DEFAULT FALSE,
                downloaded          BOOLEAN DEFAULT FALSE,
                download_path       TEXT,
                raw_metadata        TEXT,
                created_at          TEXT
            );
        """)
        self.conn.commit()

    def _run_migrations(self) -> None:
        """Apply backwards-compatible schema migrations.

        Each migration is idempotent — it uses ``ALTER TABLE … ADD COLUMN``
        with ``IF NOT EXISTS`` semantics via a try/except guard so that
        running against an already-migrated database is safe.
        """
        row = self.conn.execute(
            "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
        ).fetchone()
        current = row[0] if row else 0

        if current < 1:
            # Migration 1: ensure all L3 columns exist on pre-existing tables.
            _optional_columns: list[tuple[str, str]] = [
                ("source", "TEXT DEFAULT 'cnbv'"),
                ("country", "TEXT DEFAULT 'MX'"),
                ("ticker", "TEXT"),
                ("company_name", "TEXT"),
                ("filing_date", "TEXT"),
                ("filing_time", "TEXT"),
                ("headline", "TEXT"),
                ("filing_type", "TEXT DEFAULT 'other'"),
                ("category", "TEXT"),
                ("document_url", "TEXT"),
                ("direct_download_url", "TEXT"),
                ("file_size", "TEXT"),
                ("num_pages", "INTEGER"),
                ("price_sensitive", "BOOLEAN DEFAULT FALSE"),
                ("downloaded", "BOOLEAN DEFAULT FALSE"),
                ("download_path", "TEXT"),
                ("raw_metadata", "TEXT"),
                ("created_at", "TEXT"),
            ]
            for col_name, col_def in _optional_columns:
                try:
                    self.conn.execute(
                        f"ALTER TABLE filings ADD COLUMN {col_name} {col_def}"
                    )
                except sqlite3.OperationalError:
                    # Column already exists — safe to ignore.
                    pass
            self.conn.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (1)"
            )
            self.conn.commit()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def upsert_filing(
        self,
        filing_id: str,
        ticker: str = "",
        company_name: str = "",
        filing_date: str = "",
        filing_time: str = "",
        headline: str = "",
        filing_type: str = "other",
        category: str = "",
        document_url: str = "",
        direct_download_url: str = "",
        file_size: str = "",
        num_pages: Optional[int] = None,
        price_sensitive: bool = False,
        downloaded: bool = False,
        download_path: str = "",
        raw_metadata: str = "",
    ) -> None:
        """Insert or replace a filing record (upsert by filing_id).

        Dates are normalised to YYYY-MM-DD before storage.

        Args:
            filing_id: Unique identifier for this filing.
            ticker: Emisora / ticker code.
            company_name: Full company name.
            filing_date: Date of filing (DD/MM/YYYY or YYYY-MM-DD).
            filing_time: Time portion of the filing (HH:MM or similar).
            headline: Filing subject / headline text.
            filing_type: Normalised type string (e.g. "annual_report").
            category: Raw category from the source.
            document_url: URL for the filing detail page.
            direct_download_url: Direct URL for the document file.
            file_size: Human-readable file size string.
            num_pages: Number of pages in the document.
            price_sensitive: Whether the filing is price sensitive.
            downloaded: Whether the document has been downloaded.
            download_path: Local path to the downloaded document.
            raw_metadata: JSON-serialised raw row data.
        """
        iso_date = normalize_date(filing_date)
        now = datetime.now().isoformat()
        self.conn.execute(
            """
            INSERT OR REPLACE INTO filings (
                filing_id, source, country, ticker, company_name,
                filing_date, filing_time, headline, filing_type, category,
                document_url, direct_download_url, file_size, num_pages,
                price_sensitive, downloaded, download_path, raw_metadata, created_at
            ) VALUES (
                ?, 'cnbv', 'MX', ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                filing_id, ticker, company_name,
                iso_date, filing_time, headline, filing_type, category,
                document_url, direct_download_url, file_size, num_pages,
                price_sensitive, downloaded, download_path, raw_metadata, now,
            ),
        )
        self.conn.commit()

    def mark_downloaded(self, filing_id: str, download_path: str) -> None:
        """Mark a filing as downloaded and record its local path.

        Args:
            filing_id: Filing to update.
            download_path: Local filesystem path of the downloaded document.
        """
        self.conn.execute(
            "UPDATE filings SET downloaded = TRUE, download_path = ? WHERE filing_id = ?",
            (download_path, filing_id),
        )
        self.conn.commit()

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_filing(self, filing_id: str) -> Optional[sqlite3.Row]:
        """Fetch a single filing by its ID.

        Args:
            filing_id: Primary key to look up.

        Returns:
            ``sqlite3.Row`` if found, ``None`` otherwise.
        """
        return self.conn.execute(
            "SELECT * FROM filings WHERE filing_id = ?", (filing_id,)
        ).fetchone()

    def count_total(self) -> int:
        """Return total number of filings in the database."""
        row = self.conn.execute("SELECT COUNT(*) FROM filings").fetchone()
        return row[0]

    def count_downloaded(self) -> int:
        """Return number of filings marked as downloaded."""
        row = self.conn.execute(
            "SELECT COUNT(*) FROM filings WHERE downloaded = TRUE"
        ).fetchone()
        return row[0]

    def count_unique_companies(self) -> int:
        """Return number of distinct ticker/company values."""
        row = self.conn.execute(
            "SELECT COUNT(DISTINCT ticker) FROM filings WHERE ticker IS NOT NULL AND ticker != ''"
        ).fetchone()
        return row[0]

    def get_date_range(self) -> tuple[Optional[str], Optional[str]]:
        """Return the earliest and latest filing_date in ISO format.

        Returns:
            Tuple of (earliest, latest) date strings, or (None, None) if empty.
        """
        row = self.conn.execute(
            "SELECT MIN(filing_date), MAX(filing_date) FROM filings"
        ).fetchone()
        if row:
            return row[0], row[1]
        return None, None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self.conn.close()
