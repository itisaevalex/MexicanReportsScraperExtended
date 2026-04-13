"""
CNBV STIV-2 Scraper — Database Layer
=====================================

SQLite schema, dataclasses, and CRUD operations for:
  - EncCache: maps filing integer keys → encrypted enc tokens
  - Filing: structured representation of a scraped filing row

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
