"""BIVA ISIN lookup with JSON file cache for the CNBV scraper.

Provides a single public function :func:`load_isin_map` that returns a
``{emisora_code: isin}`` dict.  The result is written to a local JSON cache
file so that subsequent runs skip the ~140-second bulk download entirely.

Cache file: ``_biva_isin_cache.json`` (next to the caller's working directory).

Failure behaviour
-----------------
Any network error during the BIVA fetch is caught, logged as a WARNING, and
an empty dict is returned so the caller can continue without ISINs.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Optional

import requests

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# BIVA API constants (mirrored from enricher/exchanges/bmv.py)
# ---------------------------------------------------------------------------

_BIVA_BASE = "https://www.biva.mx"
_COMPANIES_URL = f"{_BIVA_BASE}/emisoras/empresas"
_EMISIONES_URL = f"{_BIVA_BASE}/emisoras/empresas/{{biva_id}}/emisiones"

_REQUEST_TIMEOUT = 20  # seconds per request
_PAGE_SIZE = 200       # BIVA returns up to 200 companies per page
_INTER_REQUEST_DELAY = 0.4  # seconds between per-company calls

_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Referer": "https://www.biva.mx/",
}

_EQUITY_MARKER = "ACCION"
_DEFAULT_CACHE_PATH = "_biva_isin_cache.json"


# ---------------------------------------------------------------------------
# Internal helpers — fetching
# ---------------------------------------------------------------------------


def _fetch_companies_page(session: requests.Session, page: int) -> list[dict]:
    """Fetch one page of the BIVA company list.

    Args:
        session: Configured requests.Session.
        page: Zero-based page index.

    Returns:
        List of raw company dicts from ``content``.  Empty on any error.
    """
    try:
        response = session.get(
            _COMPANIES_URL,
            params={"page": page, "size": _PAGE_SIZE},
            headers=_HEADERS,
            timeout=_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        log.warning("BIVA: company list page %d fetch failed: %s", page, exc)
        return []

    if not isinstance(payload, dict):
        log.warning("BIVA: unexpected response type on page %d: %s", page, type(payload))
        return []

    content = payload.get("content", [])
    return content if isinstance(content, list) else []


def _fetch_all_companies(session: requests.Session) -> list[dict]:
    """Fetch all BIVA company records across all pages.

    Args:
        session: Configured requests.Session.

    Returns:
        List of raw company dicts.
    """
    companies: list[dict] = []
    page = 0
    while True:
        page_items = _fetch_companies_page(session, page)
        if not page_items:
            break
        companies.extend(page_items)
        if len(page_items) < _PAGE_SIZE:
            break  # last page
        page += 1
    return companies


def _fetch_equity_isin(session: requests.Session, biva_id: int, clave: str) -> Optional[str]:
    """Fetch the primary equity ISIN for one BIVA company.

    Calls the emisiones endpoint, filters for rows whose ``tipoValor``
    contains "ACCION", and returns the first matching ISIN.

    Args:
        session: Configured requests.Session.
        biva_id: Numeric company ID from the BIVA company list.
        clave: EMISORA code (used only for debug logging).

    Returns:
        ISIN string or None if no equity emision found (or on any error).
    """
    url = _EMISIONES_URL.format(biva_id=biva_id)
    try:
        response = session.get(
            url,
            params={"size": 50, "page": 0, "cotizacion": "true"},
            headers=_HEADERS,
            timeout=_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        log.debug("BIVA: emisiones fetch failed for %s (id=%d): %s", clave, biva_id, exc)
        return None

    if not isinstance(payload, dict):
        return None

    for item in payload.get("content", []) or []:
        isin = str(item.get("isin", "")).strip()
        tipo_valor = str(item.get("tipoValor", "")).strip().upper()
        if isin and _EQUITY_MARKER in tipo_valor:
            return isin

    return None


# ---------------------------------------------------------------------------
# Internal helpers — cache I/O
# ---------------------------------------------------------------------------


def _load_cache(cache_path: str) -> Optional[dict[str, str]]:
    """Load the ISIN map from the JSON cache file if it exists and is valid.

    Args:
        cache_path: Filesystem path to the JSON cache file.

    Returns:
        The ``{clave: isin}`` dict from disk, or None if the file is absent
        or cannot be parsed.
    """
    path = Path(cache_path)
    if not path.exists():
        return None
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return data
        log.warning("BIVA cache file %s has unexpected format; ignoring", cache_path)
        return None
    except Exception as exc:
        log.warning("BIVA: could not read cache file %s: %s", cache_path, exc)
        return None


def _save_cache(isin_map: dict[str, str], cache_path: str) -> None:
    """Persist the ISIN map to the JSON cache file.

    Silently swallows I/O errors — a failed cache write should never abort
    the scraper run.

    Args:
        isin_map: The ``{clave: isin}`` mapping to persist.
        cache_path: Filesystem path for the JSON cache file.
    """
    try:
        with open(cache_path, "w", encoding="utf-8") as fh:
            json.dump(isin_map, fh, ensure_ascii=False, indent=2)
        log.debug("BIVA: cache written to %s (%d entries)", cache_path, len(isin_map))
    except Exception as exc:
        log.warning("BIVA: could not write cache file %s: %s", cache_path, exc)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_isin_map(session: requests.Session) -> dict[str, str]:
    """Download the full BIVA company+emisiones dataset and build the ISIN map.

    Makes one HTTP request per company (up to ~349 calls with a 0.4s delay
    between each).  Typical wall-clock time is ~140 seconds on a fresh run.

    Args:
        session: Configured requests.Session.

    Returns:
        ``{emisora_code: isin}`` dict.  Empty on complete failure (never raises).
    """
    raw_companies = _fetch_all_companies(session)
    if not raw_companies:
        log.warning("BIVA: company list is empty — cannot build ISIN map")
        return {}

    result: dict[str, str] = {}

    for i, raw in enumerate(raw_companies):
        try:
            biva_id = int(raw["id"])
            clave = str(raw.get("clave", "")).strip().upper()
        except (KeyError, TypeError, ValueError):
            continue

        if not clave:
            continue

        isin = _fetch_equity_isin(session, biva_id, clave)
        if isin:
            result[clave] = isin
            log.debug("BIVA: %s -> %s", clave, isin)
        else:
            log.debug("BIVA: no equity ISIN for %s (id=%d)", clave, biva_id)

        if i < len(raw_companies) - 1:
            time.sleep(_INTER_REQUEST_DELAY)

    log.info(
        "BIVA: lookup table built — %d / %d companies resolved",
        len(result),
        len(raw_companies),
    )
    return result


def load_isin_map(
    session: requests.Session,
    cache_path: str = _DEFAULT_CACHE_PATH,
    force_refresh: bool = False,
) -> dict[str, str]:
    """Return the ``{emisora_code: isin}`` map, using the JSON cache when available.

    Cache hit path (fast):
      Reads ``_biva_isin_cache.json`` and returns immediately — no HTTP calls.

    Cache miss / forced refresh path (slow, ~140 s):
      Downloads the full BIVA company+emisiones dataset, then persists the
      result to ``cache_path`` for future runs.

    On any BIVA API failure the function logs a WARNING and returns ``{}``,
    so the caller can continue with ``isin=None`` for all filings.

    Args:
        session: Configured requests.Session (used only on cache miss).
        cache_path: Path for the JSON cache file.
        force_refresh: When True, skip the cache and re-download from BIVA.

    Returns:
        ``{emisora_code: isin}`` dict; may be empty on failure.
    """
    if not force_refresh:
        cached = _load_cache(cache_path)
        if cached is not None:
            log.info(
                "BIVA: loaded %d ISIN entries from cache %s",
                len(cached),
                cache_path,
            )
            return cached

    log.info(
        "BIVA: cache not found or refresh requested — downloading ISIN map "
        "(~140s for ~349 companies at 0.4s/call)..."
    )

    try:
        isin_map = build_isin_map(session)
    except Exception as exc:
        log.warning("BIVA: ISIN map download failed: %s — continuing without ISINs", exc)
        return {}

    if isin_map:
        _save_cache(isin_map, cache_path)

    return isin_map
