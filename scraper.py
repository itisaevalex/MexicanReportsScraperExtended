"""
CNBV STIV-2 Mexican Financial Filings Scraper
==============================================

Scrapes the Mexican regulatory database (STIV-2) at:
  https://stivconsultasexternas.cnbv.gob.mx/ConsultaInformacionEmisoras.aspx

Extracts the filings table (Date, Emisora/Issuer, Asunto/Event) into a
structured JSON file, and downloads the associated PDF/XLS documents for
each filing. Supports full pagination across all result pages.

Architecture:
  - Uses raw HTTP requests (requests + BeautifulSoup) — no headless browsers.
  - Reverse-engineers the ASP.NET WebForms + DevExpress postback protocol.
  - The search is a synchronous form POST with the BUSCAR button.
  - Filing detail / PDF download uses the Detalle.aspx?enc= endpoint.
  - The enc parameter is obtained via a DevExpress callbackPanel callback.

Protocol notes (confirmed via Playwright browser-level network capture, 2026-04-03):
  The ASPxClientCallbackPanel.PerformCallback(key) JavaScript method prepends
  a 'c0:' prefix to the argument before sending the XHR.  The server's
  callback handler expects __CALLBACKPARAM = 'c0:<key>', and uses the prefix
  to determine the callback type.  Without the prefix, the handler calls
  Substring() on an empty/null string, producing the .NET exception:
      "Length cannot be less than zero. Parameter name: length"
  Sending 'c0:<key>' resolves this and the server returns a valid popup HTML
  fragment containing a Detalle.aspx?enc=<AES-blob> URL.

Module layout:
  scraper.py   — CLI (4 subcommands) + CNBVScraper orchestrator
  db.py        — SQLite schema, EncCache, Filing dataclass
  parsers.py   — ViewState/delta/DX response parsing, type classification
  downloader.py — download_pdf_with_enc, parallel batch downloader
  http_utils.py — session factory, safe_get/safe_post, DX callback helpers

Usage:
  python scraper.py crawl   [--max-pages N] [--no-download] [--output FILE]
  python scraper.py monitor [--interval SECS] [--start-key N]
  python scraper.py export  [--output FILE]
  python scraper.py stats
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

import requests
from bs4 import BeautifulSoup

from db import EncCache, FilingsDB, normalize_date
from isin_cache import load_isin_map
from downloader import (
    attempt_pdf_download,
    download_batch_parallel,
    download_pdf_with_enc,
)
from http_utils import (
    BASE_URL,
    BROWSER_HEADERS,
    DETALLE_URL,
    PAGE_URL,
    REQUEST_DELAY,
    build_gv_callback_param,
    get_filing_enc,
    make_session,
    resolve_enc_batch,
    safe_get,
    safe_post,
    to_dx_epoch_ms,
)
from parsers import (
    classify_filing_type,
    extract_hidden_fields,
    get_total_pages,
    parse_dx_grid_response,
    parse_filings_from_delta,
    parse_filings_grid,
    update_hidden_from_delta,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log = logging.getLogger(__name__)


def _setup_logging(log_file: str | None = None, level: int = logging.INFO) -> None:
    """Configure root logger with optional file sink.

    Args:
        log_file: Optional path to a log file; None means console-only.
        level: Logging level (default INFO).
    """
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
        force=True,
    )


# ---------------------------------------------------------------------------
# Scraper orchestrator
# ---------------------------------------------------------------------------


class CNBVScraper:
    """Scrapes filings from the CNBV STIV-2 portal.

    This class is the stateful orchestrator that wires together the HTTP
    utilities, parsers, cache, and downloader modules.  All ASP.NET
    ViewState and session state lives here.

    Attributes:
        output_path: Path to the JSON output file.
        pdf_dir: Directory for downloaded documents.
        max_pages: Maximum pages to paginate (0 = first page only).
        period: ComboPeriodo value (0–4).
        download_docs: Whether to download documents.
        parallel_workers: Number of parallel download workers.
        session: The main requests Session (holds ASP.NET_SessionId).
        hidden_fields: Current page hidden fields (__VIEWSTATE etc.).
        search_fields: Form fields from the last search (for pagination).
        cache: EncCache SQLite instance.
    """

    def __init__(
        self,
        output_path: str = "filings.json",
        pdf_dir: str = "pdfs",
        max_pages: int = 0,
        period: str = "2",
        download_docs: bool = True,
        incremental: bool = False,
        resume: bool = False,
        db_path: str = "enc_cache.db",
        filings_db_path: str = "filings_cache.db",
        with_isin: bool = False,
        isin_cache_path: str = "_biva_isin_cache.json",
    ) -> None:
        self.output_path = output_path
        self.pdf_dir = pdf_dir
        self.max_pages = max_pages
        self.period = period
        self.download_docs = download_docs
        self.incremental = incremental
        self.resume = resume
        self.with_isin = with_isin
        self.isin_cache_path = isin_cache_path
        self.parallel_workers = 1
        self.session: requests.Session = make_session()
        self.hidden_fields: dict[str, str] = {}
        self.search_fields: dict[str, str] = {}
        self.cache = EncCache(db_path)
        self.filings_db = FilingsDB(filings_db_path)
        # Populated by _load_isin_map() during run() when with_isin=True.
        self.isin_map: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Step 1: Session initialisation
    # ------------------------------------------------------------------

    def initialize(self) -> None:
        """GET the initial page to establish session and extract form state.

        Must be called before any search or callback operation so that the
        session holds a valid ASP.NET_SessionId and __VIEWSTATE.
        """
        log.info("Fetching initial page: %s", PAGE_URL)
        resp = safe_get(self.session, PAGE_URL, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        self.hidden_fields = extract_hidden_fields(soup)

        session_id = self.session.cookies.get("ASP.NET_SessionId", "")
        vs_len = len(self.hidden_fields.get("__VIEWSTATE", ""))
        log.info(
            "Session established (SessionId=%s, ViewState=%d chars)",
            session_id[:8] + "..." if session_id else "(none)",
            vs_len,
        )

    # ------------------------------------------------------------------
    # Step 2: Search + paginate
    # ------------------------------------------------------------------

    def search_filings(
        self,
        period: str = "2",
        max_pages: int = 0,
    ) -> list[dict[str, Any]]:
        """POST the search form and parse the results grid.

        Performs the main BUSCAR async postback, parses the first page,
        then paginates via DevExpress GridView callbacks up to *max_pages*.

        The search fields are preserved in ``self.search_fields`` so that
        subsequent pagination callbacks can include them.

        IMPORTANT: If *download_docs* is True, documents are resolved and
        downloaded BEFORE paginating to the next page (download-before-paginate
        invariant) because the server is stateful and session state may be
        invalidated on pagination.

        Args:
            period: ComboPeriodo value.
              0=Todos, 1=Último, 2=Últimos 6 meses, 3=Año en curso, 4=Hoy
            max_pages: Max pages to retrieve. 0 or 1 = first page only.

        Returns:
            List of filing dicts with keys: fecha, emisora, asunto, key.
        """
        log.info(
            "Searching filings (period=%s, max_pages=%s)...",
            period,
            max_pages or "1",
        )

        today = datetime.now()
        desde = today - timedelta(days=180)

        self.search_fields = {
            "ctl00$DefaultPlaceholder$ComboPeriodo": period,
            "ctl00$DefaultPlaceholder$ComboTipoInformacion": "0",
            "ctl00$DefaultPlaceholder$ComboEmisoras": "",
            "ctl00$DefaultPlaceholder$ComboEmisoras$DDD$L": "",
            "ctl00$DefaultPlaceholder$DateDesde": desde.strftime("%d/%m/%Y"),
            "ctl00$DefaultPlaceholder$DateHasta": today.strftime("%d/%m/%Y"),
            "DefaultPlaceholder_DateDesde_Raw": to_dx_epoch_ms(desde),
            "DefaultPlaceholder_DateHasta_Raw": to_dx_epoch_ms(today),
            "ctl00$DefaultPlaceholder$ComboFiltroPersonalizado": "20",
            "ctl00_DefaultPlaceholder_ComboFiltroPersonalizado_VI": "20",
        }

        # Async UpdatePanel postback (matches browser behavior; required for
        # correct ViewState that enables GridView pagination).
        form_data = {
            **self.hidden_fields,
            **self.search_fields,
            "ctl00$DefaultPlaceholder$ScriptManager1": (
                "ctl00$DefaultPlaceholder$UpdatePanelBusqueda"
                "|ctl00$DefaultPlaceholder$BotonBuscar"
            ),
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__ASYNCPOST": "true",
            "ctl00$DefaultPlaceholder$BotonBuscar": "",
        }

        resp = safe_post(
            self.session,
            PAGE_URL,
            form_data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-MicrosoftAjax": "Delta=true",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": PAGE_URL,
                "Origin": BASE_URL,
            },
            timeout=45,
        )
        resp.raise_for_status()

        # Refresh hidden fields from the async delta response
        self.hidden_fields = update_hidden_from_delta(resp.text, self.hidden_fields)

        all_filings = parse_filings_from_delta(resp.text)

        total_pages, total_count = get_total_pages(resp.text)
        if total_count:
            log.info(
                "Server reports %d total filings across %d pages",
                total_count,
                total_pages,
            )

        # Download current page documents BEFORE paginating (state machine)
        if self.download_docs and all_filings:
            self._download_page_filings(all_filings)

        # Paginate if requested
        if max_pages > 1 and total_pages > 1:
            pages_to_fetch = min(max_pages, total_pages) - 1  # page 1 already done
            for page_idx in range(1, pages_to_fetch + 1):
                time.sleep(REQUEST_DELAY)

                current_keys = [
                    f["key"] for f in all_filings[-20:] if f.get("key")
                ]
                callback_param = build_gv_callback_param(current_keys, page_idx)

                cb_data = {
                    **self.hidden_fields,
                    **self.search_fields,
                    "__CALLBACKID": "ctl00$DefaultPlaceholder$GridViewResultados",
                    "__CALLBACKPARAM": callback_param,
                    "__EVENTTARGET": "",
                    "__EVENTARGUMENT": "",
                }

                resp_page = safe_post(
                    self.session,
                    PAGE_URL,
                    cb_data,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": PAGE_URL,
                    },
                )

                page_filings = parse_dx_grid_response(resp_page.text)
                if not page_filings:
                    log.warning(
                        "No filings on page %d, stopping pagination",
                        page_idx + 1,
                    )
                    break

                # Download THIS page's docs before paginating further
                if self.download_docs:
                    self._download_page_filings(page_filings)

                all_filings.extend(page_filings)
                log.info(
                    "Page %d/%d: %d filings (total: %d)",
                    page_idx + 1,
                    min(max_pages, total_pages),
                    len(page_filings),
                    len(all_filings),
                )

        return all_filings

    def _download_page_filings(self, filings: list[dict[str, Any]]) -> None:
        """Download documents for a single page's filings.

        Called immediately after each page is parsed so that enc resolution
        happens while the ViewState is still valid for this page.

        Args:
            filings: Filing dicts from the current page.
        """
        if self.parallel_workers > 1:
            download_batch_parallel(
                filings,
                self.session,
                self.hidden_fields,
                self.cache,
                self.pdf_dir,
                workers=self.parallel_workers,
            )
        else:
            for i, filing in enumerate(filings):
                log.info(
                    "[%d/%d] %s | %s | %s",
                    i + 1,
                    len(filings),
                    filing.get("fecha", "")[:16],
                    filing.get("emisora", ""),
                    filing.get("asunto", "")[:40],
                )
                pdf_path = attempt_pdf_download(
                    filing,
                    self.session,
                    self.hidden_fields,
                    self.cache,
                    self.pdf_dir,
                )
                filing["pdf_path"] = pdf_path

    # ------------------------------------------------------------------
    # Cache builder (bulk enc pre-warming)
    # ------------------------------------------------------------------

    def build_cache(
        self,
        start: int = 1,
        end: int = 0,
        workers: int = 10,
    ) -> None:
        """Build the enc cache for a range of keys using parallel workers.

        Each worker creates its own session so all enc resolutions happen
        concurrently.  Results are cached to SQLite.

        Args:
            start: First key to resolve (default: 1).
            end: Last key to resolve (default: 0 = auto-detect from search).
            workers: Number of parallel workers (default: 10).
        """
        self.initialize()

        if end == 0:
            log.info("Auto-detecting latest key...")
            filings = self.search_filings(period="2", max_pages=0)
            if filings:
                keys = [int(f["key"]) for f in filings if f.get("key")]
                end = max(keys) if keys else 0
            if end == 0:
                log.error("Could not detect latest key. Use --end-key.")
                return

        uncached = self.cache.get_uncached_keys(start, end)
        total = end - start + 1
        already_cached = total - len(uncached)

        log.info("=" * 60)
        log.info("BUILDING ENC CACHE")
        log.info("  Range: %d → %d (%d keys)", start, end, total)
        log.info("  Already cached: %d", already_cached)
        log.info("  To resolve: %d", len(uncached))
        log.info("  Workers: %d", workers)
        log.info("=" * 60)

        if not uncached:
            log.info("All keys already cached!")
            return

        resolved = 0
        failed = 0
        start_time = time.time()

        batch_size = max(20, len(uncached) // workers)
        batches = [
            uncached[i : i + batch_size]
            for i in range(0, len(uncached), batch_size)
        ]

        log.info(
            "  Split into %d batches of ~%d keys each",
            len(batches),
            batch_size,
        )

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(resolve_enc_batch, batch): i
                for i, batch in enumerate(batches)
            }
            for future in as_completed(futures):
                batch_results = future.result()
                for key, enc in batch_results:
                    if enc:
                        self.cache.put(key, enc)
                        resolved += 1
                    else:
                        failed += 1

                elapsed = time.time() - start_time
                done = resolved + failed
                rate = done / elapsed if elapsed > 0 else 0
                remaining = (len(uncached) - done) / rate if rate > 0 else 0
                log.info(
                    "  Progress: %d/%d (%.1f/sec, ~%.0fmin remaining) | "
                    "resolved=%d failed=%d | cache=%d",
                    done,
                    len(uncached),
                    rate,
                    remaining / 60,
                    resolved,
                    failed,
                    self.cache.count(),
                )

        log.info("=" * 60)
        log.info(
            "Cache build complete: %d resolved, %d failed, %d total cached",
            resolved,
            failed,
            self.cache.count(),
        )
        log.info("=" * 60)

    # ------------------------------------------------------------------
    # Monitor mode
    # ------------------------------------------------------------------

    def _load_state(self) -> dict[str, Any]:
        """Load monitor state from disk + cache max key.

        Returns:
            State dict with at least a ``last_key`` integer.
        """
        cache_max = self.cache.get_max_key()
        state_file = os.path.join(
            os.path.dirname(self.output_path) or ".", ".monitor_state.json"
        )
        if os.path.exists(state_file):
            with open(state_file, encoding="utf-8") as fh:
                file_state: dict[str, Any] = json.load(fh)
        else:
            file_state = {"last_key": 0}
        file_state["last_key"] = max(file_state["last_key"], cache_max)
        return file_state

    def _save_state(self, state: dict[str, Any]) -> None:
        """Persist monitor state to disk.

        Args:
            state: State dict to serialise.
        """
        state_file = os.path.join(
            os.path.dirname(self.output_path) or ".", ".monitor_state.json"
        )
        with open(state_file, "w", encoding="utf-8") as fh:
            json.dump(state, fh)

    def probe_key(self, key: int) -> dict[str, Any] | None:
        """Check if a filing key exists by calling the callbackPanel directly.

        No prior search is needed — the callbackPanel callback is effectively
        stateless (it only requires a valid ASP.NET_SessionId).

        Args:
            key: Integer filing key to probe.

        Returns:
            Filing dict with enc if the key resolves, else None.
        """
        enc = get_filing_enc(self.session, self.hidden_fields, str(key), self.cache)
        if not enc:
            return None

        detail_url = f"{DETALLE_URL}?enc={enc}"
        resp = safe_get(self.session, detail_url, timeout=30)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.find("title")
        if title and "Error" in title.get_text():
            return None

        page_text = soup.get_text()
        emisora_match = re.search(r"Emisora:\s*(.+?)(?:\n|$)", page_text)
        asunto_match = re.search(r"Asunto\s*:?\s*(.+?)(?:\n|$)", page_text)
        fecha_match = re.search(
            r"Fecha de recepción.*?:\s*(.+?)(?:\n|$)", page_text
        )

        return {
            "key": str(key),
            "enc": enc,
            "emisora": emisora_match.group(1).strip() if emisora_match else "?",
            "asunto": asunto_match.group(1).strip() if asunto_match else "?",
            "fecha": fecha_match.group(1).strip() if fecha_match else "?",
        }

    def monitor(self, interval: int = 300) -> None:
        """Monitor mode: poll for new filings by probing sequential keys.

        Adaptive look-ahead: starts at 5, doubles when all slots find new
        filings (burst detection, up to 200), resets to 5 when a cycle is
        empty.  Stops probing after 3 consecutive misses within a cycle.

        Args:
            interval: Seconds to sleep between idle poll cycles (default 300).
        """
        self.initialize()

        state = self._load_state()
        last_key = state["last_key"]

        if last_key == 0:
            log.info(
                "No monitor state found — running initial search to find latest key..."
            )
            filings = self.search_filings(period="2", max_pages=0)
            if filings:
                keys = [int(f["key"]) for f in filings if f.get("key")]
                last_key = max(keys) if keys else 0
                log.info("Latest key found: %d", last_key)
                state["last_key"] = last_key
                self._save_state(state)
            else:
                log.error(
                    "Could not find any filings to establish baseline. "
                    "Use --start-key."
                )
                return

        look_ahead = 5
        MAX_LOOK_AHEAD = 200

        log.info("=" * 60)
        log.info("MONITOR MODE — watching for new filings")
        log.info("  Last known key: %d", last_key)
        log.info("  Poll interval: %ds (idle)", interval)
        log.info("  Adaptive look-ahead: 5–%d keys", MAX_LOOK_AHEAD)
        log.info("=" * 60)

        try:
            while True:
                new_count = 0
                miss_streak = 0

                for offset in range(1, look_ahead + 1):
                    probe = last_key + offset

                    filing = self.probe_key(probe)
                    if not filing:
                        miss_streak += 1
                        if miss_streak >= 3:
                            break
                        continue

                    miss_streak = 0
                    new_count += 1
                    last_key = probe
                    log.info(
                        "NEW FILING: key=%d | %s | %s",
                        probe,
                        filing["emisora"][:30],
                        filing["asunto"][:40],
                    )

                    if self.download_docs:
                        time.sleep(REQUEST_DELAY)
                        pdf_path = download_pdf_with_enc(
                            filing["enc"],
                            self.pdf_dir,
                            filename_hint=(
                                f"{filing['emisora']}_{filing['asunto']}.pdf"
                            ),
                        )
                        filing["pdf_path"] = pdf_path

                    self._append_filing(filing)
                    time.sleep(REQUEST_DELAY)

                if new_count:
                    state["last_key"] = last_key
                    self._save_state(state)

                    if new_count >= look_ahead - 2:
                        look_ahead = min(look_ahead * 2, MAX_LOOK_AHEAD)
                        log.info(
                            "Processed %d new filing(s). Last key: %d. "
                            "Expanding look-ahead to %d (burst detected).",
                            new_count,
                            last_key,
                            look_ahead,
                        )
                        continue
                    else:
                        look_ahead = 5
                        log.info(
                            "Processed %d new filing(s). Last key: %d.",
                            new_count,
                            last_key,
                        )
                else:
                    look_ahead = 5
                    log.info(
                        "No new filings. Last key: %d. Sleeping %ds...",
                        last_key,
                        interval,
                    )

                time.sleep(interval)

        except KeyboardInterrupt:
            log.info("Monitor stopped by user. Last key: %d", last_key)
            state["last_key"] = last_key
            self._save_state(state)

    def _append_filing(self, filing: dict[str, Any]) -> None:
        """Append a single filing to the output JSON file.

        Args:
            filing: Filing dict to append.
        """
        if os.path.exists(self.output_path):
            with open(self.output_path, encoding="utf-8") as fh:
                data: dict[str, Any] = json.load(fh)
        else:
            data = {"metadata": {"source": PAGE_URL}, "filings": []}

        data["filings"].append(filing)
        data["metadata"]["last_updated"] = datetime.now().isoformat()
        data["metadata"]["total_filings"] = len(data["filings"])

        with open(self.output_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)

    # ------------------------------------------------------------------
    # BIVA ISIN resolution
    # ------------------------------------------------------------------

    def _load_isin_map(self) -> None:
        """Populate ``self.isin_map`` from BIVA (cache-first).

        Called at the start of :meth:`run` when ``with_isin=True``.
        On any failure, ``self.isin_map`` remains ``{}`` and the scraper
        continues without ISINs (all filings get ``isin=None``).
        """
        log.info("BIVA ISIN lookup enabled — loading ISIN map...")
        try:
            self.isin_map = load_isin_map(
                self.session,
                cache_path=self.isin_cache_path,
            )
        except Exception as exc:
            log.warning(
                "BIVA: ISIN map load failed: %s — continuing without ISINs", exc
            )
            self.isin_map = {}

        if self.isin_map:
            log.info("BIVA: %d ISIN entries loaded.", len(self.isin_map))
        else:
            log.warning("BIVA: ISIN map is empty; filings will have isin=None.")

    # ------------------------------------------------------------------
    # Main crawl pipeline
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Execute the full crawl pipeline (initialize → search → download → save).

        Records a crawl_log entry that starts at the beginning and is
        completed (with counts and any error summary) at the end of the run.

        Used by the ``crawl`` subcommand.
        """
        log.info("=" * 60)
        log.info("CNBV STIV-2 Scraper — Starting")
        log.info("=" * 60)

        query_params = json.dumps(
            {"period": self.period, "max_pages": self.max_pages},
            ensure_ascii=False,
        )
        log_id = self.filings_db.log_crawl_start(
            crawl_type="crawl",
            query_params=query_params,
        )

        filings: list[dict[str, Any]] = []
        errors: list[str] = []

        try:
            self.initialize()

            # Optional: load BIVA ISIN map before crawling so ISINs are
            # available at upsert time.  Cache-first — fast on repeat runs.
            if self.with_isin:
                self._load_isin_map()

            # search_filings handles per-page downloads internally when
            # download_docs=True (download-before-paginate invariant).
            filings = self.search_filings(
                period=self.period, max_pages=self.max_pages
            )
        except Exception as exc:
            errors.append(str(exc))
            self.filings_db.log_crawl_complete(
                log_id,
                filings_found=0,
                filings_new=0,
                pages_crawled=0,
                errors="; ".join(errors),
            )
            raise

        if not filings:
            log.error("No filings found. The server may have no data for this period.")
            self.filings_db.log_crawl_complete(
                log_id,
                filings_found=0,
                filings_new=0,
                pages_crawled=0,
                errors="No filings found",
            )
            sys.exit(1)

        log.info("Extracted %d filings", len(filings))

        # Enrich with filing_type classification
        for filing in filings:
            filing["filing_type"] = classify_filing_type(
                filing.get("asunto", "")
            )

        # Persist to L3 spec FilingsDB
        filings_new = 0
        for filing in filings:
            raw_key = filing.get("key") or ""
            filing_id = f"cnbv_{raw_key}" if raw_key else f"cnbv_{filing.get('asunto', '')[:40]}"
            emisora = filing.get("emisora", "").strip().upper()
            isin: str | None = self.isin_map.get(emisora) if emisora else None
            is_new = self.filings_db.get_filing(filing_id) is None
            self.filings_db.upsert_filing(
                filing_id=filing_id,
                ticker=filing.get("emisora", ""),
                isin=isin,
                company_name=filing.get("emisora", ""),
                filing_date=filing.get("fecha", ""),
                headline=filing.get("asunto", ""),
                filing_type=filing.get("filing_type", "other"),
                downloaded=bool(filing.get("pdf_path")),
                download_path=filing.get("pdf_path") or "",
                raw_metadata=json.dumps(filing, ensure_ascii=False),
            )
            if is_new:
                filings_new += 1

        pdf_success = sum(1 for f in filings if f.get("pdf_path"))
        pdf_fail = len(filings) - pdf_success if self.download_docs else 0

        # Complete the crawl log entry
        self.filings_db.log_crawl_complete(
            log_id,
            filings_found=len(filings),
            filings_new=filings_new,
            pages_crawled=max(1, self.max_pages or 1),
            errors=None,
        )

        output = {
            "metadata": {
                "source": PAGE_URL,
                "scraped_at": datetime.now().isoformat(),
                "total_filings": len(filings),
                "pdfs_downloaded": pdf_success,
                "pdfs_failed": pdf_fail,
            },
            "filings": filings,
        }

        with open(self.output_path, "w", encoding="utf-8") as fh:
            json.dump(output, fh, ensure_ascii=False, indent=2)

        log.info("=" * 60)
        log.info("Results saved to: %s", self.output_path)
        log.info(
            "Filings: %d | PDFs downloaded: %d | PDFs failed: %d",
            len(filings),
            pdf_success,
            pdf_fail,
        )
        log.info("=" * 60)

    # ------------------------------------------------------------------
    # Export / stats (read-only, no HTTP)
    # ------------------------------------------------------------------

    def export(self, output_path: str | None = None) -> None:
        """Export cached data to a JSON file.

        If the output JSON already exists, re-exports it with updated metadata.
        Otherwise exports what is in the enc cache.

        Args:
            output_path: Override output path; defaults to self.output_path.
        """
        dest = output_path or self.output_path
        if os.path.exists(self.output_path):
            with open(self.output_path, encoding="utf-8") as fh:
                data = json.load(fh)
            with open(dest, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False, indent=2)
            log.info("Exported %d filings to %s", len(data.get("filings", [])), dest)
        else:
            log.warning("No filings file found at %s", self.output_path)

    def _compute_stats(self) -> dict[str, Any]:
        """Compute and return the statistics dict.

        Health is derived from the ``crawl_log`` table according to these rules:

        - ``"empty"``    — no crawl_log entries at all
        - ``"stale"``    — last completed crawl is older than 48 hours
        - ``"degraded"`` — last crawl had an error rate > 10%
                           (errors present and errors/(filings_found+1) > 0.1)
        - ``"ok"``       — last crawl completed within 48 h with error rate <= 10%
        - ``"error"``    — last crawl_log row has NULL completed_at
                           (the run never finished)
        - ``"error"``    — an exception occurred computing stats

        Returns:
            Stats dict matching the L3 spec schema.
        """
        _STALE_HOURS = 48

        try:
            total = self.filings_db.count_total()
            downloaded = self.filings_db.count_downloaded()
            unique_companies = self.filings_db.count_unique_companies()
            earliest, latest = self.filings_db.get_date_range()

            # Crawl run count from crawl_log
            total_crawl_runs_row = self.filings_db.conn.execute(
                "SELECT COUNT(*) FROM crawl_log WHERE completed_at IS NOT NULL"
            ).fetchone()
            total_crawl_runs = total_crawl_runs_row[0] if total_crawl_runs_row else 0

            # DB size
            db_size = 0
            if os.path.exists(self.filings_db.db_path):
                db_size = os.path.getsize(self.filings_db.db_path)

            # Documents dir size
            docs_size = 0
            if os.path.isdir(self.pdf_dir):
                for dirpath, _dirnames, filenames in os.walk(self.pdf_dir):
                    for fname in filenames:
                        try:
                            docs_size += os.path.getsize(
                                os.path.join(dirpath, fname)
                            )
                        except OSError:
                            pass

            # Health determination — crawl_log-based (48h threshold)
            last_log = self.filings_db.get_last_crawl_log()

            if last_log is None:
                health = "empty"
            elif last_log["completed_at"] is None:
                # Crawl started but never finished
                health = "error"
            else:
                try:
                    completed_dt = datetime.fromisoformat(last_log["completed_at"])
                    age_hours = (datetime.now() - completed_dt).total_seconds() / 3600
                except (ValueError, TypeError):
                    age_hours = float("inf")

                errors_text: str = last_log["errors"] or ""
                filings_found: int = last_log["filings_found"] or 0
                has_errors = bool(errors_text.strip())
                # Error rate: treat any non-empty errors field as a degraded run.
                # We use filings_found+1 as denominator to avoid div-by-zero.
                error_rate = 1.0 if has_errors else 0.0

                if age_hours > _STALE_HOURS:
                    health = "stale"
                elif error_rate > 0.10:
                    health = "degraded"
                else:
                    health = "ok"

            return {
                "scraper": "mexico-scraper",
                "country": "MX",
                "sources": ["cnbv"],
                "total_filings": total,
                "downloaded": downloaded,
                "pending_download": total - downloaded,
                "unique_companies": unique_companies,
                "total_crawl_runs": total_crawl_runs,
                "earliest_record": earliest,
                "latest_record": latest,
                "db_size_bytes": db_size,
                "documents_size_bytes": docs_size,
                "health": health,
            }
        except Exception as exc:
            log.error("Error computing stats: %s", exc)
            return {
                "scraper": "mexico-scraper",
                "country": "MX",
                "sources": ["cnbv"],
                "total_filings": 0,
                "downloaded": 0,
                "pending_download": 0,
                "unique_companies": 0,
                "total_crawl_runs": 0,
                "earliest_record": None,
                "latest_record": None,
                "db_size_bytes": 0,
                "documents_size_bytes": 0,
                "health": "error",
            }

    def stats(self, as_json: bool = False) -> None:
        """Print summary statistics to stdout.

        Args:
            as_json: When True, emit a single JSON object to stdout
                instead of the human-readable table.
        """
        stats_data = self._compute_stats()

        if as_json:
            print(json.dumps(stats_data, indent=2, ensure_ascii=False))
            return

        # Legacy enc cache stats (human-readable mode only)
        cache_count = self.cache.count()
        cache_max = self.cache.get_max_key()

        print("=" * 50)
        print("CNBV STIV-2 Scraper — Statistics")
        print("=" * 50)
        print(f"  Health            : {stats_data['health']}")
        print(f"  Total filings     : {stats_data['total_filings']}")
        print(f"  Downloaded        : {stats_data['downloaded']}")
        print(f"  Pending download  : {stats_data['pending_download']}")
        print(f"  Unique companies  : {stats_data['unique_companies']}")
        print(f"  Earliest record   : {stats_data['earliest_record']}")
        print(f"  Latest record     : {stats_data['latest_record']}")
        print(f"  DB size           : {stats_data['db_size_bytes']} bytes")
        print(f"  Docs size         : {stats_data['documents_size_bytes']} bytes")
        print(f"  Enc cache entries : {cache_count}")
        print(f"  Highest cached key: {cache_max}")
        print(f"  Output file       : {self.output_path}")
        print("=" * 50)


# ---------------------------------------------------------------------------
# CLI — 4 subcommands
# ---------------------------------------------------------------------------


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add flags shared across all subcommands."""
    parser.add_argument(
        "--output",
        default="filings.json",
        help="Output JSON file (default: filings.json)",
    )
    parser.add_argument(
        "--pdf-dir",
        default="pdfs",
        help="Directory for downloaded documents (default: pdfs/)",
    )
    parser.add_argument(
        "--db",
        default="enc_cache.db",
        help="SQLite enc cache path (default: enc_cache.db)",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Write log output to this file in addition to stdout",
    )
    parser.add_argument(
        "--filings-db",
        default="filings_cache.db",
        dest="filings_db",
        help="Path to the L3 filings SQLite DB (default: filings_cache.db)",
    )


def cmd_crawl(args: argparse.Namespace) -> int:
    """Handler for the ``crawl`` subcommand.

    Exit codes:
        0 — crawl completed successfully
        1 — no filings found (server returned empty results)
    """
    _setup_logging(log_file=args.log_file)
    max_pages = args.max_pages if args.max_pages >= 0 else 99999
    filings_db_path = getattr(args, "filings_db", "filings_cache.db")
    with_isin = getattr(args, "with_isin", False)
    isin_cache_path = getattr(args, "isin_cache", "_biva_isin_cache.json")
    scraper = CNBVScraper(
        output_path=args.output,
        pdf_dir=args.pdf_dir,
        max_pages=max_pages,
        period=args.period,
        download_docs=not args.no_download,
        incremental=args.incremental,
        resume=args.resume,
        db_path=args.db,
        filings_db_path=filings_db_path,
        with_isin=with_isin,
        isin_cache_path=isin_cache_path,
    )
    scraper.parallel_workers = args.parallel
    scraper.run()
    return 0


def cmd_monitor(args: argparse.Namespace) -> int:
    """Handler for the ``monitor`` subcommand."""
    _setup_logging(log_file=args.log_file)
    filings_db_path = getattr(args, "filings_db", "filings_cache.db")
    scraper = CNBVScraper(
        output_path=args.output,
        pdf_dir=args.pdf_dir,
        download_docs=not args.no_download,
        db_path=args.db,
        filings_db_path=filings_db_path,
    )
    if args.start_key:
        state_file = os.path.join(
            os.path.dirname(args.output) or ".", ".monitor_state.json"
        )
        with open(state_file, "w") as fh:
            json.dump({"last_key": args.start_key}, fh)
    scraper.monitor(interval=args.interval)
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """Handler for the ``export`` subcommand."""
    _setup_logging(log_file=args.log_file)
    filings_db_path = getattr(args, "filings_db", "filings_cache.db")
    scraper = CNBVScraper(
        output_path=args.output,
        db_path=args.db,
        filings_db_path=filings_db_path,
    )
    scraper.export(output_path=args.export_output)
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    """Handler for the ``stats`` subcommand.

    Exit codes:
        0 — success (health ok or degraded)
        1 — health is stale (data exists but outdated)
        2 — health is empty (no filings found)
        3 — health is error (unexpected exception)
    """
    _setup_logging(log_file=args.log_file)
    filings_db_path = getattr(args, "filings_db", "filings_cache.db")
    scraper = CNBVScraper(
        output_path=args.output,
        db_path=args.db,
        filings_db_path=filings_db_path,
    )
    as_json = getattr(args, "json", False)
    scraper.stats(as_json=as_json)

    # Derive exit code from health
    stats_data = scraper._compute_stats()
    health = stats_data.get("health", "ok")
    _HEALTH_EXIT_CODES: dict[str, int] = {
        "ok": 0,
        "degraded": 0,
        "stale": 1,
        "empty": 2,
        "error": 3,
    }
    return _HEALTH_EXIT_CODES.get(health, 0)


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser with 4 subcommands.

    Returns:
        Configured ArgumentParser.
    """
    root = argparse.ArgumentParser(
        description="Scrape Mexican financial filings from CNBV STIV-2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scraper.py crawl --max-pages 10 --download\n"
            "  python scraper.py monitor --interval 300\n"
            "  python scraper.py export --output filings_export.json\n"
            "  python scraper.py stats\n"
        ),
    )
    sub = root.add_subparsers(dest="command", required=True)

    # ---- crawl ----
    crawl = sub.add_parser(
        "crawl",
        help="Crawl the portal and download filings",
    )
    _add_common_args(crawl)
    crawl.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Max result pages (0=first page only, -1=all pages)",
    )
    crawl.add_argument(
        "--period",
        default="2",
        choices=["0", "1", "2", "3", "4"],
        help="Period filter: 0=All 1=Latest 2=Last6months 3=ThisYear 4=Today",
    )
    crawl.add_argument(
        "--no-download",
        action="store_true",
        help="Skip document downloads (metadata only)",
    )
    crawl.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of parallel download workers (default: 1)",
    )
    crawl.add_argument(
        "--incremental",
        action="store_true",
        help="Skip filings whose key is already in the enc cache",
    )
    crawl.add_argument(
        "--resume",
        action="store_true",
        help="Resume from the last saved state",
    )
    crawl.add_argument(
        "--with-isin",
        action="store_true",
        dest="with_isin",
        help=(
            "Resolve ISIN codes via BIVA before storing filings. "
            "Results are cached in _biva_isin_cache.json so subsequent runs "
            "are fast. First run takes ~140s (one HTTP call per company)."
        ),
    )
    crawl.add_argument(
        "--isin-cache",
        default="_biva_isin_cache.json",
        dest="isin_cache",
        help="Path to the BIVA ISIN JSON cache file (default: _biva_isin_cache.json)",
    )
    crawl.set_defaults(func=cmd_crawl)

    # ---- monitor ----
    monitor = sub.add_parser(
        "monitor",
        help="Continuously watch for new filings",
    )
    _add_common_args(monitor)
    monitor.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Poll interval in seconds when idle (default: 300)",
    )
    monitor.add_argument(
        "--start-key",
        type=int,
        default=0,
        help="Start monitoring from this key (default: auto-detect)",
    )
    monitor.add_argument(
        "--no-download",
        action="store_true",
        help="Skip document downloads",
    )
    monitor.set_defaults(func=cmd_monitor)

    # ---- export ----
    export = sub.add_parser(
        "export",
        help="Export cached filings to JSON",
    )
    _add_common_args(export)
    export.add_argument(
        "--export-output",
        default=None,
        dest="export_output",
        help="Destination file (default: same as --output)",
    )
    export.set_defaults(func=cmd_export)

    # ---- stats ----
    stats = sub.add_parser(
        "stats",
        help="Print scraper statistics",
    )
    _add_common_args(stats)
    stats.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Emit statistics as a JSON object to stdout",
    )
    stats.set_defaults(func=cmd_stats)

    return root


def main() -> int:
    """Entry point."""
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
