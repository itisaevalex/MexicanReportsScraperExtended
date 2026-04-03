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

Usage:
  python scraper.py [--output filings.json] [--pdf-dir pdfs/]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta
from typing import Any

import requests
import urllib3
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://stivconsultasexternas.cnbv.gob.mx"
PAGE_URL = f"{BASE_URL}/ConsultaInformacionEmisoras.aspx"
DETALLE_URL = f"{BASE_URL}/Detalle.aspx"

BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

REQUEST_DELAY = 1.0  # seconds between requests (be polite)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def to_dx_epoch_ms(dt: datetime) -> str:
    """Convert a datetime to DevExpress Raw epoch-millisecond string."""
    epoch = datetime(1970, 1, 1)
    return str(int((dt - epoch).total_seconds() * 1000))


def extract_hidden_fields(soup: BeautifulSoup) -> dict[str, str]:
    """Extract all hidden input fields from a parsed HTML page."""
    fields: dict[str, str] = {}
    for inp in soup.find_all("input", {"type": "hidden"}):
        name = inp.get("name") or inp.get("id")
        if name:
            fields[name] = inp.get("value", "")
    return fields


def sanitize_filename(name: str) -> str:
    """Remove characters that are unsafe for filenames."""
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip(". ")


# ---------------------------------------------------------------------------
# Scraper class
# ---------------------------------------------------------------------------


class CNBVScraper:
    """Scrapes filings from the CNBV STIV-2 portal."""

    def __init__(
        self,
        output_path: str = "filings.json",
        pdf_dir: str = "pdfs",
        max_pages: int = 0,
        period: str = "2",
        download_docs: bool = True,
    ):
        self.output_path = output_path
        self.pdf_dir = pdf_dir
        self.max_pages = max_pages
        self.period = period
        self.download_docs = download_docs
        self.session = requests.Session()
        self.session.verify = False
        self.hidden_fields: dict[str, str] = {}
        self.search_fields: dict[str, str] = {}  # preserved for pagination

    # ----- Step 1: Initialize session -----

    def initialize(self) -> None:
        """GET the initial page to establish session and extract form state."""
        log.info("Fetching initial page: %s", PAGE_URL)
        resp = self.session.get(PAGE_URL, headers=BROWSER_HEADERS, timeout=30)
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

    # ----- Step 2: Search for filings (with pagination) -----

    def _build_gv_callback_param(self, keys: list[str], page_index: int, page_size: int = 20) -> str:
        """
        Build the DevExpress GridView callback parameter for pagination.

        Format (confirmed via Playwright network capture):
          c0:KV|<kv_len>;['key1','key2',...];GB|<page_size>;<action_len>|PAGERONCLICK3|PN<page_index>;

        The KV block contains the keys currently visible on the page.
        """
        kv_array = "[" + ",".join(f"'{k}'" for k in keys) + "]"
        kv_part = f"KV|{len(kv_array)};{kv_array};"
        gb_part = f"GB|{page_size};"
        # The length prefix is len("PAGERONCLICK") = 12, NOT len of the full action.
        action_part = f"12|PAGERONCLICK3|PN{page_index};"
        return f"c0:{kv_part}{gb_part}{action_part}"

    def _parse_dx_grid_response(self, text: str) -> list[dict[str, Any]]:
        """Parse filings from a DevExpress GridView callback response."""
        dx_idx = text.find("/*DX*/")
        if dx_idx < 0:
            return []

        payload = text[dx_idx + 6:]
        if payload.startswith("(") and payload.endswith(")"):
            payload = payload[1:-1]

        res_match = re.search(
            r"'result'\s*:\s*'((?:[^'\\]|\\.)*)'", payload, re.DOTALL
        )
        if not res_match:
            return []

        html = (
            res_match.group(1)
            .replace("\\'", "'")
            .replace("\\r\\n", "\n")
            .replace("\\n", "\n")
            .replace("\\/", "/")
        )
        soup = BeautifulSoup(html, "html.parser")
        return self._parse_filings_grid(soup)

    def _get_total_pages(self, text: str) -> tuple[int, int]:
        """Extract total pages and total filings from pager text."""
        # Look for "Página X de Y (Z Envíos)" in raw text or DX response
        match = re.search(r"gina\s+\d+\s+de\s+(\d+)\s+\((\d+)\s+Env", text)
        if match:
            return int(match.group(1)), int(match.group(2))
        return 1, 0

    def search_filings(self, period: str = "2", max_pages: int = 0) -> list[dict[str, Any]]:
        """
        POST the search form and parse the results grid.

        Args:
            period: ComboPeriodo value. Options:
                0 = Todos, 1 = Último documento, 2 = Últimos 6 meses,
                3 = Año en curso, 4 = Hoy
            max_pages: Maximum number of pages to scrape. 0 = first page only.

        Returns:
            List of filing dicts with keys: fecha, emisora, asunto, key
        """
        log.info("Searching filings (period=%s, max_pages=%s)...", period, max_pages or "1")

        today = datetime.now()
        desde = today - timedelta(days=180)

        # Preserve search fields for use in pagination callbacks
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

        # Use async postback (matches browser behavior, required for correct
        # ViewState that enables GridView pagination).
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

        post_headers = {
            **BROWSER_HEADERS,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": PAGE_URL,
            "Origin": BASE_URL,
        }

        resp = self.session.post(
            PAGE_URL, data=form_data, headers=post_headers, timeout=45
        )
        resp.raise_for_status()

        # Parse the async delta response to extract updated hidden fields
        self._update_hidden_from_delta(resp.text)

        # Parse filings from the delta response (grid HTML is embedded)
        all_filings = self._parse_filings_from_delta(resp.text)

        # Check total pages available
        total_pages, total_count = self._get_total_pages(resp.text)
        if total_count:
            log.info("Server reports %d total filings across %d pages", total_count, total_pages)

        # Paginate if requested
        if max_pages > 1 and total_pages > 1:
            pages_to_fetch = min(max_pages, total_pages) - 1  # already have page 1
            for page_idx in range(1, pages_to_fetch + 1):
                time.sleep(REQUEST_DELAY)

                current_keys = [f["key"] for f in all_filings[-20:] if f.get("key")]
                callback_param = self._build_gv_callback_param(current_keys, page_idx)

                cb_data = {
                    **self.hidden_fields,
                    **self.search_fields,
                    "__CALLBACKID": "ctl00$DefaultPlaceholder$GridViewResultados",
                    "__CALLBACKPARAM": callback_param,
                    "__EVENTTARGET": "",
                    "__EVENTARGUMENT": "",
                }
                cb_headers = {
                    **BROWSER_HEADERS,
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": PAGE_URL,
                }

                resp_page = self.session.post(
                    PAGE_URL, data=cb_data, headers=cb_headers, timeout=30
                )

                page_filings = self._parse_dx_grid_response(resp_page.text)
                if not page_filings:
                    log.warning("No filings on page %d, stopping pagination", page_idx + 1)
                    break

                all_filings.extend(page_filings)
                log.info("Page %d/%d: %d filings (total: %d)", page_idx + 1, min(max_pages, total_pages), len(page_filings), len(all_filings))

        return all_filings

    def _parse_filings_grid(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        """Parse the DevExpress GridView data rows."""
        filings: list[dict[str, Any]] = []

        grid_rows = soup.find_all("tr", class_=re.compile(r"dxgvDataRow"))
        log.info("Found %d filing rows in results grid", len(grid_rows))

        for row in grid_rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            fecha_text = cells[0].get_text(strip=True)
            emisora_text = cells[1].get_text(strip=True)

            # The Asunto cell contains a link with the callback key
            asunto_link = cells[2].find(
                "a", onclick=re.compile(r"PerformCallback")
            )
            if asunto_link:
                asunto_text = asunto_link.get_text(strip=True)
                key_match = re.search(
                    r"PerformCallback\((\d+)\)",
                    asunto_link.get("onclick", ""),
                )
                key = key_match.group(1) if key_match else None
            else:
                asunto_text = cells[2].get_text(strip=True)
                key = None

            filings.append(
                {
                    "fecha": fecha_text,
                    "emisora": emisora_text,
                    "asunto": asunto_text,
                    "key": key,
                }
            )

        return filings

    # ----- Delta response parsing -----

    def _update_hidden_from_delta(self, delta_text: str) -> None:
        """Extract updated hidden fields from an ASP.NET ScriptManager delta response."""
        # Format: LENGTH|hiddenField|FIELD_NAME|VALUE|
        for match in re.finditer(
            r"(\d+)\|hiddenField\|([^|]+)\|", delta_text
        ):
            length = int(match.group(1))
            field_name = match.group(2)
            value_start = match.end()
            value = delta_text[value_start : value_start + length]
            self.hidden_fields[field_name] = value

    def _parse_filings_from_delta(self, delta_text: str) -> list[dict[str, Any]]:
        """Parse filings from an async delta response containing the results grid."""
        # Find the UpdatePanelResultados segment
        marker = "updatePanel|DefaultPlaceholder_UpdatePanelResultados|"
        idx = delta_text.find(marker)
        if idx < 0:
            # Fallback: parse entire delta for grid rows
            soup = BeautifulSoup(delta_text, "html.parser")
            return self._parse_filings_grid(soup)

        content_start = idx + len(marker)
        # Content extends for LENGTH chars (from the segment header)
        # Find the length by going backwards to the pipe before "updatePanel"
        before = delta_text[:idx]
        pipe_pos = before.rfind("|")
        if pipe_pos >= 0:
            len_start = before[:pipe_pos].rfind("|") + 1
            try:
                seg_len = int(before[len_start:pipe_pos])
                html = delta_text[content_start : content_start + seg_len]
            except ValueError:
                html = delta_text[content_start : content_start + 50000]
        else:
            html = delta_text[content_start : content_start + 50000]

        soup = BeautifulSoup(html, "html.parser")
        return self._parse_filings_grid(soup)

    # ----- Step 3: Get filing detail enc value (via callbackPanel) -----

    def get_filing_enc(self, key: str) -> str | None:
        """
        Call the DevExpress callbackPanel to get the encrypted enc parameter
        for Detalle.aspx.

        Critical discovery (2026-04-03, confirmed via Playwright network capture):
        The browser sends __CALLBACKPARAM = 'c0:<key>' — NOT just the raw key.
        The 'c0:' prefix is prepended by the ASPxClientCallbackPanel.PerformCallback()
        JavaScript method before the XHR is dispatched. Without this prefix the
        server's Substring() call on an empty string throws:
            "Length cannot be less than zero. Parameter name: length"

        Returns:
            The enc value string (URL-encoded), or None if the callback fails.
        """
        log.debug("Calling callbackPanel for key=%s", key)

        cb_data = {
            **self.hidden_fields,
            "__CALLBACKID": "ctl00$DefaultPlaceholder$callbackPanel",
            # The 'c0:' prefix is essential — it is prepended by the DX JS runtime.
            "__CALLBACKPARAM": f"c0:{key}",
        }

        cb_headers = {
            **BROWSER_HEADERS,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": PAGE_URL,
        }

        resp = self.session.post(
            PAGE_URL, data=cb_data, headers=cb_headers, timeout=30
        )

        # Parse DevExpress response: N|<viewstate>/*DX*/({...})
        dx_idx = resp.text.find("/*DX*/")
        if dx_idx < 0:
            log.warning("No /*DX*/ marker in callbackPanel response for key=%s", key)
            return None

        payload = resp.text[dx_idx + 6:]
        if payload.startswith("(") and payload.endswith(")"):
            payload = payload[1:-1]

        # Check for error
        err_match = re.search(
            r"'message'\s*:\s*'((?:[^'\\]|\\.)*)'", payload, re.DOTALL
        )
        if err_match:
            err_msg = err_match.group(1).replace("\\r\\n", " ").replace("\\'", "'")
            log.warning("Server error for key=%s: %s", key, err_msg[:100])

        # Look for enc value in the result HTML
        res_match = re.search(
            r"'result'\s*:\s*'((?:[^'\\]|\\.)*)'", payload, re.DOTALL
        )
        if res_match:
            html = (
                res_match.group(1)
                .replace("\\'", "'")
                .replace("\\r\\n", "\n")
                .replace("\\/", "/")
            )
            enc_match = re.search(r"enc=([^&\"'\s>]+)", html)
            if enc_match:
                return enc_match.group(1)

        return None

    # ----- Step 4: Download PDF from Detalle.aspx -----

    def download_pdf_with_enc(self, enc: str, filename_hint: str = "") -> str | None:
        """
        Download a PDF from Detalle.aspx using the encrypted enc parameter.

        This is the working implementation — tested and confirmed functional
        when a valid enc value is available.

        Args:
            enc: The encrypted filing identifier (Base64-encoded AES block).
            filename_hint: Optional hint for the output filename.

        Returns:
            Path to the downloaded PDF, or None on failure.
        """
        detail_url = f"{DETALLE_URL}?enc={enc}"

        # Step 1: GET the detail page to extract hidden fields
        resp1 = self.session.get(detail_url, headers=BROWSER_HEADERS, timeout=30)
        if resp1.status_code != 200:
            log.warning("Detalle.aspx GET failed: status=%d", resp1.status_code)
            return None

        soup = BeautifulSoup(resp1.text, "html.parser")
        title = soup.find("title")
        if title and "Error" in title.get_text():
            log.warning("Detalle.aspx returned error page for enc=%s", enc[:20])
            return None

        hidden = extract_hidden_fields(soup)

        # Extract the attached filename from the page
        page_text = soup.get_text()
        archivo_match = re.search(r"Archivo adjunto:\s*(.+?)(?:\n|$)", page_text)
        if archivo_match:
            attached_name = archivo_match.group(1).strip()
        else:
            attached_name = filename_hint or "document.pdf"

        # Step 2: POST to trigger download via ASP.NET postback
        post_data = {
            **hidden,
            "__EVENTTARGET": "DataViewContenido$DescargaArchivo",
            "__EVENTARGUMENT": "",
        }

        resp2 = self.session.post(
            detail_url,
            data=post_data,
            headers={
                **BROWSER_HEADERS,
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": detail_url,
            },
            timeout=60,
        )

        content_type = resp2.headers.get("Content-Type", "")
        is_pdf = "pdf" in content_type.lower() or resp2.content[:4] == b"%PDF"
        is_file = (
            "application/" in content_type
            and "html" not in content_type.lower()
        )
        if not is_pdf and not is_file:
            log.warning(
                "Download did not return a file (Content-Type=%s, size=%d)",
                content_type,
                len(resp2.content),
            )
            return None

        # Determine filename from Content-Disposition or page text
        content_disp = resp2.headers.get("Content-Disposition", "")
        disp_match = re.search(r"filename=(.+?)(?:;|$)", content_disp)
        if disp_match:
            file_name = disp_match.group(1).strip("\"' ")
            file_name = requests.utils.unquote(file_name)
        else:
            file_name = sanitize_filename(attached_name)
            if not any(file_name.lower().endswith(ext) for ext in (".pdf", ".xls", ".xlsx", ".doc", ".docx", ".zip")):
                file_name += ".pdf" if is_pdf else ".bin"

        os.makedirs(self.pdf_dir, exist_ok=True)
        file_path = os.path.join(self.pdf_dir, sanitize_filename(file_name))
        with open(file_path, "wb") as f:
            f.write(resp2.content)

        log.info("Downloaded: %s (%d bytes)", file_path, len(resp2.content))
        return file_path

    def attempt_pdf_download(self, filing: dict[str, Any]) -> str | None:
        """
        Attempt to download the PDF for a filing.

        Tries the callbackPanel → enc → Detalle.aspx flow.
        Returns the path to the downloaded PDF, or None.
        """
        key = filing.get("key")
        if not key:
            log.warning("No key for filing: %s", filing.get("asunto", "?"))
            return None

        time.sleep(REQUEST_DELAY)

        # Try to get the enc value from the server
        enc = self.get_filing_enc(key)
        if enc:
            time.sleep(REQUEST_DELAY)
            return self.download_pdf_with_enc(
                enc, filename_hint=f"{filing['emisora']}_{filing['asunto']}.pdf"
            )

        log.info(
            "Could not get enc for key=%s (%s) — server callback error",
            key,
            filing.get("emisora", "?"),
        )
        return None

    # ----- Main pipeline -----

    def run(self) -> None:
        """Execute the full scraping pipeline."""
        log.info("=" * 60)
        log.info("CNBV STIV-2 Scraper — Starting")
        log.info("=" * 60)

        # Step 1: Initialize
        self.initialize()

        # Step 2: Search for latest filings
        filings = self.search_filings(period=self.period, max_pages=self.max_pages)

        if not filings:
            log.error("No filings found. The server may have no data for this period.")
            sys.exit(1)

        log.info("Extracted %d filings from first page", len(filings))

        # Step 3: Attempt document downloads
        pdf_success = 0
        pdf_fail = 0

        if self.download_docs:
            for i, filing in enumerate(filings):
                log.info(
                    "[%d/%d] %s | %s | %s",
                    i + 1,
                    len(filings),
                    filing["fecha"][:16],
                    filing["emisora"],
                    filing["asunto"][:40],
                )

                pdf_path = self.attempt_pdf_download(filing)
                filing["pdf_path"] = pdf_path
                if pdf_path:
                    pdf_success += 1
                else:
                    pdf_fail += 1
        else:
            log.info("Skipping document downloads (--no-download)")

        # Step 4: Save results to JSON
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

        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        log.info("=" * 60)
        log.info("Results saved to: %s", self.output_path)
        log.info(
            "Filings: %d | PDFs downloaded: %d | PDFs failed: %d",
            len(filings),
            pdf_success,
            pdf_fail,
        )
        log.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape Mexican financial filings from CNBV STIV-2"
    )
    parser.add_argument(
        "--output",
        default="filings.json",
        help="Output JSON file path (default: filings.json)",
    )
    parser.add_argument(
        "--pdf-dir",
        default="pdfs",
        help="Directory for downloaded PDFs (default: pdfs/)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Max pages to scrape. 0=first page only, -1=all pages (default: 0)",
    )
    parser.add_argument(
        "--period",
        default="2",
        choices=["0", "1", "2", "3", "4"],
        help="Period filter: 0=All, 1=Latest, 2=Last 6 months, 3=This year, 4=Today (default: 2)",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Skip document downloads (extract metadata only)",
    )
    args = parser.parse_args()

    max_pages = args.max_pages if args.max_pages >= 0 else 99999
    scraper = CNBVScraper(
        output_path=args.output,
        pdf_dir=args.pdf_dir,
        max_pages=max_pages,
        period=args.period,
        download_docs=not args.no_download,
    )
    scraper.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
