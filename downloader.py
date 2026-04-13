"""
CNBV STIV-2 Scraper — Downloader
==================================

Document download pipeline for the CNBV portal.

CRITICAL — Download-before-paginate pattern
-------------------------------------------
The CNBV portal is STATEFUL (ASP.NET ViewState). Session state on the server
can be invalidated when you paginate to the next page.  For this reason,
enc values must be *resolved and documents downloaded from the current page
BEFORE paginating*.

Enc value resolution (callbackPanel) shares the main session and its
ViewState.  The actual file download (Detalle.aspx GET + POST) can use a
separate fresh session — the enc token is self-contained and session-
independent at download time.

Flow per filing
---------------
1. callbackPanel POST → ``/*DX*/`` response containing ``enc=<AES-blob>``
2. ``Detalle.aspx?enc=<blob>`` GET → detail page with hidden fields + filename
3. POST to ``Detalle.aspx?enc=<blob>`` with ``__EVENTTARGET=DataViewContenido$DescargaArchivo``
   → binary file response (PDF, XLS, ZIP, …)
"""

from __future__ import annotations

import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests

from http_utils import (
    BROWSER_HEADERS,
    DETALLE_URL,
    REQUEST_DELAY,
    get_filing_enc,
    make_session,
    safe_get,
    safe_post,
)
from db import EncCache
from parsers import extract_hidden_fields, sanitize_filename

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Single-file download
# ---------------------------------------------------------------------------


def download_pdf_with_enc(
    enc: str,
    pdf_dir: str,
    filename_hint: str = "",
    session_override: requests.Session | None = None,
) -> str | None:
    """Download a file from Detalle.aspx using the encrypted enc parameter.

    Two-step ASP.NET postback pattern:
      1. GET ``Detalle.aspx?enc=<blob>`` to obtain hidden fields and filename.
      2. POST back with ``__EVENTTARGET=DataViewContenido$DescargaArchivo``
         to trigger the file stream response.

    Args:
        enc: The encrypted filing identifier (Base64-encoded AES block).
        pdf_dir: Directory to save downloaded files into.
        filename_hint: Optional fallback filename when the page has no name.
        session_override: Use a specific session; creates a fresh one if None.

    Returns:
        Absolute path to the downloaded file, or None on failure.
    """
    s = session_override or make_session()
    detail_url = f"{DETALLE_URL}?enc={enc}"

    resp1 = safe_get(s, detail_url, timeout=30)
    if resp1.status_code != 200:
        log.warning("Detalle.aspx GET failed: status=%d", resp1.status_code)
        return None

    from bs4 import BeautifulSoup

    soup = BeautifulSoup(resp1.text, "html.parser")
    title = soup.find("title")
    if title and "Error" in title.get_text():
        log.warning("Detalle.aspx returned error page for enc=%s", enc[:20])
        return None

    hidden = extract_hidden_fields(soup)

    page_text = soup.get_text()
    archivo_match = re.search(r"Archivo adjunto:\s*(.+?)(?:\n|$)", page_text)
    if archivo_match:
        attached_name = archivo_match.group(1).strip()
    else:
        attached_name = filename_hint or "document.pdf"

    post_data = {
        **hidden,
        "__EVENTTARGET": "DataViewContenido$DescargaArchivo",
        "__EVENTARGUMENT": "",
    }

    resp2 = safe_post(
        s,
        detail_url,
        post_data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": detail_url,
        },
        timeout=60,
    )

    content_type = resp2.headers.get("Content-Type", "")
    is_pdf = "pdf" in content_type.lower() or resp2.content[:4] == b"%PDF"
    is_file = "application/" in content_type and "html" not in content_type.lower()

    if not is_pdf and not is_file:
        log.warning(
            "Download did not return a file (Content-Type=%s, size=%d)",
            content_type,
            len(resp2.content),
        )
        return None

    content_disp = resp2.headers.get("Content-Disposition", "")
    disp_match = re.search(r"filename=(.+?)(?:;|$)", content_disp)
    if disp_match:
        file_name = disp_match.group(1).strip("\"' ")
        file_name = requests.utils.unquote(file_name)
    else:
        file_name = sanitize_filename(attached_name)
        valid_exts = (".pdf", ".xls", ".xlsx", ".doc", ".docx", ".zip")
        if not any(file_name.lower().endswith(ext) for ext in valid_exts):
            file_name += ".pdf" if is_pdf else ".bin"

    os.makedirs(pdf_dir, exist_ok=True)
    file_path = os.path.join(pdf_dir, sanitize_filename(file_name))
    with open(file_path, "wb") as fh:
        fh.write(resp2.content)

    log.info("Downloaded: %s (%d bytes)", file_path, len(resp2.content))
    return file_path


# ---------------------------------------------------------------------------
# Per-filing download attempt (callbackPanel → enc → download)
# ---------------------------------------------------------------------------


def attempt_pdf_download(
    filing: dict[str, Any],
    session: requests.Session,
    hidden_fields: dict[str, str],
    cache: EncCache,
    pdf_dir: str,
) -> str | None:
    """Attempt to download the PDF for a filing.

    Resolves the enc value via the callbackPanel (cache-first), then
    downloads the file.

    IMPORTANT: This function uses the SHARED session for the callbackPanel
    enc resolution because that step requires the current ViewState.  The
    actual file download uses a fresh independent session.

    Args:
        filing: Filing dict containing at least ``key``, ``emisora``, ``asunto``.
        session: Main scraper session (must hold valid ViewState).
        hidden_fields: Current page hidden fields.
        cache: EncCache for cache-first enc lookup.
        pdf_dir: Target directory for downloaded files.

    Returns:
        Local file path on success, or None on failure.
    """
    key = filing.get("key")
    if not key:
        log.warning("No key for filing: %s", filing.get("asunto", "?"))
        return None

    time.sleep(REQUEST_DELAY)

    enc = get_filing_enc(session, hidden_fields, key, cache)
    if enc:
        time.sleep(REQUEST_DELAY)
        return download_pdf_with_enc(
            enc,
            pdf_dir,
            filename_hint=f"{filing['emisora']}_{filing['asunto']}.pdf",
        )

    log.info(
        "Could not get enc for key=%s (%s) — server callback error",
        key,
        filing.get("emisora", "?"),
    )
    return None


# ---------------------------------------------------------------------------
# Parallel download pipeline
# ---------------------------------------------------------------------------


def _download_worker(enc: str, filename_hint: str, pdf_dir: str) -> str | None:
    """Download a single file using a fresh session (thread-safe).

    Each parallel worker gets its own session to avoid shared-state races.

    Args:
        enc: Encrypted filing identifier.
        filename_hint: Fallback filename.
        pdf_dir: Target directory for the downloaded file.

    Returns:
        Local file path on success, or None on failure.
    """
    worker_session = make_session()
    try:
        return download_pdf_with_enc(
            enc,
            pdf_dir,
            filename_hint=filename_hint,
            session_override=worker_session,
        )
    finally:
        worker_session.close()


def download_batch_parallel(
    filings: list[dict[str, Any]],
    session: requests.Session,
    hidden_fields: dict[str, str],
    cache: EncCache,
    pdf_dir: str,
    workers: int = 5,
) -> None:
    """Download documents for multiple filings in parallel.

    Phase 1 (sequential): Resolve enc values via the callbackPanel.
      This uses the shared session/ViewState and MUST be sequential.

    Phase 2 (parallel): Download files using independent per-worker sessions.
      Each worker is stateless at download time — only the enc token is needed.

    This two-phase design preserves the download-before-paginate invariant:
    the caller is responsible for calling this function before advancing
    to the next page (i.e. before the ViewState is updated by a pagination
    callback).

    Args:
        filings: List of filing dicts to process.
        session: Main scraper session holding the current ViewState.
        hidden_fields: Current page hidden fields.
        cache: EncCache for cache-first enc lookup.
        pdf_dir: Target directory for downloaded files.
        workers: Number of parallel download workers.
    """
    # Phase 1: resolve enc values sequentially (shares ViewState)
    to_download: list[tuple[int, str, str]] = []
    for i, filing in enumerate(filings):
        key = filing.get("key")
        if not key:
            continue
        time.sleep(REQUEST_DELAY)
        enc = get_filing_enc(session, hidden_fields, key, cache)
        if enc:
            hint = f"{filing['emisora']}_{filing['asunto']}.pdf"
            to_download.append((i, enc, hint))
        else:
            log.warning("  [%d] No enc for key=%s", i + 1, key)

    log.info(
        "Resolved %d enc values. Downloading with %d parallel workers...",
        len(to_download),
        workers,
    )

    # Phase 2: parallel downloads (each worker uses its own session)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures: dict[Any, int] = {}
        for i, (idx, enc, hint) in enumerate(to_download):
            future = pool.submit(_download_worker, enc, hint, pdf_dir)
            futures[future] = idx
            # Stagger launches to avoid WAF burst detection
            if i < workers:
                time.sleep(0.3)

        for future in as_completed(futures):
            idx = futures[future]
            path = future.result()
            filings[idx]["pdf_path"] = path
            if path:
                log.info("  [%d/%d] Downloaded: %s", idx + 1, len(filings), path)
            else:
                log.warning("  [%d/%d] Download failed", idx + 1, len(filings))
