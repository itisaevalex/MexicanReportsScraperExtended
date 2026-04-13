"""
CNBV STIV-2 Scraper — HTTP Utilities
======================================

Session management, request helpers, and DevExpress callback construction.

Key concerns:
  - Azure WAF light protection: always send browser User-Agent + Accept headers
  - ASP.NET ViewState must be forwarded with every POST
  - DevExpress GridView pagination callback parameter construction (c0: prefix)
  - DevExpress callbackPanel uses ``c0:<key>`` prefix — critical for correct
    server-side Substring() call (without it the server throws
    "Length cannot be less than zero. Parameter name: length")
  - ``requests`` library (not curl_cffi): plain TLS is sufficient here
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

import requests
import urllib3

from db import EncCache
from parsers import extract_hidden_fields, parse_enc_from_dx_response

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
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

REQUEST_DELAY = 1.0  # seconds between polite requests


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------


def to_dx_epoch_ms(dt: datetime) -> str:
    """Convert a datetime to DevExpress Raw epoch-millisecond string.

    DevExpress date picker fields expect the epoch as an integer string
    (milliseconds since 1970-01-01 UTC).

    Args:
        dt: Datetime to convert.

    Returns:
        String representation of milliseconds since Unix epoch.
    """
    epoch = datetime(1970, 1, 1)
    return str(int((dt - epoch).total_seconds() * 1000))


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------


def make_session() -> requests.Session:
    """Create a new requests Session configured for the CNBV portal.

    Returns:
        A Session with SSL verification disabled (self-signed cert on portal)
        and no persistent cookies/headers pre-set (caller supplies headers).
    """
    s = requests.Session()
    s.verify = False
    return s


# ---------------------------------------------------------------------------
# Safe request wrappers
# ---------------------------------------------------------------------------


def safe_get(
    session: requests.Session,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
    retries: int = 2,
) -> requests.Response:
    """GET with retry on connection errors.

    Args:
        session: Active requests Session.
        url: Target URL.
        headers: Optional request headers (merged with BROWSER_HEADERS).
        timeout: Request timeout in seconds.
        retries: Number of retry attempts on network errors.

    Returns:
        Response object.

    Raises:
        requests.RequestException: If all retries are exhausted.
    """
    merged = {**BROWSER_HEADERS, **(headers or {})}
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return session.get(url, headers=merged, timeout=timeout)
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(1.0 * (attempt + 1))
    raise last_exc  # type: ignore[misc]


def safe_post(
    session: requests.Session,
    url: str,
    data: dict[str, Any],
    *,
    headers: dict[str, str] | None = None,
    timeout: int = 45,
    retries: int = 2,
) -> requests.Response:
    """POST with retry on connection errors.

    Args:
        session: Active requests Session.
        url: Target URL.
        data: Form data dict.
        headers: Optional extra request headers (merged with BROWSER_HEADERS).
        timeout: Request timeout in seconds.
        retries: Number of retry attempts on network errors.

    Returns:
        Response object.

    Raises:
        requests.RequestException: If all retries are exhausted.
    """
    merged = {**BROWSER_HEADERS, **(headers or {})}
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return session.post(url, data=data, headers=merged, timeout=timeout)
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(1.0 * (attempt + 1))
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# DevExpress callback helpers
# ---------------------------------------------------------------------------


def build_gv_callback_param(
    keys: list[str],
    page_index: int,
    page_size: int = 20,
) -> str:
    """Build the DevExpress GridView callback parameter for pagination.

    Format (confirmed via Playwright network capture):
      ``c0:KV|<kv_len>;['key1','key2',...];GB|<page_size>;<action_len>|PAGERONCLICK3|PN<page_index>;``

    The KV block contains the keys currently visible on the page.
    The ``c0:`` prefix is prepended here to match what the browser's
    ASPxClientCallbackPanel.PerformCallback() JavaScript method does.

    Args:
        keys: Filing key strings visible on the current page.
        page_index: 0-based page index to navigate to.
        page_size: Number of rows per page (default 20).

    Returns:
        Complete ``__CALLBACKPARAM`` string.
    """
    kv_array = "[" + ",".join(f"'{k}'" for k in keys) + "]"
    kv_part = f"KV|{len(kv_array)};{kv_array};"
    gb_part = f"GB|{page_size};"
    # The length prefix is len("PAGERONCLICK") = 12, NOT len of the full action.
    action_part = f"12|PAGERONCLICK3|PN{page_index};"
    return f"c0:{kv_part}{gb_part}{action_part}"


def get_filing_enc(
    session: requests.Session,
    hidden_fields: dict[str, str],
    key: str,
    cache: EncCache,
) -> str | None:
    """Call the DevExpress callbackPanel to get the encrypted enc parameter.

    Critical discovery (2026-04-03, confirmed via Playwright network capture):
    The browser sends ``__CALLBACKPARAM = 'c0:<key>'`` — NOT just the raw key.
    The ``c0:`` prefix is prepended by the ASPxClientCallbackPanel.PerformCallback()
    JavaScript method before the XHR is dispatched.  Without this prefix the
    server's Substring() call on an empty string throws:
        "Length cannot be less than zero. Parameter name: length"

    Uses cache-first lookup to avoid redundant server calls.

    Args:
        session: Active requests Session (must have a valid ASP.NET_SessionId).
        hidden_fields: Current page hidden fields including __VIEWSTATE.
        key: Filing integer key as a string.
        cache: EncCache instance for cache-first lookup.

    Returns:
        The enc value string (URL-encoded), or None if the callback fails.
    """
    cached = cache.get(int(key))
    if cached:
        log.debug("Cache hit for key=%s", key)
        return cached

    log.debug("Cache miss — calling callbackPanel for key=%s", key)

    cb_data = {
        **hidden_fields,
        "__CALLBACKID": "ctl00$DefaultPlaceholder$callbackPanel",
        # The 'c0:' prefix is essential — it is prepended by the DX JS runtime.
        "__CALLBACKPARAM": f"c0:{key}",
    }

    resp = safe_post(
        session,
        PAGE_URL,
        cb_data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": PAGE_URL,
        },
    )

    enc_val = parse_enc_from_dx_response(resp.text)
    if enc_val:
        cache.put(int(key), enc_val)
    else:
        log.debug("No enc in callback response for key=%s", key)
    return enc_val


def resolve_enc_batch(keys: list[int]) -> list[tuple[int, str | None]]:
    """Resolve enc for a batch of keys using ONE session (thread-safe).

    Uses a single GET to establish session + ViewState, then fires N
    callbackPanel POSTs reusing that ViewState.  This gives ~2× throughput
    over creating a new session per key (1 GET + N callbacks vs N GETs + N callbacks).

    Designed to be called from :class:`concurrent.futures.ThreadPoolExecutor`.

    Args:
        keys: List of integer filing keys to resolve.

    Returns:
        List of (key, enc_or_None) tuples in input order.
    """
    from bs4 import BeautifulSoup  # local import to avoid circular at module level

    results: list[tuple[int, str | None]] = []
    worker_session = make_session()
    try:
        r = worker_session.get(PAGE_URL, headers=BROWSER_HEADERS, timeout=30)
        if r.status_code != 200:
            return [(k, None) for k in keys]
        hidden = extract_hidden_fields(BeautifulSoup(r.text, "html.parser"))

        cb_headers = {
            **BROWSER_HEADERS,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": PAGE_URL,
        }

        for key in keys:
            try:
                cb = worker_session.post(
                    PAGE_URL,
                    data={
                        **hidden,
                        "__CALLBACKID": "ctl00$DefaultPlaceholder$callbackPanel",
                        "__CALLBACKPARAM": f"c0:{key}",
                    },
                    headers=cb_headers,
                    timeout=30,
                )
                results.append((key, parse_enc_from_dx_response(cb.text)))
            except Exception:
                results.append((key, None))
    except Exception:
        results.extend((k, None) for k in keys[len(results):])
    finally:
        worker_session.close()
    return results
