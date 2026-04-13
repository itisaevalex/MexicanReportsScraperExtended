"""
CNBV STIV-2 Scraper — Parsers
==============================

All HTML/delta/DevExpress response parsing lives here:

  - extract_hidden_fields        — pull every hidden input from a page
  - parse_enc_from_dx_response   — decode /*DX*/ callback to get enc=…
  - update_hidden_from_delta     — refresh __VIEWSTATE from async delta
  - parse_filings_from_delta     — extract grid rows from UpdatePanel delta
  - parse_dx_grid_response       — extract grid rows from a DX GridView callback
  - parse_filings_grid           — low-level row extractor from a BeautifulSoup tree
  - get_total_pages              — read pager text to find total pages / count
  - classify_filing_type         — Spanish keyword → normalised filing type string
  - sanitize_filename            — strip unsafe chars from filenames

Protocol notes
--------------
ASP.NET UpdatePanel delta format:
  <LENGTH>|<type>|<id>|<content>|

DevExpress GridView callback format:
  /*DX*/({'result': '...escaped HTML...',...})

DevExpress callbackPanel callback format:
  /*DX*/<HTML fragment containing Detalle.aspx?enc=<AES-blob>...
"""

from __future__ import annotations

import re
from typing import Any

from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Generic HTML helpers
# ---------------------------------------------------------------------------


def extract_hidden_fields(soup: BeautifulSoup) -> dict[str, str]:
    """Extract all hidden input fields from a parsed HTML page.

    Args:
        soup: Parsed page.

    Returns:
        Mapping of field name (or id) → value string.
    """
    fields: dict[str, str] = {}
    for inp in soup.find_all("input", {"type": "hidden"}):
        name = inp.get("name") or inp.get("id")
        if name:
            fields[name] = inp.get("value", "")
    return fields


def sanitize_filename(name: str) -> str:
    """Remove characters that are unsafe in filenames.

    Args:
        name: Raw filename candidate.

    Returns:
        Safe filename string.
    """
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip(". ")


# ---------------------------------------------------------------------------
# ASP.NET delta-response helpers
# ---------------------------------------------------------------------------


def update_hidden_from_delta(
    delta_text: str,
    hidden_fields: dict[str, str],
) -> dict[str, str]:
    """Extract updated hidden fields from an ASP.NET ScriptManager delta response.

    Delta format per segment: ``<LENGTH>|hiddenField|<FIELD_NAME>|<VALUE>|``

    The function returns a *new* dict (immutable pattern) with the refreshed
    ViewState / EventValidation values merged in.

    Args:
        delta_text: Raw text body of the async partial-page update response.
        hidden_fields: Current hidden fields dict to extend.

    Returns:
        New dict with updated hidden fields merged in.
    """
    updated = dict(hidden_fields)
    for match in re.finditer(r"(\d+)\|hiddenField\|([^|]+)\|", delta_text):
        length = int(match.group(1))
        field_name = match.group(2)
        value_start = match.end()
        value = delta_text[value_start : value_start + length]
        updated[field_name] = value
    return updated


def parse_filings_from_delta(delta_text: str) -> list[dict[str, Any]]:
    """Parse filings from an async delta response containing the results grid.

    Locates the ``UpdatePanelResultados`` segment, extracts its HTML, and
    delegates to :func:`parse_filings_grid`.

    Args:
        delta_text: Raw text body of the async partial-page update.

    Returns:
        List of filing dicts with keys: fecha, emisora, asunto, key.
    """
    marker = "updatePanel|DefaultPlaceholder_UpdatePanelResultados|"
    idx = delta_text.find(marker)
    if idx < 0:
        soup = BeautifulSoup(delta_text, "html.parser")
        return parse_filings_grid(soup)

    content_start = idx + len(marker)
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
    return parse_filings_grid(soup)


# ---------------------------------------------------------------------------
# DevExpress response helpers
# ---------------------------------------------------------------------------


def parse_enc_from_dx_response(text: str) -> str | None:
    """Extract enc= value from a DevExpress ``/*DX*/`` callback response.

    The server returns a ``/*DX*/`` marker followed by escaped HTML that
    contains a ``Detalle.aspx?enc=<AES-blob>`` URL.

    Args:
        text: Raw response body from the callbackPanel XHR.

    Returns:
        URL-encoded enc string, or None if not found.
    """
    dx_idx = text.find("/*DX*/")
    if dx_idx < 0:
        return None
    payload = text[dx_idx + 6:]
    payload = payload.replace("\\'", "'").replace("\\r\\n", "\n").replace("\\/", "/")
    match = re.search(r"enc=([^&\"'\s>\\]+)", payload)
    return match.group(1) if match else None


def parse_dx_grid_response(text: str) -> list[dict[str, Any]]:
    """Parse filings from a DevExpress GridView callback response.

    Response format::

        /*DX*/({'result': '...escaped HTML of the grid...', ...})

    Args:
        text: Raw response body from the GridView pagination callback.

    Returns:
        List of filing dicts parsed from the embedded grid HTML.
    """
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
    return parse_filings_grid(soup)


def parse_filings_grid(soup: BeautifulSoup) -> list[dict[str, Any]]:
    """Parse the DevExpress GridView data rows from a BeautifulSoup tree.

    Each row has three cells: Fecha (date), Emisora (issuer), Asunto (subject).
    The Asunto cell contains an ``<a>`` with an ``onclick`` that carries the
    filing integer key used for the callbackPanel lookup.

    Args:
        soup: Parsed HTML fragment containing the grid.

    Returns:
        List of filing dicts (fecha, emisora, asunto, key).
    """
    filings: list[dict[str, Any]] = []

    grid_rows = soup.find_all("tr", class_=re.compile(r"dxgvDataRow"))

    for row in grid_rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        fecha_text = cells[0].get_text(strip=True)
        emisora_text = cells[1].get_text(strip=True)

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


def get_total_pages(text: str) -> tuple[int, int]:
    """Extract total pages and total filings from pager text.

    Looks for the Spanish pager string: ``Página X de Y (Z Envíos)``.

    Args:
        text: Raw response body (delta or plain HTML).

    Returns:
        Tuple of (total_pages, total_filings). Returns (1, 0) if not found.
    """
    match = re.search(r"gina\s+\d+\s+de\s+(\d+)\s+\((\d+)\s+Env", text)
    if match:
        return int(match.group(1)), int(match.group(2))
    return 1, 0


# ---------------------------------------------------------------------------
# Filing type classification
# ---------------------------------------------------------------------------

#: Ordered list of (pattern, canonical_type) pairs.
#: First match wins; patterns are case-insensitive Spanish keywords.
_FILING_TYPE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"informe\s+anual|reporte\s+anual|annual\s+report", re.I), "annual_report"),
    (re.compile(r"resultados\s+financieros|estados?\s+financieros|financial\s+results", re.I), "financial_results"),
    (re.compile(r"prospecto|prospec", re.I), "prospectus"),
    (re.compile(r"aviso\s+de\s+oferta|oferta\s+p[uú]blica", re.I), "public_offering"),
    (re.compile(r"asamblea|junta\s+de\s+accionistas", re.I), "shareholder_meeting"),
    (re.compile(r"dividendo", re.I), "dividend"),
    (re.compile(r"fusión|fusio|escisión|escisio|adquisición|adquisicio", re.I), "corporate_action"),
    (re.compile(r"calificación|calificacio|rating", re.I), "credit_rating"),
    (re.compile(r"cambio\s+de\s+auditor|auditor", re.I), "auditor_change"),
    (re.compile(r"reporte\s+trimestral|informe\s+trimestral|trimestre", re.I), "quarterly_report"),
    (re.compile(r"informe\s+semestral|semestre", re.I), "semi_annual_report"),
    (re.compile(r"evento\s+relevante|hecho\s+relevante|material\s+event", re.I), "material_event"),
    (re.compile(r"comunicado\s+de\s+prensa|press\s+release", re.I), "press_release"),
    (re.compile(r"actualiz|update", re.I), "update"),
]


def classify_filing_type(headline: str) -> str:
    """Classify a filing by its Spanish headline into a normalised type string.

    Args:
        headline: The ``asunto`` text from the filing row.

    Returns:
        A normalised type string such as ``"annual_report"``, ``"prospectus"``,
        etc.  Falls back to ``"other"`` when no pattern matches.

    Examples:
        >>> classify_filing_type("Informe Anual 2024")
        'annual_report'
        >>> classify_filing_type("Resultados Financieros Q3")
        'financial_results'
        >>> classify_filing_type("Prospecto de colocación")
        'prospectus'
    """
    for pattern, filing_type in _FILING_TYPE_PATTERNS:
        if pattern.search(headline):
            return filing_type
    return "other"
