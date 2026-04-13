"""
Unit tests for parsers.py

Tests cover:
  - extract_hidden_fields
  - sanitize_filename
  - parse_enc_from_dx_response
  - update_hidden_from_delta
  - parse_filings_from_delta
  - parse_dx_grid_response
  - parse_filings_grid
  - get_total_pages
  - classify_filing_type
"""

from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

from parsers import (
    classify_filing_type,
    extract_hidden_fields,
    get_total_pages,
    parse_dx_grid_response,
    parse_enc_from_dx_response,
    parse_filings_from_delta,
    parse_filings_grid,
    sanitize_filename,
    update_hidden_from_delta,
)


# ---------------------------------------------------------------------------
# extract_hidden_fields
# ---------------------------------------------------------------------------


class TestExtractHiddenFields:
    def test_extracts_viewstate(self, initial_page_html):
        soup = BeautifulSoup(initial_page_html, "html.parser")
        fields = extract_hidden_fields(soup)
        assert "__VIEWSTATE" in fields
        assert fields["__VIEWSTATE"] == "FAKE_VS_VALUE_123abc"

    def test_extracts_eventvalidation(self, initial_page_html):
        soup = BeautifulSoup(initial_page_html, "html.parser")
        fields = extract_hidden_fields(soup)
        assert "__EVENTVALIDATION" in fields
        assert fields["__EVENTVALIDATION"] == "FAKE_EV_VALUE_456def"

    def test_extracts_viewstategenerator(self, initial_page_html):
        soup = BeautifulSoup(initial_page_html, "html.parser")
        fields = extract_hidden_fields(soup)
        assert "__VIEWSTATEGENERATOR" in fields
        assert fields["__VIEWSTATEGENERATOR"] == "CAFE1234"

    def test_returns_empty_for_no_hidden(self):
        soup = BeautifulSoup("<html><body><p>hello</p></body></html>", "html.parser")
        assert extract_hidden_fields(soup) == {}

    def test_uses_id_when_name_missing(self):
        html = '<input type="hidden" id="myField" value="myValue" />'
        soup = BeautifulSoup(html, "html.parser")
        fields = extract_hidden_fields(soup)
        assert fields.get("myField") == "myValue"

    def test_empty_value(self):
        html = '<input type="hidden" name="__PREVIOUSPAGE" value="" />'
        soup = BeautifulSoup(html, "html.parser")
        fields = extract_hidden_fields(soup)
        assert fields.get("__PREVIOUSPAGE") == ""


# ---------------------------------------------------------------------------
# sanitize_filename
# ---------------------------------------------------------------------------


class TestSanitizeFilename:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("normal.pdf", "normal.pdf"),
            ("file/with/slashes.pdf", "file_with_slashes.pdf"),
            ("file:with:colons.pdf", "file_with_colons.pdf"),
            ('file"with"quotes.pdf', "file_with_quotes.pdf"),
            ("file<with>brackets.pdf", "file_with_brackets.pdf"),
            ("  .leading_dots.pdf  ", "leading_dots.pdf"),
            ("file\x00with\x1fnull.pdf", "file_with_null.pdf"),
        ],
    )
    def test_sanitizes(self, raw, expected):
        assert sanitize_filename(raw) == expected


# ---------------------------------------------------------------------------
# parse_enc_from_dx_response
# ---------------------------------------------------------------------------


class TestParseEncFromDxResponse:
    def test_extracts_enc_value(self, dx_cb_enc):
        enc = parse_enc_from_dx_response(dx_cb_enc)
        assert enc is not None
        assert "ABC123XYZ" in enc

    def test_returns_none_without_dx_marker(self):
        assert parse_enc_from_dx_response("plain text response") is None

    def test_returns_none_when_no_enc(self, dx_cb_no_enc):
        enc = parse_enc_from_dx_response(dx_cb_no_enc)
        assert enc is None

    def test_unescapes_backslash_slashes(self):
        text = "/*DX*/some html enc=ABC\\/DEF end"
        enc = parse_enc_from_dx_response(text)
        assert enc == "ABC/DEF"

    def test_handles_inline_dx_marker(self):
        text = 'prefix /*DX*/stuff enc=TOKEN123 suffix'
        enc = parse_enc_from_dx_response(text)
        assert enc == "TOKEN123"


# ---------------------------------------------------------------------------
# update_hidden_from_delta
# ---------------------------------------------------------------------------


class TestUpdateHiddenFromDelta:
    def test_refreshes_viewstate(self, asp_delta):
        old_fields = {
            "__VIEWSTATE": "OLD_VS",
            "__EVENTVALIDATION": "OLD_EV",
        }
        updated = update_hidden_from_delta(asp_delta, old_fields)
        assert updated["__VIEWSTATE"] == "NEW_VS_VALUE_789"
        assert updated["__EVENTVALIDATION"] == "NEW_EV_VALUE_012"

    def test_preserves_existing_when_not_in_delta(self, asp_delta):
        old_fields = {"__VIEWSTATE": "OLD_VS", "extra_field": "keep_me"}
        updated = update_hidden_from_delta(asp_delta, old_fields)
        assert updated["extra_field"] == "keep_me"

    def test_returns_new_dict_not_mutating_original(self, asp_delta):
        """Immutability: original dict must not be mutated."""
        old_fields = {"__VIEWSTATE": "OLD"}
        original_copy = dict(old_fields)
        update_hidden_from_delta(asp_delta, old_fields)
        assert old_fields == original_copy

    def test_handles_empty_delta(self):
        old_fields = {"__VIEWSTATE": "VS123"}
        updated = update_hidden_from_delta("no hidden fields here", old_fields)
        assert updated["__VIEWSTATE"] == "VS123"


# ---------------------------------------------------------------------------
# parse_filings_from_delta
# ---------------------------------------------------------------------------


class TestParseFilingsFromDelta:
    def test_parses_filings_from_update_panel(self, asp_delta):
        filings = parse_filings_from_delta(asp_delta)
        assert len(filings) == 2
        assert filings[0]["emisora"] == "FEMSA"
        assert filings[0]["asunto"] == "Informe Anual 2025"
        assert filings[0]["key"] == "453816"

    def test_second_filing(self, asp_delta):
        filings = parse_filings_from_delta(asp_delta)
        assert filings[1]["emisora"] == "BIMBO"
        assert filings[1]["key"] == "453815"

    def test_fallback_when_no_update_panel_marker(self, grid_html):
        # grid_html has no delta markers, should fallback to parsing entire text
        filings = parse_filings_from_delta(grid_html)
        assert len(filings) >= 1


# ---------------------------------------------------------------------------
# parse_dx_grid_response
# ---------------------------------------------------------------------------


class TestParseDxGridResponse:
    def test_parses_grid_from_dx_response(self, dx_grid_response):
        filings = parse_dx_grid_response(dx_grid_response)
        assert len(filings) == 2
        assert filings[0]["emisora"] == "WALMEX"
        assert filings[0]["key"] == "453800"
        assert filings[1]["emisora"] == "AMXL"
        assert filings[1]["key"] == "453799"

    def test_returns_empty_for_no_dx_marker(self):
        filings = parse_dx_grid_response("plain text no dx marker")
        assert filings == []

    def test_returns_empty_for_dx_without_result(self):
        filings = parse_dx_grid_response("/*DX*/({'error': 'something'})")
        assert filings == []


# ---------------------------------------------------------------------------
# parse_filings_grid
# ---------------------------------------------------------------------------


class TestParseFilingsGrid:
    def test_parses_all_rows(self, grid_html):
        soup = BeautifulSoup(grid_html, "html.parser")
        filings = parse_filings_grid(soup)
        # 3 rows with keys + 1 row without key
        assert len(filings) == 4

    def test_row_with_callback_key(self, grid_html):
        soup = BeautifulSoup(grid_html, "html.parser")
        filings = parse_filings_grid(soup)
        assert filings[0]["fecha"] == "15/03/2026"
        assert filings[0]["emisora"] == "FEMSA"
        assert filings[0]["asunto"] == "Informe Anual 2025"
        assert filings[0]["key"] == "453816"

    def test_row_without_callback_key(self, grid_html):
        soup = BeautifulSoup(grid_html, "html.parser")
        filings = parse_filings_grid(soup)
        # TELMEX row has no PerformCallback link
        telmex = next(f for f in filings if f["emisora"] == "TELMEX")
        assert telmex["key"] is None
        assert telmex["asunto"] == "Evento sin clave"

    def test_second_row(self, grid_html):
        soup = BeautifulSoup(grid_html, "html.parser")
        filings = parse_filings_grid(soup)
        bimbo = next(f for f in filings if f["emisora"] == "BIMBO")
        assert bimbo["key"] == "453815"

    def test_returns_empty_for_empty_grid(self):
        soup = BeautifulSoup("<table></table>", "html.parser")
        assert parse_filings_grid(soup) == []


# ---------------------------------------------------------------------------
# get_total_pages
# ---------------------------------------------------------------------------


class TestGetTotalPages:
    def test_extracts_pages_and_count(self, grid_html):
        pages, count = get_total_pages(grid_html)
        assert pages == 5
        assert count == 87

    def test_extracts_from_asp_delta(self, asp_delta):
        pages, count = get_total_pages(asp_delta)
        assert pages == 3
        assert count == 45

    def test_returns_defaults_when_not_found(self):
        pages, count = get_total_pages("no pager text here")
        assert pages == 1
        assert count == 0


# ---------------------------------------------------------------------------
# classify_filing_type
# ---------------------------------------------------------------------------


class TestClassifyFilingType:
    @pytest.mark.parametrize(
        "headline, expected",
        [
            ("Informe Anual 2025", "annual_report"),
            ("Reporte Anual 2025", "annual_report"),
            ("INFORME ANUAL 2024", "annual_report"),
            ("Resultados Financieros Q3 2025", "financial_results"),
            ("Estados Financieros al 31 de diciembre", "financial_results"),
            ("Financial Results 2025", "financial_results"),
            ("Prospecto de colocación", "prospectus"),
            ("Prospectos varios", "prospectus"),
            ("Aviso de oferta pública", "public_offering"),
            ("Oferta Pública de Acciones", "public_offering"),
            ("Asamblea de Accionistas", "shareholder_meeting"),
            ("Junta de Accionistas", "shareholder_meeting"),
            ("Pago de dividendo", "dividend"),
            ("Fusión con empresa subsidiaria", "corporate_action"),
            ("Adquisición de activos", "corporate_action"),
            ("Calificación crediticia", "credit_rating"),
            ("Rating upgrade", "credit_rating"),
            ("Cambio de auditor externo", "auditor_change"),
            ("Reporte Trimestral Q1", "quarterly_report"),
            ("Informe Semestral H1", "semi_annual_report"),
            ("Evento relevante", "material_event"),
            ("Comunicado de prensa", "press_release"),
            ("Actualización de información", "update"),
            ("Documento sin clasificación", "other"),
            ("", "other"),
        ],
    )
    def test_classification(self, headline, expected):
        assert classify_filing_type(headline) == expected

    def test_case_insensitive(self):
        assert classify_filing_type("INFORME ANUAL") == "annual_report"
        assert classify_filing_type("informe anual") == "annual_report"

    def test_returns_first_match(self):
        # "resultados financieros" should match before "otro"
        result = classify_filing_type("resultados financieros trimestrales")
        assert result == "financial_results"
