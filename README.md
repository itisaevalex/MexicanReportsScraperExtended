# CNBV STIV-2 Mexican Financial Filings Scraper

Scrapes Mexico's regulatory financial filings portal (STIV-2) — the Mexican equivalent of SEC EDGAR — using raw HTTP requests. No headless browsers.

**Portal:** https://stivconsultasexternas.cnbv.gob.mx/ConsultaInformacionEmisoras.aspx

## What It Does

1. **Scrapes** the first page of the latest filings table
2. **Extracts** structured data (Date, Emisora/Issuer, Asunto/Event) into JSON
3. **Downloads** the attached PDF/document for each filing

## Quick Start

```bash
pip install requests beautifulsoup4
python scraper.py
```

Output:
- `filings.json` — structured filing data
- `pdfs/` — downloaded documents (PDFs, XLS, etc.)

### Options

```bash
python scraper.py --output results.json --pdf-dir documents/
```

## Approach & How It Works

The CNBV portal is an ASP.NET WebForms application with DevExpress controls, fronted by an Azure Application Gateway WAF. Reverse-engineering it required understanding three distinct protocols:

### 1. Session & WAF Bypass

The Azure WAF blocks requests without proper browser headers. The scraper sets a Chrome User-Agent and standard browser Accept/Language headers to establish a session (`ASP.NET_SessionId` cookie).

### 2. Search — ASP.NET Synchronous Form POST

The search is triggered by a synchronous form POST that includes:
- All hidden fields from the initial GET (`__VIEWSTATE`, `__EVENTVALIDATION`, `_TSM_HiddenField_`, plus ~17 DevExpress widget-state fields)
- The `ctl00$DefaultPlaceholder$BotonBuscar` submit button as a form field (with `__EVENTTARGET` left empty — this is critical)
- `ComboPeriodo=2` (last 6 months) and date fields with DevExpress epoch-millisecond Raw values

The server returns a full HTML page. Filing data is in a DevExpress GridView (`dxgvDataRow` CSS class) with three columns: Fecha, Emisora, Asunto. Each Asunto cell contains `<a onclick="callbackPanel.PerformCallback(NUMERIC_KEY)">`.

### 3. PDF Download — Three-Step DevExpress Callback Chain

This was the hardest part. Each filing's document is behind an encrypted URL (`Detalle.aspx?enc=<AES-128 Base64 blob>`). Getting the `enc` value requires:

**Step A:** POST a `WebForm_DoCallback` to the page with:
- `__CALLBACKID = ctl00$DefaultPlaceholder$callbackPanel`
- `__CALLBACKPARAM = c0:<numeric_key>` (the `c0:` prefix is critical — see below)

**Step B:** Parse the DevExpress response (`N|<ViewState>/*DX*/({result:'<HTML>'})`) to extract the `enc` value from the popup HTML fragment.

**Step C:** GET `Detalle.aspx?enc=<value>`, extract hidden fields, then POST with `__EVENTTARGET=DataViewContenido$DescargaArchivo` to trigger the file download.

### The `c0:` Prefix Discovery

The DevExpress `ASPxClientCallbackPanel.PerformCallback(key)` JavaScript method silently prepends `c0:` to the argument before dispatching the XHR. This was discovered via Playwright browser-level network interception. Without it, the server's C# handler calls `Substring(2)` on the raw key string, hits index 0, and throws:

> "Length cannot be less than zero. Parameter name: length"

The fix: `__CALLBACKPARAM = "c0:453884"` instead of just `"453884"`.

## Pagination Architecture

The scraper currently extracts only the first page (20 results). Here's how I would architect full pagination:

### Server-Side Pagination via DevExpress GridView

The DevExpress GridView supports server-side pagination through callback postbacks. The grid header contains page navigation controls. To paginate:

1. **Async Postback**: After the initial search, send an ASP.NET ScriptManager async postback with:
   - `ctl00$DefaultPlaceholder$ScriptManager1 = ctl00$DefaultPlaceholder$UpdatePanelResultados|<page_control_id>`
   - `__EVENTTARGET = <GridView pager control>`
   - `__EVENTARGUMENT = <page number>`
   - Updated `__VIEWSTATE` from the previous response

2. **Delta Response Parsing**: The async response uses the ASP.NET pipe-delimited delta format (`LENGTH|updatePanel|ID|CONTENT|`). Parse segments to extract the new `UpdatePanelResultados` HTML and updated `__VIEWSTATE`/`__EVENTVALIDATION`.

3. **Alternative — Increase Page Size**: The simpler approach is to change `ComboFiltroPersonalizado` to `100` or "Ver todos" (view all) to get more results per page, reducing the need for pagination.

### Production Considerations

- **Rate limiting**: The Azure WAF throttles rapid requests. Use 1-2 second delays between requests.
- **Parallelism**: PDF downloads can be parallelized with `concurrent.futures.ThreadPoolExecutor` (each uses its own session with shared cookies).
- **Retry logic**: Add exponential backoff for 403/5xx responses.
- **ViewState management**: Each postback returns updated ViewState. Chain requests sequentially to maintain state consistency.
- **Monitoring**: Log enc values and filing keys for debugging. The `enc` values are deterministic (same key always produces the same enc) and don't expire.

## Tech Stack

- Python 3.10+
- `requests` — HTTP client
- `beautifulsoup4` — HTML parsing
- No headless browsers (Selenium/Playwright/Puppeteer)

## Project Structure

```
scraper.py          # Main scraper script
filings.json        # Output: structured filing data
pdfs/               # Output: downloaded documents
README.md           # This file
_investigation/     # Reverse-engineering scripts (not needed to run)
```
