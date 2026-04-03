# CNBV STIV-2 Mexican Financial Filings Scraper

Scrapes Mexico's regulatory financial filings portal (STIV-2) — the Mexican equivalent of SEC EDGAR — using raw HTTP requests. No headless browsers.

**Portal:** https://stivconsultasexternas.cnbv.gob.mx/ConsultaInformacionEmisoras.aspx

## What It Does

1. **Scrapes** the filings table with full pagination support (~14,000 filings available)
2. **Extracts** structured data (Date, Emisora/Issuer, Asunto/Event) into JSON
3. **Downloads** the attached documents (PDFs, XLS, etc.) for each filing

## Quick Start

```bash
pip install requests beautifulsoup4
python scraper.py
```

Output:
- `filings.json` — structured filing data
- `pdfs/` — downloaded documents

### CLI Options

```bash
# Default: first page (20 filings) + download documents
python scraper.py

# Scrape 10 pages (200 filings) with downloads
python scraper.py --max-pages 10

# All filings, metadata only (no document downloads)
python scraper.py --max-pages -1 --no-download

# Custom output paths and period filter
python scraper.py --output results.json --pdf-dir documents/ --period 0

# Period options: 0=All, 1=Latest, 2=Last 6 months (default), 3=This year, 4=Today
```

## Technical Approach

The CNBV portal is an ASP.NET WebForms application with DevExpress controls, fronted by an Azure Application Gateway WAF. It required reverse-engineering three distinct protocols to scrape.

### 1. Session & WAF Bypass

The Azure WAF returns 403 for requests without browser-like headers. The scraper sets a Chrome User-Agent and Accept/Language headers to establish an `ASP.NET_SessionId` session cookie.

### 2. Search — ASP.NET Async Postback

The search uses an **async postback** (not a sync form POST — this distinction matters for pagination):
- ScriptManager target: `UpdatePanelBusqueda|BotonBuscar`
- Headers: `X-MicrosoftAjax: Delta=true`, `X-Requested-With: XMLHttpRequest`
- The response is a pipe-delimited delta format containing updated `__VIEWSTATE`, `__EVENTVALIDATION`, and the results grid HTML

Filing data is in a DevExpress GridView with `dxgvDataRow` CSS class rows. Each row has 3 cells: Fecha, Emisora, Asunto. The Asunto cell contains `<a onclick="callbackPanel.PerformCallback(NUMERIC_KEY)">`.

### 3. Pagination — DevExpress GridView Callback

The GridView paginates via `WebForm_DoCallback` with a precisely formatted `__CALLBACKPARAM`:

```
c0:KV|181;['453884','453882',...];GB|20;12|PAGERONCLICK3|PN1;
```

Format: `c0:` prefix + `KV|<len>;<keys_array>;` + `GB|<page_size>;` + `12|PAGERONCLICK3|PN<page_index>;`

The `12` is `len("PAGERONCLICK")` — **not** the length of the full action string. The search form fields must also be included in every pagination POST for the server to maintain query context.

### 4. PDF Download — Three-Step Callback Chain

Each filing's document is behind an encrypted URL (`Detalle.aspx?enc=<AES-128 Base64>`). The flow:

1. **Get enc value:** POST `__CALLBACKID=ctl00$DefaultPlaceholder$callbackPanel` with `__CALLBACKPARAM=c0:<numeric_key>`. Parse the DevExpress `/*DX*/` response to extract the `enc=` value from the popup HTML fragment.
2. **Get detail page:** GET `Detalle.aspx?enc=<value>` to extract hidden fields.
3. **Download file:** POST with `__EVENTTARGET=DataViewContenido$DescargaArchivo` to trigger the binary download.

## Reverse-Engineering Journey

This section documents the investigation process and the roadblocks encountered, since the ASPX portal was deliberately chosen as a tricky target.

### Phase 1: Page Analysis

Initial GET revealed the page structure: ASP.NET WebForms with a `ScriptManager`, four `UpdatePanel`s (Periodo, Desde, Busqueda, Resultados), and DevExpress controls (`ASPxClientButton`, `ASPxClientCallbackPanel`, `ASPxClientPopupControl`, `ASPxClientDateEdit`, `ASPxClientComboBox`). 17 DevExpress client objects total.

### Phase 2: Getting the Search to Return Results

**Roadblock:** Initial sync POST with `__EVENTTARGET=BotonBuscar` returned the page with an empty results panel.

**Investigation:** Launched 4 parallel investigation branches testing different hypotheses:
- Different ScriptManager targets (`UpdatePanelResultados` vs `UpdatePanelBusqueda`)
- Different `__EVENTTARGET` / `__EVENTARGUMENT` values
- Two-step postback chains (period selection → search)
- DevExpress callback vs ASP.NET postback mechanisms

**Resolution:** For a sync POST, `__EVENTTARGET` must be **empty** and the button appears as a separate form field (`ctl00$DefaultPlaceholder$BotonBuscar=""`). For async postback, the ScriptManager target determines which panels update. Using `ComboPeriodo=2` (last 6 months) instead of `0` (Todos) was also needed to get results with the default date range.

### Phase 3: The `c0:` Prefix Discovery

**Roadblock:** The callbackPanel callback (needed to get the `enc` value for PDF downloads) returned a .NET exception: *"Length cannot be less than zero. Parameter name: length"* — for every filing key.

**Initial (wrong) conclusion:** I declared this a server-side bug and built the scraper without PDF support.

**Investigation:** Launched Playwright browser automation to capture the actual network traffic when clicking a filing link. Compared the browser's XHR payload byte-for-byte with my Python request.

**Discovery:** Using Playwright as a **one-time debugging tool** (not part of the scraper — the scraper uses only `requests`), I captured the browser's actual XHR payload. The DevExpress `ASPxClientCallbackPanel.PerformCallback(key)` JavaScript method **silently prepends `c0:` to the argument** before dispatching the XHR. The browser sends `__CALLBACKPARAM=c0:453884`, not `__CALLBACKPARAM=453884`. The server's C# handler does `parameter.Substring(2)` to strip this prefix — without it, the substring call on a shorter string throws the `ArgumentOutOfRangeException`.

**Fix:** One line change: `f"c0:{key}"` instead of `key`.

### Phase 4: Pagination

**Roadblock:** GridView pagination callbacks (`__CALLBACKID=GridViewResultados`, `__CALLBACKPARAM=PN2`) always returned page 1 data.

**Investigation:** Again used Playwright to capture the exact browser payload when clicking page 2.

**Discovery:** Two issues:
1. The GridView callback parameter has a complex format: `c0:KV|<len>;[<current_keys>];GB|<page_size>;12|PAGERONCLICK3|PN<index>;` — the `12` is the length of the string `"PAGERONCLICK"`, not of the full action.
2. The search **must use async postback** (not sync POST) for the resulting ViewState to correctly support GridView pagination callbacks. The sync POST ViewState lacks the grid's internal state.

### Summary of Key Lessons

| Roadblock | Root Cause | Discovery Method |
|-----------|-----------|-----------------|
| Empty results panel | Wrong `__EVENTTARGET` for button submit | Parallel hypothesis testing |
| "Length cannot be less than zero" | Missing `c0:` prefix on callbacks | Playwright network capture |
| Pagination returns page 1 | Action length field + sync vs async ViewState | Playwright + byte comparison |

## Production Considerations

- **Rate limiting**: Azure WAF throttles rapid requests. 1-second delay between requests.
- **Parallelism**: PDF downloads can be parallelized with `ThreadPoolExecutor`.
- **Retry logic**: Add exponential backoff for 403/5xx responses.
- **Scale**: ~14,000 filings × 3 requests each (enc + detail + download) = ~42,000 requests. At 1 req/sec, full scrape takes ~12 hours.

## Tech Stack

- Python 3.10+
- `requests` — HTTP client
- `beautifulsoup4` — HTML parsing
- No headless browsers (Selenium/Playwright/Puppeteer)

## Project Structure

```
scraper.py          # Main scraper script (production code)
filings.json        # Output: structured filing data (generated)
pdfs/               # Output: downloaded documents (generated)
README.md           # This file
_investigation/     # Reverse-engineering scripts (not committed)
```
