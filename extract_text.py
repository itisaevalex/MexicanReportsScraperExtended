"""
OCR Text Extraction for CNBV Financial Filings
===============================================

Extracts structured JSON from downloaded filing documents (PDF, XLS, etc.)
using Qwen3.5-397B vision model on Ollama Cloud. All pages of a document
are sent together in a single request so the model produces one unified
JSON per filing.

Requirements:
  pip install ollama pdf2image xlrd
  # poppler-utils must be installed: sudo apt-get install poppler-utils

Setup:
  export OLLAMA_API_KEY=your_key_from_ollama.com/settings/keys

Usage:
  python extract_text.py                          # Process all filings
  python extract_text.py --file pdfs/report.pdf   # Process single file
  python extract_text.py --max-pages 3            # Limit pages per PDF
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from pdf2image import convert_from_path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MODEL = "qwen3.5:397b"

EXTRACTION_PROMPT = (
    "You are analyzing a Mexican financial regulatory filing (CNBV STIV-2). "
    "All pages of this document are provided as images. "
    "Extract ALL information into a single structured JSON object with these fields "
    "(include only those present in the document):\n"
    '- "document_type": type of document (e.g. "Acta de Asamblea", "Aviso de Suscripción", '
    '"Informe Anual", "Calificación Crediticia", "Estados Financieros", "Composición de Cartera", etc.)\n'
    '- "issuer": primary company/entity name\n'
    '- "dates": object with relevant dates (e.g. {"filing_date": "...", "meeting_date": "...", "period": "..."})\n'
    '- "parties": list of {{"name": "...", "role": "..."}} for all entities involved\n'
    '- "amounts": list of {{"value": "...", "currency": "...", "description": "..."}} for monetary figures\n'
    '- "key_terms": list of important identifiers (certificate numbers, fideicomiso IDs, ticker symbols, etc.)\n'
    '- "summary": 3-5 sentence summary of the entire document\n'
    '- "raw_text": the complete extracted text from all pages combined, preserving structure\n'
    "Respond ONLY with valid JSON. No markdown fences, no explanation."
)


# ---------------------------------------------------------------------------
# Ollama client
# ---------------------------------------------------------------------------


def create_ollama_client():
    """Create an Ollama client using cloud API key or local instance."""
    from ollama import Client

    api_key = os.environ.get("OLLAMA_API_KEY")
    if api_key:
        log.info("Using Ollama Cloud (API key set)")
        return Client(
            host="https://ollama.com",
            headers={"Authorization": f"Bearer {api_key}"},
        )
    log.info("No OLLAMA_API_KEY set, trying local Ollama at localhost:11434")
    return Client(host="http://localhost:11434")


def image_to_base64(pil_image) -> str:
    """Convert a PIL Image to a base64-encoded JPEG string."""
    buf = io.BytesIO()
    pil_image.save(buf, format="JPEG", quality=85)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def parse_json_response(text: str) -> dict:
    """Parse a JSON response, stripping markdown fences if needed."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    clean = text.strip()
    if clean.startswith("```"):
        clean = clean.split("\n", 1)[1] if "\n" in clean else clean[3:]
    if clean.endswith("```"):
        clean = clean[:-3]
    clean = clean.strip()
    if clean.startswith("json"):
        clean = clean[4:].strip()
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        return {"raw_text": text, "_parse_error": True}


# ---------------------------------------------------------------------------
# File → images conversion
# ---------------------------------------------------------------------------


def pdf_to_images(pdf_path: str, max_pages: int = 0, dpi: int = 200) -> list:
    """Convert PDF pages to PIL images."""
    kwargs = {"pdf_path": pdf_path, "dpi": dpi, "fmt": "jpeg"}
    if max_pages > 0:
        kwargs["last_page"] = max_pages
    return convert_from_path(**kwargs)


def xls_to_images(xls_path: str) -> list:
    """Convert XLS/XLSX to images via LibreOffice → PDF → images."""
    tmp_dir = Path(xls_path).parent / "_tmp_convert"
    tmp_dir.mkdir(exist_ok=True)
    try:
        # LibreOffice headless conversion to PDF
        result = subprocess.run(
            ["libreoffice", "--headless", "--convert-to", "pdf",
             "--outdir", str(tmp_dir), xls_path],
            capture_output=True, timeout=30,
        )
        pdf_name = Path(xls_path).stem + ".pdf"
        pdf_path = tmp_dir / pdf_name
        if pdf_path.exists():
            images = pdf_to_images(str(pdf_path), max_pages=5)
            pdf_path.unlink()
            return images
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    finally:
        try:
            tmp_dir.rmdir()
        except OSError:
            pass

    # Fallback: read with xlrd and render as text image
    return _xls_to_text_images(xls_path)


def _xls_to_text_images(xls_path: str) -> list:
    """Fallback: render XLS data as a text image using PIL."""
    try:
        import xlrd
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        log.warning("xlrd or PIL not available for XLS rendering")
        return []

    try:
        wb = xlrd.open_workbook(xls_path)
    except Exception as e:
        log.warning("Cannot open XLS %s: %s", xls_path, e)
        return []

    images = []
    for sheet in wb.sheets():
        lines = []
        for row_idx in range(min(sheet.nrows, 60)):
            cells = []
            for col in range(sheet.ncols):
                val = str(sheet.cell_value(row_idx, col)).strip()
                cells.append(val[:25].ljust(25))
            lines.append(" | ".join(cells))

        text = f"Sheet: {sheet.name}\n" + "-" * 80 + "\n" + "\n".join(lines)

        # Render to image
        img = Image.new("RGB", (1600, max(400, len(lines) * 18 + 60)), "white")
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", 13)
        except (OSError, IOError):
            font = ImageFont.load_default()
        draw.text((10, 10), text, fill="black", font=font)
        images.append(img)

    return images


def file_to_images(file_path: str, max_pages: int = 0) -> list:
    """Convert any supported file type to a list of PIL images."""
    path = Path(file_path)
    suffix = path.suffix.lower()

    if suffix == ".pdf":
        return pdf_to_images(file_path, max_pages=max_pages)
    elif suffix in (".xls", ".xlsx"):
        return xls_to_images(file_path)
    elif suffix == ".zip":
        log.info("Skipping ZIP: %s", path.name)
        return []
    else:
        log.warning("Unsupported file type: %s", suffix)
        return []


# ---------------------------------------------------------------------------
# OCR extraction (all pages → one JSON)
# ---------------------------------------------------------------------------


def extract_filing(client, file_path: str, max_pages: int = 0) -> dict | None:
    """
    Extract structured JSON from a filing document.

    All pages are sent together in a single request so the model
    produces one unified JSON output per filing.
    """
    path = Path(file_path)
    if not path.exists():
        log.warning("File not found: %s", file_path)
        return None

    images = file_to_images(file_path, max_pages=max_pages)
    if not images:
        return None

    log.info("  %d page(s) → sending to %s...", len(images), MODEL)
    b64_images = [image_to_base64(img) for img in images]

    response = client.chat(
        model=MODEL,
        messages=[
            {
                "role": "user",
                "content": EXTRACTION_PROMPT,
                "images": b64_images,
            }
        ],
    )

    text = response["message"]["content"]
    parsed = parse_json_response(text)
    log.info("  Extracted: %d chars, %d fields", len(text), len(parsed))
    return parsed


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------


def process_filings(
    filings_path: str = "filings.json",
    output_path: str = "extracted_text.json",
    max_pages: int = 0,
) -> None:
    """Process all filings from scraper output into structured JSON."""
    with open(filings_path, encoding="utf-8") as f:
        data = json.load(f)

    filings = data.get("filings", [])
    processable = [f for f in filings if f.get("pdf_path") and not f["pdf_path"].endswith(".zip")]
    log.info("Processing %d filings (%d total, skipping ZIPs)", len(processable), len(filings))

    client = create_ollama_client()
    results = []

    for i, filing in enumerate(processable):
        file_path = filing["pdf_path"]
        log.info(
            "[%d/%d] %s | %s | %s",
            i + 1, len(processable),
            filing.get("emisora", "?"),
            filing.get("asunto", "?")[:40],
            Path(file_path).name,
        )

        extracted = extract_filing(client, file_path, max_pages=max_pages)
        if extracted:
            results.append({
                "filing": {
                    "fecha": filing.get("fecha"),
                    "emisora": filing.get("emisora"),
                    "asunto": filing.get("asunto"),
                    "key": filing.get("key"),
                },
                "source_file": file_path,
                "extracted": extracted,
            })

        time.sleep(0.5)

    output = {
        "metadata": {
            "model": MODEL,
            "total_processed": len(results),
        },
        "documents": results,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info("Saved %d extracted documents to %s", len(results), output_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract structured JSON from CNBV filings via OCR (Ollama Cloud)"
    )
    parser.add_argument("--file", help="Process a single file (PDF/XLS)")
    parser.add_argument("--filings", default="filings.json", help="filings.json from scraper.py")
    parser.add_argument("--output", default="extracted_text.json", help="Output JSON file")
    parser.add_argument("--max-pages", type=int, default=0, help="Max pages per document (0=all)")
    args = parser.parse_args()

    if args.file:
        client = create_ollama_client()
        result = extract_filing(client, args.file, max_pages=args.max_pages)
        if result:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump({"source": args.file, "extracted": result}, f, ensure_ascii=False, indent=2)
            log.info("Saved to %s", args.output)
    else:
        process_filings(args.filings, args.output, args.max_pages)

    return 0


if __name__ == "__main__":
    sys.exit(main())
