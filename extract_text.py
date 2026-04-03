"""
OCR Text Extraction for CNBV Financial Filings
===============================================

Extracts structured text from downloaded PDF/XLS filing documents using
GLM-OCR on Ollama Cloud. Reads the filings.json produced by scraper.py
and outputs extracted text for each document.

Uses the GLM-OCR model (0.9B params, #1 ranked on OmniDocBench) which
specializes in document understanding, table recognition, and formula
extraction.

Requirements:
  pip install ollama pdf2image
  # poppler-utils must be installed: sudo apt-get install poppler-utils

Setup (choose one):
  Option A - Ollama Cloud API key:
    export OLLAMA_API_KEY=your_key_from_ollama.com/settings/keys

  Option B - Local Ollama with cloud sign-in:
    curl -fsSL https://ollama.com/install.sh | sh
    ollama signin
    ollama pull glm-ocr

Usage:
  python extract_text.py                          # Process all filings
  python extract_text.py --pdf pdfs/report.pdf    # Process single file
  python extract_text.py --max-pages 3            # Limit pages per PDF
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import logging
import os
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

# ---------------------------------------------------------------------------
# Ollama client setup
# ---------------------------------------------------------------------------

MODEL = "qwen3-vl:235b-instruct"


def create_ollama_client():
    """Create an Ollama client, preferring cloud API key, falling back to local."""
    from ollama import Client

    api_key = os.environ.get("OLLAMA_API_KEY")

    if api_key:
        log.info("Using Ollama Cloud (API key set)")
        return Client(
            host="https://ollama.com",
            headers={"Authorization": f"Bearer {api_key}"},
        )

    # Try local Ollama instance
    log.info("No OLLAMA_API_KEY set, trying local Ollama at localhost:11434")
    return Client(host="http://localhost:11434")


def image_to_base64(pil_image) -> str:
    """Convert a PIL Image to a base64-encoded JPEG string."""
    buffer = io.BytesIO()
    pil_image.save(buffer, format="JPEG", quality=85)
    return base64.b64encode(buffer.getvalue()).decode("utf-8")


# ---------------------------------------------------------------------------
# PDF text extraction
# ---------------------------------------------------------------------------


def extract_text_from_pdf(
    client,
    pdf_path: str,
    max_pages: int = 0,
    dpi: int = 200,
) -> list[dict]:
    """
    Extract text from a PDF using GLM-OCR via Ollama.

    Args:
        client: Ollama client instance.
        pdf_path: Path to the PDF file.
        max_pages: Max pages to process (0 = all).
        dpi: Resolution for PDF-to-image conversion.

    Returns:
        List of dicts with 'page' and 'text' keys.
    """
    log.info("Converting %s to images (dpi=%d)...", pdf_path, dpi)

    kwargs = {"pdf_path": pdf_path, "dpi": dpi, "fmt": "jpeg"}
    if max_pages > 0:
        kwargs["last_page"] = max_pages

    images = convert_from_path(**kwargs)
    log.info("Converted %d page(s)", len(images))

    results = []
    for page_num, image in enumerate(images, 1):
        log.info("  OCR page %d/%d...", page_num, len(images))

        b64_image = image_to_base64(image)

        response = client.chat(
            model=MODEL,
            messages=[
                {
                    "role": "user",
                    "content": "Extract all text from this document page. Preserve the structure, tables, and formatting as much as possible.",
                    "images": [b64_image],
                }
            ],
        )

        text = response["message"]["content"]
        results.append({"page": page_num, "text": text})
        log.info("  Page %d: %d chars extracted", page_num, len(text))

    return results


def extract_text_from_file(
    client,
    file_path: str,
    max_pages: int = 0,
) -> list[dict] | None:
    """Extract text from a filing document (PDF or skip non-PDF)."""
    path = Path(file_path)

    if not path.exists():
        log.warning("File not found: %s", file_path)
        return None

    suffix = path.suffix.lower()

    if suffix == ".pdf":
        return extract_text_from_pdf(client, str(path), max_pages=max_pages)

    if suffix in (".xls", ".xlsx"):
        log.info("Skipping spreadsheet (use pandas for XLS): %s", path.name)
        return None

    log.warning("Unsupported file type: %s", suffix)
    return None


# ---------------------------------------------------------------------------
# Batch processing from filings.json
# ---------------------------------------------------------------------------


def process_filings(
    filings_path: str = "filings.json",
    output_path: str = "extracted_text.json",
    max_pages: int = 0,
) -> None:
    """Process all filings from the scraper output."""
    with open(filings_path, encoding="utf-8") as f:
        data = json.load(f)

    filings = data.get("filings", [])
    pdf_filings = [f for f in filings if f.get("pdf_path", "").endswith(".pdf")]

    log.info("Found %d PDF filings to process (out of %d total)", len(pdf_filings), len(filings))

    client = create_ollama_client()

    results = []
    for i, filing in enumerate(pdf_filings):
        pdf_path = filing["pdf_path"]
        log.info(
            "[%d/%d] %s | %s | %s",
            i + 1,
            len(pdf_filings),
            filing.get("emisora", "?"),
            filing.get("asunto", "?")[:40],
            pdf_path,
        )

        extracted = extract_text_from_file(client, pdf_path, max_pages=max_pages)
        if extracted:
            results.append(
                {
                    "filing": {
                        "fecha": filing.get("fecha"),
                        "emisora": filing.get("emisora"),
                        "asunto": filing.get("asunto"),
                        "key": filing.get("key"),
                    },
                    "source_file": pdf_path,
                    "pages": extracted,
                }
            )

        time.sleep(0.5)  # Be polite to the API

    # Save results
    output = {
        "metadata": {
            "model": MODEL,
            "total_processed": len(results),
            "total_pages_ocred": sum(len(r["pages"]) for r in results),
        },
        "documents": results,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info("Extracted text saved to: %s", output_path)
    log.info(
        "Processed %d documents, %d total pages",
        len(results),
        sum(len(r["pages"]) for r in results),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract text from CNBV filing PDFs using GLM-OCR on Ollama"
    )
    parser.add_argument(
        "--pdf",
        help="Process a single PDF file instead of filings.json",
    )
    parser.add_argument(
        "--filings",
        default="filings.json",
        help="Path to filings.json from scraper.py (default: filings.json)",
    )
    parser.add_argument(
        "--output",
        default="extracted_text.json",
        help="Output JSON file (default: extracted_text.json)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Max pages per PDF to OCR (0 = all, default: 0)",
    )
    args = parser.parse_args()

    if args.pdf:
        # Single file mode
        client = create_ollama_client()
        results = extract_text_from_file(client, args.pdf, max_pages=args.max_pages)
        if results:
            output = {"source": args.pdf, "pages": results}
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(output, f, ensure_ascii=False, indent=2)
            log.info("Saved to %s", args.output)
    else:
        # Batch mode from filings.json
        process_filings(
            filings_path=args.filings,
            output_path=args.output,
            max_pages=args.max_pages,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
