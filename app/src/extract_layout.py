"""Run Azure Document Intelligence to extract structured layout for PDFs."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List

from azure.core.exceptions import HttpResponseError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .blob_io import list_pdf_blobs, iter_blob_bytes
from .config import get_document_intelligence_client, get_settings
from .utils import determine_year_from_filename, ensure_out_dir, log_timing, normalize_whitespace, save_json

LOGGER = logging.getLogger(__name__)
OUTPUT_DIR = ensure_out_dir(Path(__file__).resolve().parent.parent / "out")

FEATURES = ["tables", "figures", "style", "headings"]


@retry(
    wait=wait_exponential(multiplier=2, min=5, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(HttpResponseError),
)
def analyze_blob(blob_name: str) -> Dict:
    client = get_document_intelligence_client()
    LOGGER.info("Analyzing blob via Document Intelligence: %s", blob_name)
    data = b"".join(iter_blob_bytes(blob_name))
    poller = client.begin_analyze_document(
        model_id="prebuilt-layout",
        document=data,
        content_type="application/pdf",
        features=FEATURES,
    )
    result = poller.result()

    pages: List[Dict] = []
    for page in result.pages or []:
        page_text_parts: List[str] = []
        if result.paragraphs:
            for paragraph in result.paragraphs:
                regions = paragraph.bounding_regions or []
                if any(br.page_number == page.page_number for br in regions):
                    page_text_parts.append(normalize_whitespace(paragraph.content or ""))
        elif page.lines:
            for line in page.lines:
                page_text_parts.append(normalize_whitespace(line.content or ""))

        pages.append(
            {
                "pageNumber": page.page_number,
                "width": page.width,
                "height": page.height,
                "unit": page.unit,
                "content": "\n".join(filter(None, page_text_parts)),
            }
        )

    tables: List[Dict] = []
    for table_index, table in enumerate(result.tables or []):
        table_cells: List[Dict] = []
        for cell in table.cells or []:
            table_cells.append(
                {
                    "content": normalize_whitespace(cell.content or ""),
                    "rowIndex": cell.row_index,
                    "columnIndex": cell.column_index,
                    "rowSpan": cell.row_span or 1,
                    "columnSpan": cell.column_span or 1,
                    "kind": cell.kind,
                    "pageNumber": (cell.bounding_regions or [None])[0].page_number if cell.bounding_regions else None,
                }
            )
        tables.append(
            {
                "id": f"{table_index:03d}",
                "rowCount": table.row_count,
                "columnCount": table.column_count,
                "cells": table_cells,
                "boundingRegions": [
                    {"pageNumber": region.page_number} for region in (table.bounding_regions or [])
                ],
            }
        )

    payload = {
        "blobName": blob_name,
        "sourceFile": Path(blob_name).name,
        "year": determine_year_from_filename(Path(blob_name).name),
        "pageCount": len(pages),
        "tableCount": len(tables),
        "pages": pages,
        "tables": tables,
    }

    return payload


def save_layout(payload: Dict) -> Path:
    source_file = Path(payload["sourceFile"])
    output_path = OUTPUT_DIR / f"layout_{source_file.stem}.json"
    save_json(output_path, payload)
    LOGGER.info("Saved layout output -> %s", output_path)
    return output_path


def main() -> None:
    settings = get_settings()
    blobs = list_pdf_blobs(prefix=None)
    if not blobs:
        LOGGER.warning("No PDF blobs found in container %s", settings.azure_storage_container)
        return

    for blob_name in blobs:
        with log_timing(f"layout:{blob_name}"):
            payload = analyze_blob(blob_name)
            save_layout(payload)
            LOGGER.info(
                "Processed %s -- pages=%d tables=%d",
                blob_name,
                payload["pageCount"],
                payload["tableCount"],
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
