"""Upload narrative and table documents to Azure Cognitive Search."""
from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Dict, Iterable, Iterator, List

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .config import get_search_client, get_settings
from .utils import ensure_out_dir, log_timing, safe_float

LOGGER = logging.getLogger(__name__)
OUTPUT_DIR = ensure_out_dir(Path(__file__).resolve().parent.parent / "out")
BATCH_SIZE = 500


def batched(items: Iterable[Dict], batch_size: int) -> Iterator[List[Dict]]:
    batch: List[Dict] = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def load_narrative() -> List[Dict]:
    path = OUTPUT_DIR / "narrative.jsonl"
    if not path.exists():
        LOGGER.warning("Narrative file %s not found", path)
        return []
    documents: List[Dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            documents.append(json.loads(line))
    return documents


def load_tables() -> List[Dict]:
    documents: List[Dict] = []
    for path in OUTPUT_DIR.glob("facts_*.csv"):
        with path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if "value" in row:
                    row["value"] = safe_float(row["value"] or "")
                if "page" in row and row["page"]:
                    try:
                        row["page"] = int(float(row["page"]))
                    except ValueError:
                        row["page"] = None
                documents.append(row)
    return documents


@retry(
    wait=wait_exponential(multiplier=2, min=5, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(Exception),
)
def upload_batch(client, documents: List[Dict]) -> None:
    if not documents:
        return
    client.merge_or_upload_documents(documents)


def upload_documents(index_name: str, documents: List[Dict]) -> None:
    if not documents:
        LOGGER.warning("No documents to upload for index %s", index_name)
        return
    client = get_search_client(index_name)
    total = 0
    for batch in batched(documents, BATCH_SIZE):
        upload_batch(client, batch)
        total += len(batch)
    LOGGER.info("Uploaded %d documents to %s", total, index_name)


def main() -> None:
    settings = get_settings()
    narrative_docs = load_narrative()
    table_docs = load_tables()

    with log_timing("upload:narrative"):
        upload_documents(settings.azure_search_index_narrative, narrative_docs)

    with log_timing("upload:tables"):
        upload_documents(settings.azure_search_index_tables, table_docs)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
