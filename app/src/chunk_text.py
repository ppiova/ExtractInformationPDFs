"""Chunk narrative text from layout JSON into Azure Search documents."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List

from .utils import (
    detect_section,
    ensure_out_dir,
    load_json,
    majority_vote,
    normalize_whitespace,
    remove_headers_and_footers,
)

LOGGER = logging.getLogger(__name__)
OUTPUT_DIR = ensure_out_dir(Path(__file__).resolve().parent.parent / "out")
CHUNK_TOKEN_TARGET = 1500
OVERLAP_TOKENS = 180
TOKEN_ENCODING = "cl100k_base"


def load_layouts() -> List[Dict]:
    layouts: List[Dict] = []
    for path in sorted(OUTPUT_DIR.glob("layout_*.json")):
        layouts.append(load_json(path))
    return layouts


def prepare_pages(payload: Dict) -> List[Dict]:
    prepared: List[Dict] = []
    for page in payload.get("pages", []):
        text = page.get("content") or ""
        lines = remove_headers_and_footers(text.splitlines())
        normalized = normalize_whitespace(" ".join(lines))
        if not normalized:
            continue
        section = detect_section(normalized)
        prepared.append(
            {
                "pageNumber": page.get("pageNumber"),
                "text": normalized,
                "section": section,
            }
        )
    return prepared


def chunk_document(payload: Dict) -> List[Dict]:
    pages = prepare_pages(payload)
    if not pages:
        return []

    try:
        import tiktoken

        encoding = tiktoken.get_encoding(TOKEN_ENCODING)
        use_tiktoken = True
    except Exception:  # pragma: no cover - fallback path for tests
        encoding = None
        use_tiktoken = False

    doc_tokens: List[int | str] = []
    page_spans: List[Dict] = []
    for page in pages:
        if use_tiktoken:
            tokens = encoding.encode(page["text"] + "\n")  # type: ignore[union-attr]
        else:
            tokens = page["text"].split()
        start = len(doc_tokens)
        doc_tokens.extend(tokens)
        end = len(doc_tokens)
        page_spans.append(
            {
                "page": page["pageNumber"],
                "start": start,
                "end": end,
                "section": page.get("section"),
            }
        )

    chunks: List[Dict] = []
    cursor = 0
    chunk_index = 0
    while cursor < len(doc_tokens):
        end = min(cursor + CHUNK_TOKEN_TARGET, len(doc_tokens))
        chunk_token_slice = doc_tokens[cursor:end]
        if not chunk_token_slice:
            break
        if use_tiktoken:
            content = encoding.decode(chunk_token_slice).strip()  # type: ignore[union-attr]
        else:
            content = " ".join(chunk_token_slice).strip()
        if not content:
            cursor = end
            continue

        overlapping_pages = [
            span for span in page_spans if not (span["end"] <= cursor or span["start"] >= end)
        ]
        if not overlapping_pages:
            cursor = end
            continue
        page_start = min(span["page"] for span in overlapping_pages if span["page"] is not None)
        page_end = max(span["page"] for span in overlapping_pages if span["page"] is not None)
        section = majority_vote(span.get("section") for span in overlapping_pages) or "General"

        chunk_id = f"{payload['sourceFile']}_p{page_start:03d}_c{chunk_index:03d}"
        chunks.append(
            {
                "id": chunk_id,
                "content": content,
                "year": payload.get("year"),
                "section": section,
                "sourceFile": payload["sourceFile"],
                "pageStart": page_start,
                "pageEnd": page_end,
            }
        )
        chunk_index += 1
        if end == len(doc_tokens):
            break
        step = OVERLAP_TOKENS if use_tiktoken else max(1, OVERLAP_TOKENS // 5)
        cursor = max(end - step, cursor + 1)

    return chunks


def write_jsonl(documents: List[Dict], path: Path) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for doc in documents:
            handle.write(json.dumps(doc, ensure_ascii=False) + "\n")


def main() -> None:
    documents: List[Dict] = []
    for payload in load_layouts():
        LOGGER.info(
            "Chunking narrative for %s (pages=%d)",
            payload.get("sourceFile"),
            payload.get("pageCount", 0),
        )
        chunks = chunk_document(payload)
        LOGGER.info("Generated %d narrative chunks", len(chunks))
        documents.extend(chunks)

    output_path = OUTPUT_DIR / "narrative.jsonl"
    write_jsonl(documents, output_path)
    LOGGER.info("Wrote narrative index payload -> %s (%d docs)", output_path, len(documents))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
