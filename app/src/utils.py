"""Utility helpers shared across extractor modules."""
from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, List, Optional

LOGGER = logging.getLogger(__name__)

SECTION_PATTERNS = {
    r"management's discussion": "MD&A",
    r"item\s+7\.?": "MD&A",
    r"risk factors": "Risk Factors",
    r"financial statements": "Financial Statements",
    r"consolidated statements": "Financial Statements",
    r"notes to": "Notes",
    r"liquidity and capital resources": "MD&A",
    r"results of operations": "MD&A",
    r"outlook": "Outlook",
}

HEADER_FOOTER_PATTERNS = [
    re.compile(r"^\s*page\s+\d+", re.I),
    re.compile(r"^\s*\d+\s*$"),
]


@contextmanager
def log_timing(label: str):
    start = time.time()
    LOGGER.info("%s -- start", label)
    try:
        yield
    finally:
        duration = time.time() - start
        LOGGER.info("%s -- completed in %.2fs", label, duration)


def ensure_out_dir(path: str | Path) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_whitespace(text: str) -> str:
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = text.replace("\u2018", "'").replace("\u2019", "'")
    text = text.replace("\u201c", '"').replace("\u201d", '"')
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def remove_headers_and_footers(lines: Iterable[str]) -> List[str]:
    cleaned: List[str] = []
    for line in lines:
        normalized = line.strip()
        if not normalized:
            continue
        if any(pattern.search(normalized) for pattern in HEADER_FOOTER_PATTERNS):
            continue
        cleaned.append(normalized)
    return cleaned


def detect_section(text: str) -> Optional[str]:
    lower = text.lower()
    for pattern, section in SECTION_PATTERNS.items():
        if re.search(pattern, lower):
            return section
    return None


def determine_year_from_filename(filename: str) -> Optional[str]:
    match = re.search(r"_fy(\d{4})", filename, flags=re.I)
    if match:
        return f"FY{match.group(1)}"
    return None


def chunk_by_tokens(
    text: str,
    encoding_name: str,
    chunk_tokens: int,
    overlap_tokens: int,
) -> Iterator[str]:
    try:
        import tiktoken

        encoding = tiktoken.get_encoding(encoding_name)
    except Exception:  # pragma: no cover - fallback path
        LOGGER.warning("Falling back to naive token splitting; tiktoken unavailable")
        tokens = text.split()
        step = chunk_tokens - overlap_tokens
        for i in range(0, len(tokens), step):
            yield " ".join(tokens[i : i + chunk_tokens])
        return

    token_ids = encoding.encode(text)
    step = chunk_tokens - overlap_tokens
    for start in range(0, len(token_ids), step):
        end = min(start + chunk_tokens, len(token_ids))
        chunk_ids = token_ids[start:end]
        chunk_text = encoding.decode(chunk_ids)
        if chunk_text.strip():
            yield chunk_text


def safe_float(value: str) -> Optional[float]:
    value = value.replace(",", "").strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def majority_vote(values: Iterable[str | None]) -> Optional[str]:
    filtered = [v for v in values if v]
    if not filtered:
        return None
    counts = Counter(filtered)
    return counts.most_common(1)[0][0]


def slugify(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_").lower()
