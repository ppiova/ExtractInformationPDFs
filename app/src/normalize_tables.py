"""Normalize tables extracted from Azure Document Intelligence into CSV facts."""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

from .utils import (
    detect_section,
    ensure_out_dir,
    load_json,
    majority_vote,
    normalize_whitespace,
    safe_float,
    slugify,
)

LOGGER = logging.getLogger(__name__)
OUTPUT_DIR = ensure_out_dir(Path(__file__).resolve().parent.parent / "out")

STATEMENT_PATTERNS = {
    r"income": "Income",
    r"operations": "Income",
    r"balance": "BalanceSheet",
    r"financial position": "BalanceSheet",
    r"cash": "CashFlow",
    r"equity": "Other",
    r"notes": "Notes",
}


def iter_layout_files() -> Iterable[Path]:
    return sorted(OUTPUT_DIR.glob("layout_*.json"))


def detect_statement_type(text: str) -> str:
    lower = text.lower()
    for pattern, label in STATEMENT_PATTERNS.items():
        if re.search(pattern, lower):
            return label
    return "Other"


def normalize_metric_label(label: str) -> str:
    label = normalize_whitespace(label)
    if not label:
        return "UnknownMetric"
    slug = slugify(label)
    return "".join(part.capitalize() for part in slug.split("_")) or "UnknownMetric"


def extract_table_records(payload: Dict) -> List[Dict]:
    records: List[Dict] = []
    tables = payload.get("tables", [])

    for table_index, table in enumerate(tables):
        row_count = table.get("rowCount") or 0
        column_count = table.get("columnCount") or 0
        grid = [["" for _ in range(column_count)] for _ in range(row_count)]

        for cell in table.get("cells", []):
            content = cell.get("content") or ""
            for row in range(cell.get("rowIndex", 0), cell.get("rowIndex", 0) + cell.get("rowSpan", 1)):
                for col in range(cell.get("columnIndex", 0), cell.get("columnIndex", 0) + cell.get("columnSpan", 1)):
                    if grid[row][col]:
                        grid[row][col] += " " + content
                    else:
                        grid[row][col] = content

        grid = [
            [normalize_whitespace(cell) for cell in row]
            for row in grid
            if any((cell or "").strip() for cell in row)
        ]
        if not grid:
            continue

        # Remove empty columns
        non_empty_columns = [
            idx
            for idx in range(len(grid[0]))
            if any((row[idx] or "").strip() for row in grid)
        ]
        grid = [[row[idx] for idx in non_empty_columns] for row in grid]

        header_rows = min(2, len(grid))
        headers: List[str] = []
        for col_idx in range(len(grid[0])):
            header_values = [grid[row_idx][col_idx] for row_idx in range(header_rows)]
            header = normalize_whitespace(" ".join(filter(None, header_values)))
            headers.append(header or f"Column{col_idx}")

        data_rows = grid[header_rows:]
        if not data_rows:
            continue

        metric_column = headers[0]
        table_records: List[Dict] = []
        section_candidates = []
        statement_candidates = []

        for row_idx, row in enumerate(data_rows):
            row_dict = {header: row[col_idx] for col_idx, header in enumerate(headers)}
            metric_raw = normalize_whitespace(str(row_dict.get(metric_column, "")))
            metric = normalize_metric_label(metric_raw)
            statement_text = " ".join(normalize_whitespace(str(v or "")) for v in row_dict.values())
            statement_candidates.append(detect_statement_type(statement_text))
            section_candidates.append(detect_section(statement_text))

            for col_idx, col in enumerate(headers[1:], start=1):
                value_raw = str(row_dict.get(col, "")) if row_dict.get(col) is not None else ""
                value = safe_float(value_raw)
                unit = "%" if "%" in value_raw else "$" if "$" in value_raw else ""
                record = {
                    "id": f"{payload['sourceFile']}_p{table_index:03d}_r{row_idx:03d}_c{col_idx:03d}",
                    "year": payload.get("year"),
                    "statementType": detect_statement_type(metric_raw or statement_text),
                    "section": detect_section(metric_raw) or detect_section(col) or None,
                    "metric": metric,
                    "value": value,
                    "unit": unit,
                    "sourceFile": payload["sourceFile"],
                    "page": (table.get("boundingRegions") or [{}])[0].get("pageNumber"),
                }
                table_records.append(record)

        if table_records:
            dominant_section = majority_vote(section_candidates)
            dominant_statement = majority_vote(statement_candidates)
            for record in table_records:
                record["section"] = record.get("section") or dominant_section or "Financial Statements"
                record["statementType"] = dominant_statement or record["statementType"]
            records.extend(table_records)

    return records


def group_records_by_year(records: Iterable[Dict]) -> Dict[str, List[Dict]]:
    grouped: Dict[str, List[Dict]] = defaultdict(list)
    for record in records:
        year = record.get("year") or "Unknown"
        grouped[year].append(record)
    return grouped


def write_csvs(grouped: Dict[str, List[Dict]]) -> None:
    for year, records in grouped.items():
        if not records:
            continue
        output_path = OUTPUT_DIR / f"facts_{year}.csv"
        with output_path.open("w", encoding="utf-8") as handle:
            if records:
                headers = list(records[0].keys())
                handle.write(",".join(headers) + "\n")
                for record in records:
                    row = [
                        "" if record[key] is None else str(record[key])
                        for key in headers
                    ]
                    handle.write(",".join(row) + "\n")
        LOGGER.info("Wrote %s with %d records", output_path, len(records))


def main() -> None:
    all_records: List[Dict] = []
    for layout_path in iter_layout_files():
        payload = load_json(layout_path)
        LOGGER.info(
            "Normalizing tables for %s (tables=%d)",
            payload.get("sourceFile"),
            payload.get("tableCount", 0),
        )
        records = extract_table_records(payload)
        LOGGER.info("Extracted %d fact rows", len(records))
        all_records.extend(records)

    grouped = group_records_by_year(all_records)
    write_csvs(grouped)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
