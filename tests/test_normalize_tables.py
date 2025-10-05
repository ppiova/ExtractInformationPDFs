import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "app"))

from src import normalize_tables


def test_extract_table_records_creates_long_format():
    payload = {
        "sourceFile": "Company_FY2024.pdf",
        "year": "FY2024",
        "tables": [
            {
                "rowCount": 4,
                "columnCount": 2,
                "cells": [
                    {"rowIndex": 0, "columnIndex": 0, "content": "Consolidated Statements of Income"},
                    {"rowIndex": 1, "columnIndex": 1, "content": "FY2024"},
                    {"rowIndex": 2, "columnIndex": 0, "content": "Revenue"},
                    {"rowIndex": 2, "columnIndex": 1, "content": "1,000"},
                    {"rowIndex": 3, "columnIndex": 0, "content": "Net income"},
                    {"rowIndex": 3, "columnIndex": 1, "content": "200"},
                ],
                "boundingRegions": [{"pageNumber": 12}],
            }
        ],
    }

    records = normalize_tables.extract_table_records(payload)
    assert len(records) == 2
    revenue = next(rec for rec in records if rec["metric"] == "Revenue")
    assert revenue["value"] == 1000.0
    assert revenue["page"] == 12
