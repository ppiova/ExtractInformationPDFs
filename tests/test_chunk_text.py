import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "app"))

from src import chunk_text


def test_chunk_document_creates_chunks(monkeypatch):
    payload = {
        "sourceFile": "Company_FY2024.pdf",
        "year": "FY2024",
        "pages": [
            {"pageNumber": 1, "content": "Item 7. Management's Discussion\nRevenue increased."},
            {"pageNumber": 2, "content": "Risk Factors\nSupply chain disruptions."},
        ],
    }

    monkeypatch.setattr(chunk_text, "CHUNK_TOKEN_TARGET", 50)
    monkeypatch.setattr(chunk_text, "OVERLAP_TOKENS", 10)

    chunks = chunk_text.chunk_document(payload)
    assert chunks, "Expected at least one chunk"
    first = chunks[0]
    assert first["pageStart"] == 1
    assert first["pageEnd"] >= first["pageStart"]
    assert first["section"] in {"MD&A", "Risk Factors", "General"}
