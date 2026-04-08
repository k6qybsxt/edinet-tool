from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.collector.document_filter_service import (  # noqa: E402
    filter_target_filings,
    is_target_filing,
)


def build_document_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "docID": "S100TEST",
        "edinetCode": "E00001",
        "secCode": "12340",
        "filerName": "Test Company",
        "docDescription": "有価証券報告書",
        "formCode": "030000",
        "docTypeCode": "120",
        "ordinanceCode": "010",
        "periodEnd": "2026-03-31",
        "submitDateTime": "2026-04-01 09:00",
        "legalStatus": "1",
        "docInfoEditStatus": "0",
    }
    row.update(overrides)
    return row


class DocumentFilterServiceTest(unittest.TestCase):
    def test_is_target_filing_accepts_legal_status_1(self) -> None:
        self.assertTrue(is_target_filing(build_document_row(legalStatus="1")))

    def test_is_target_filing_accepts_legal_status_2(self) -> None:
        self.assertTrue(is_target_filing(build_document_row(legalStatus="2")))

    def test_is_target_filing_rejects_legal_status_0(self) -> None:
        self.assertFalse(is_target_filing(build_document_row(legalStatus="0")))

    def test_filter_target_filings_keeps_extended_period_rows(self) -> None:
        rows = [
            build_document_row(docID="S100A001", legalStatus="1"),
            build_document_row(docID="S100A002", legalStatus="2"),
            build_document_row(docID="S100A003", legalStatus="0"),
        ]

        filtered = filter_target_filings(rows)

        self.assertEqual(
            [str(row["docID"]) for row in filtered],
            ["S100A001", "S100A002"],
        )


if __name__ == "__main__":
    unittest.main()
