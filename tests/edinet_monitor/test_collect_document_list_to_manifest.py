from __future__ import annotations

import csv
import shutil
import sys
import unittest
import uuid
from datetime import date
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli.collect_document_list_to_manifest import (  # noqa: E402
    build_default_manifest_name,
    collect_document_manifest_for_dates,
)
from edinet_monitor.services.collector.document_list_service import (  # noqa: E402
    DocumentListResult,
)
from edinet_monitor.services.collector.issuer_master_csv_service import (  # noqa: E402
    load_allowed_edinet_codes,
)
from edinet_monitor.services.storage.manifest_service import (  # noqa: E402
    merge_manifest_rows,
    read_manifest_rows,
    summarize_manifest_rows,
    write_manifest_rows,
)


def make_tempdir() -> Path:
    base_dir = ROOT_DIR / "tests" / "_tmp_edinet_monitor"
    base_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = base_dir / f"case_{uuid.uuid4().hex}"
    temp_dir.mkdir()
    return temp_dir


def build_document_row(doc_id: str, **overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "docID": doc_id,
        "edinetCode": "E00001",
        "secCode": "12340",
        "filerName": "テスト株式会社",
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


class CollectDocumentListToManifestTest(unittest.TestCase):
    def test_build_default_manifest_name_for_single_date(self) -> None:
        self.assertEqual(
            build_default_manifest_name([date(2026, 4, 1)]),
            "document_manifest_2026-04-01",
        )

    def test_load_allowed_edinet_codes_uses_tse_rows_only(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        csv_path = tmpdir / "issuer_master.csv"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["edinet_code", "exchange", "company_name"],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "edinet_code": "E00001",
                    "exchange": "TSE",
                    "company_name": "テスト株式会社",
                }
            )
            writer.writerow(
                {
                    "edinet_code": "E00002",
                    "exchange": "NYSE",
                    "company_name": "Other Corp",
                }
            )
            writer.writerow(
                {
                    "edinet_code": "",
                    "exchange": "TSE",
                    "company_name": "Blank Corp",
                }
            )

        allowed_codes = load_allowed_edinet_codes(csv_path)

        self.assertEqual(allowed_codes, {"E00001"})

    def test_merge_manifest_rows_overwrites_by_doc_id(self) -> None:
        existing_rows = [
            {"doc_id": "S100AAAA", "company_name": "旧会社", "submit_date": "2026-04-01 09:00"},
        ]
        incoming_rows = [
            {"doc_id": "S100AAAA", "company_name": "新会社", "submit_date": "2026-04-01 09:00"},
            {"doc_id": "S100BBBB", "company_name": "別会社", "submit_date": "2026-04-02 09:00"},
        ]

        merged = merge_manifest_rows(existing_rows, incoming_rows)

        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[0]["company_name"], "新会社")
        self.assertEqual(merged[1]["doc_id"], "S100BBBB")

    def test_collect_document_manifest_for_dates_saves_filtered_rows(self) -> None:
        def fake_fetcher(*, target_date: date, api_key: str, list_type: int = 2) -> DocumentListResult:
            self.assertEqual(api_key, "dummy-key")
            self.assertEqual(list_type, 2)
            self.assertEqual(target_date, date(2026, 4, 1))
            return DocumentListResult(
                metadata={
                    "date": "2026-04-01",
                    "status": "200",
                    "message": "OK",
                },
                results=[
                    build_document_row("S100AAAA"),
                    build_document_row("S100BBBB", edinetCode="E99999"),
                    build_document_row("S100CCCC", formCode="043000"),
                ],
            )

        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_path = tmpdir / "document_manifest.jsonl"

        summary = collect_document_manifest_for_dates(
            [date(2026, 4, 1)],
            api_key="dummy-key",
            allowed_edinet_codes={"E00001"},
            manifest_path=manifest_path,
            fetcher=fake_fetcher,
        )
        saved_rows = read_manifest_rows(manifest_path)

        self.assertEqual(summary["totals"]["all_results"], 3)
        self.assertEqual(summary["totals"]["target_results"], 2)
        self.assertEqual(summary["totals"]["issuer_target_results"], 1)
        self.assertEqual(summary["saved_manifest_rows"], 1)
        self.assertEqual(saved_rows[0]["doc_id"], "S100AAAA")
        self.assertEqual(saved_rows[0]["company_name"], "テスト株式会社")
        self.assertTrue(saved_rows[0]["zip_path"].endswith(r"2026-04-01\S100AAAA.zip"))

    def test_write_manifest_rows_round_trips_utf8_jsonl(self) -> None:
        rows = [
            {"doc_id": "S100AAAA", "company_name": "テスト株式会社", "submit_date": "2026-04-01 09:00"},
        ]

        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_path = tmpdir / "manifest.jsonl"
        written = write_manifest_rows(manifest_path, rows)
        loaded = read_manifest_rows(manifest_path)

        self.assertEqual(written, 1)
        self.assertEqual(loaded, rows)

    def test_summarize_manifest_rows_counts_statuses(self) -> None:
        summary = summarize_manifest_rows(
            [
                {"doc_id": "S100AAAA", "download_status": "pending"},
                {"doc_id": "S100BBBB", "download_status": "downloaded"},
                {
                    "doc_id": "S100CCCC",
                    "company_name": "テスト株式会社",
                    "submit_date": "2026-04-01 09:00",
                    "download_status": "error",
                    "download_error": "RuntimeError('boom')",
                },
            ]
        )

        self.assertEqual(summary["manifest_rows"], 3)
        self.assertEqual(summary["pending_rows"], 1)
        self.assertEqual(summary["downloaded_rows"], 1)
        self.assertEqual(summary["error_rows"], 1)
        self.assertEqual(summary["sample_errors"][0]["doc_id"], "S100CCCC")


if __name__ == "__main__":
    unittest.main()
