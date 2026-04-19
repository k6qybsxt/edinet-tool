from __future__ import annotations

import shutil
import sqlite3
import sys
import unittest
import uuid
import zipfile
from datetime import date
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli.check_download_coverage import (  # noqa: E402
    CoverageOptions,
    generate_report,
)
from edinet_monitor.services.collector.document_list_service import DocumentListResult  # noqa: E402
from edinet_monitor.services.storage.manifest_service import write_manifest_rows  # noqa: E402


def make_tempdir() -> Path:
    base_dir = ROOT_DIR / "tests" / "_tmp_edinet_monitor"
    base_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = base_dir / f"case_{uuid.uuid4().hex}"
    temp_dir.mkdir()
    return temp_dir


def create_filings_db(db_path: Path, doc_ids: list[str]) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE filings (
                doc_id TEXT PRIMARY KEY,
                download_status TEXT,
                parse_status TEXT,
                zip_path TEXT
            )
            """
        )
        for doc_id in doc_ids:
            conn.execute(
                """
                INSERT INTO filings (doc_id, download_status, parse_status, zip_path)
                VALUES (?, 'downloaded', 'pending', '')
                """,
                (doc_id,),
            )
        conn.commit()
    finally:
        conn.close()


def build_document_row(doc_id: str, **overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "docID": doc_id,
        "edinetCode": "E00001",
        "secCode": "12340",
        "filerName": "Test Corp",
        "docDescription": "Annual Securities Report",
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


class CheckDownloadCoverageTest(unittest.TestCase):
    def test_generate_report_detects_missing_manifest_zip_and_filing(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        db_path = tmpdir / "edinet_monitor.db"
        create_filings_db(db_path, [])

        def fake_fetcher(*, target_date: date, api_key: str, list_type: int = 2) -> DocumentListResult:
            self.assertEqual(target_date, date(2026, 4, 1))
            self.assertEqual(api_key, "dummy-key")
            self.assertEqual(list_type, 2)
            return DocumentListResult(
                metadata={
                    "date": "2026-04-01",
                    "status": "200",
                    "message": "OK",
                    "resultset_count": 1,
                },
                results=[build_document_row("S100AAAA")],
            )

        options = CoverageOptions(
            target_dates=[date(2026, 4, 1)],
            api_key="dummy-key",
            output_dir=tmpdir,
            db_path=db_path,
            manifest_root=tmpdir / "manifests",
            zip_root=tmpdir / "zip",
            master_csv_path=tmpdir / "master.csv",
            manifest_prefix="document_manifest",
            scan_all_manifests=False,
            skip_edinet=False,
            validate_zip=True,
            max_sample_docs=5,
        )

        output_path, rows = generate_report(
            options,
            allowed_edinet_codes={"E00001"},
            fetcher=fake_fetcher,
        )

        self.assertTrue(output_path.exists())
        self.assertEqual(rows[0]["status"], "MANIFEST_MISSING")
        self.assertIn("ZIP_MISSING", rows[0]["issues"])
        self.assertIn("FILING_MISSING", rows[0]["issues"])
        self.assertEqual(rows[0]["edinet_target"], 1)
        self.assertEqual(rows[0]["manifest_rows"], 0)
        self.assertEqual(rows[0]["filing_missing"], 1)

    def test_generate_report_is_ok_when_manifest_zip_and_filing_exist(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_root = tmpdir / "manifests"
        zip_root = tmpdir / "zip"
        zip_path = zip_root / "2026-04-01" / "S100AAAA.zip"
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("dummy.txt", "ok")

        write_manifest_rows(
            manifest_root / "document_manifest_2026-04-01.jsonl",
            [
                {
                    "doc_id": "S100AAAA",
                    "edinet_code": "E00001",
                    "security_code": "12340",
                    "company_name": "Test Corp",
                    "form_code": "030000",
                    "period_end": "2026-03-31",
                    "submit_date": "2026-04-01 09:00",
                    "source_date": "2026-04-01",
                    "zip_path": str(zip_path),
                    "download_status": "downloaded",
                }
            ],
        )
        db_path = tmpdir / "edinet_monitor.db"
        create_filings_db(db_path, ["S100AAAA"])

        def fake_fetcher(*, target_date: date, api_key: str, list_type: int = 2) -> DocumentListResult:
            return DocumentListResult(
                metadata={
                    "date": target_date.isoformat(),
                    "status": "200",
                    "message": "OK",
                    "resultset_count": 1,
                },
                results=[build_document_row("S100AAAA")],
            )

        options = CoverageOptions(
            target_dates=[date(2026, 4, 1)],
            api_key="dummy-key",
            output_dir=tmpdir,
            db_path=db_path,
            manifest_root=manifest_root,
            zip_root=zip_root,
            master_csv_path=tmpdir / "master.csv",
            manifest_prefix="document_manifest",
            scan_all_manifests=False,
            skip_edinet=False,
            validate_zip=True,
            max_sample_docs=5,
        )

        _, rows = generate_report(
            options,
            allowed_edinet_codes={"E00001"},
            fetcher=fake_fetcher,
        )

        self.assertEqual(rows[0]["status"], "OK")
        self.assertEqual(rows[0]["issues"], "")
        self.assertEqual(rows[0]["zip_ok"], 1)
        self.assertEqual(rows[0]["filings"], 1)


if __name__ == "__main__":
    unittest.main()
