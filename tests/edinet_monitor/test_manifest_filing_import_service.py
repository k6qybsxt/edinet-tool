from __future__ import annotations

import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.collector.manifest_filing_import_service import (  # noqa: E402
    build_filing_record_from_manifest_row,
    load_manifest_rows_for_filing_sync,
    resolve_manifest_paths,
    upsert_manifest_filing_records,
)
from edinet_monitor.services.storage.manifest_service import write_manifest_rows  # noqa: E402


def make_tempdir() -> Path:
    base_dir = ROOT_DIR / "tests" / "_tmp_edinet_monitor"
    base_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = base_dir / f"case_{uuid.uuid4().hex}"
    temp_dir.mkdir()
    return temp_dir


def create_filings_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE filings (
            doc_id TEXT PRIMARY KEY,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            form_type TEXT NOT NULL,
            period_end TEXT,
            submit_date TEXT,
            amendment_flag INTEGER NOT NULL DEFAULT 0,
            doc_info_edit_status TEXT,
            legal_status TEXT,
            accounting_standard TEXT,
            document_display_unit TEXT,
            zip_path TEXT,
            xbrl_path TEXT,
            download_status TEXT NOT NULL,
            parse_status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


class ManifestFilingImportServiceTest(unittest.TestCase):
    def test_resolve_manifest_paths_returns_sorted_jsonl_files(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        (tmpdir / "b.jsonl").write_text("", encoding="utf-8")
        (tmpdir / "a.jsonl").write_text("", encoding="utf-8")
        (tmpdir / "ignore.txt").write_text("", encoding="utf-8")

        paths = resolve_manifest_paths(manifest_root=tmpdir)

        self.assertEqual([path.name for path in paths], ["a.jsonl", "b.jsonl"])

    def test_load_manifest_rows_for_filing_sync_prefers_downloaded_row(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        manifest_a = tmpdir / "document_manifest_2026-04-01.jsonl"
        manifest_b = tmpdir / "document_manifest_2026-04.jsonl"
        write_manifest_rows(
            manifest_a,
            [
                {
                    "doc_id": "S100AAAA",
                    "submit_date": "2026-04-01 09:00",
                    "download_status": "pending",
                }
            ],
        )
        write_manifest_rows(
            manifest_b,
            [
                {
                    "doc_id": "S100AAAA",
                    "submit_date": "2026-04-01 09:00",
                    "download_status": "downloaded",
                }
            ],
        )

        rows = load_manifest_rows_for_filing_sync([manifest_a, manifest_b])

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["download_status"], "downloaded")

    def test_build_filing_record_from_manifest_row_marks_xbrl_ready_when_file_exists(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        xbrl_root = tmpdir / "xbrl"
        xbrl_path = xbrl_root / "2026-04-01" / "S100AAAA.xbrl"
        xbrl_path.parent.mkdir(parents=True, exist_ok=True)
        xbrl_path.write_text("dummy", encoding="utf-8")

        with patch("edinet_monitor.services.storage.path_service.XBRL_ROOT", xbrl_root):
            filing = build_filing_record_from_manifest_row(
                {
                    "doc_id": "S100AAAA",
                    "edinet_code": "E00001",
                    "security_code": "12340",
                    "form_code": "030000",
                    "period_end": "2026-03-31",
                    "submit_date": "2026-04-01 09:00",
                    "download_status": "pending",
                },
                timestamp_text="2026-04-10 10:00:00",
            )

        self.assertEqual(filing["download_status"], "downloaded")
        self.assertEqual(filing["parse_status"], "xbrl_ready")
        self.assertEqual(filing["xbrl_path"], str(xbrl_path))

    def test_upsert_manifest_filing_records_preserves_later_parse_status(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        create_filings_table(conn)
        conn.execute(
            """
            INSERT INTO filings (
                doc_id,
                edinet_code,
                security_code,
                form_type,
                period_end,
                submit_date,
                amendment_flag,
                doc_info_edit_status,
                legal_status,
                accounting_standard,
                document_display_unit,
                zip_path,
                xbrl_path,
                download_status,
                parse_status,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "S100AAAA",
                "E00001",
                "12340",
                "030000",
                "2026-03-31",
                "2026-04-01 09:00",
                0,
                "0",
                "1",
                "Japan GAAP",
                "百万円",
                r"D:\EDINET_Data\edinet_monitor\raw\zip\2026-04-01\S100AAAA.zip",
                r"D:\EDINET_Data\edinet_monitor\raw\xbrl\2026-04-01\S100AAAA.xbrl",
                "downloaded",
                "raw_facts_saved",
                "2026-04-10 10:00:00",
                "2026-04-10 10:00:00",
            ),
        )
        conn.commit()

        summary = upsert_manifest_filing_records(
            conn,
            [
                {
                    "doc_id": "S100AAAA",
                    "edinet_code": "E00001",
                    "security_code": "12340",
                    "form_type": "030000",
                    "period_end": "2026-03-31",
                    "submit_date": "2026-04-01 09:00",
                    "amendment_flag": 0,
                    "doc_info_edit_status": "0",
                    "legal_status": "2",
                    "accounting_standard": "",
                    "document_display_unit": "",
                    "zip_path": "",
                    "xbrl_path": "",
                    "download_status": "pending",
                    "parse_status": "pending",
                    "created_at": "2026-04-11 09:00:00",
                    "updated_at": "2026-04-11 09:00:00",
                }
            ],
        )

        row = conn.execute(
            "SELECT legal_status, accounting_standard, document_display_unit, parse_status, download_status FROM filings WHERE doc_id = ?",
            ("S100AAAA",),
        ).fetchone()

        self.assertEqual(summary["updated_rows"], 1)
        self.assertEqual(row[0], "2")
        self.assertEqual(row[1], "Japan GAAP")
        self.assertEqual(row[2], "百万円")
        self.assertEqual(row[3], "raw_facts_saved")
        self.assertEqual(row[4], "downloaded")


if __name__ == "__main__":
    unittest.main()
