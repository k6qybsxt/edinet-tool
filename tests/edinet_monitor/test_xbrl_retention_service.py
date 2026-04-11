from __future__ import annotations

import shutil
import sqlite3
import sys
import unittest
import uuid
from datetime import date
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.storage.xbrl_retention_service import (  # noqa: E402
    cleanup_old_xbrl_storage,
    detect_latest_filing_month,
    resolve_xbrl_keep_from_month,
)


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
            submit_date TEXT,
            zip_path TEXT,
            xbrl_path TEXT,
            parse_status TEXT,
            accounting_standard TEXT,
            document_display_unit TEXT
        )
        """
    )
    conn.commit()


class XbrlRetentionServiceTest(unittest.TestCase):
    def test_detect_latest_filing_month_uses_submit_date(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        create_filings_table(conn)
        conn.executemany(
            """
            INSERT INTO filings (
                doc_id, submit_date, zip_path, xbrl_path, parse_status, accounting_standard, document_display_unit
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("S100AAAA", "2026-02-25 09:00", "", "", "pending", "", ""),
                ("S100BBBB", "2026-03-31 09:00", "", "", "pending", "", ""),
            ],
        )
        conn.commit()

        latest_month = detect_latest_filing_month(conn)

        self.assertEqual(str(latest_month), "2026-03-01")

    def test_resolve_xbrl_keep_from_month_keeps_three_month_window(self) -> None:
        keep_from_month = resolve_xbrl_keep_from_month(
            latest_month=date(2026, 3, 1),
            keep_months=3,
        )
        self.assertEqual(str(keep_from_month), "2026-01-01")

    def test_cleanup_old_xbrl_storage_deletes_only_completed_old_rows(self) -> None:
        tmpdir = make_tempdir()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        old_xbrl = tmpdir / "old.xbrl"
        old_xbrl.write_text("old", encoding="utf-8")
        recent_xbrl = tmpdir / "recent.xbrl"
        recent_xbrl.write_text("recent", encoding="utf-8")
        pending_xbrl = tmpdir / "pending.xbrl"
        pending_xbrl.write_text("pending", encoding="utf-8")

        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        create_filings_table(conn)
        conn.executemany(
            """
            INSERT INTO filings (
                doc_id, submit_date, zip_path, xbrl_path, parse_status, accounting_standard, document_display_unit
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "S100OLD1",
                    "2025-12-25 09:00",
                    r"D:\EDINET_Data\edinet_monitor\raw\zip\2025-12-25\S100OLD1.zip",
                    str(old_xbrl),
                    "derived_metrics_saved",
                    "Japan GAAP",
                    "百万円",
                ),
                (
                    "S100NEW1",
                    "2026-02-25 09:00",
                    r"D:\EDINET_Data\edinet_monitor\raw\zip\2026-02-25\S100NEW1.zip",
                    str(recent_xbrl),
                    "derived_metrics_saved",
                    "Japan GAAP",
                    "百万円",
                ),
                (
                    "S100PEND",
                    "2025-12-20 09:00",
                    r"D:\EDINET_Data\edinet_monitor\raw\zip\2025-12-20\S100PEND.zip",
                    str(pending_xbrl),
                    "normalized_metrics_saved",
                    "Japan GAAP",
                    "百万円",
                ),
            ],
        )
        conn.commit()

        summary = cleanup_old_xbrl_storage(
            conn,
            enabled=True,
            keep_months=3,
            latest_month=date(2026, 3, 1),
        )

        rows = conn.execute(
            "SELECT doc_id, xbrl_path FROM filings ORDER BY doc_id ASC"
        ).fetchall()
        by_doc_id = {row[0]: row[1] for row in rows}

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["reference_month"], "2026-03")
        self.assertEqual(summary["keep_from_month"], "2026-01")
        self.assertEqual(summary["target_total"], 1)
        self.assertEqual(summary["deleted_total"], 1)
        self.assertEqual(summary["missing_file_total"], 0)
        self.assertFalse(old_xbrl.exists())
        self.assertTrue(recent_xbrl.exists())
        self.assertTrue(pending_xbrl.exists())
        self.assertEqual(by_doc_id["S100OLD1"], "")
        self.assertEqual(by_doc_id["S100NEW1"], str(recent_xbrl))
        self.assertEqual(by_doc_id["S100PEND"], str(pending_xbrl))

    def test_cleanup_old_xbrl_storage_skips_when_disabled(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        create_filings_table(conn)

        summary = cleanup_old_xbrl_storage(conn, enabled=False)

        self.assertEqual(summary["status"], "skipped")
        self.assertEqual(summary["reason"], "disabled")


if __name__ == "__main__":
    unittest.main()
