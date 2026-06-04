from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.collector.download_queue_service import (  # noqa: E402
    fetch_downloaded_filings_without_xbrl,
    mark_derived_metrics_saved_many,
    mark_xbrl_extract_success,
)


def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE issuer_master (
            edinet_code TEXT PRIMARY KEY,
            exchange TEXT,
            is_listed INTEGER NOT NULL
        );

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
            xbrl_member_name TEXT,
            download_status TEXT NOT NULL,
            parse_status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    conn.commit()


class DownloadQueueServiceTest(unittest.TestCase):
    def test_fetch_downloaded_filings_without_xbrl_excludes_completed_docs(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        create_tables(conn)

        conn.execute(
            """
            INSERT INTO issuer_master (edinet_code, exchange, is_listed) VALUES
            ('E00001', 'TSE', 1),
            ('E00002', 'TSE', 1),
            ('E00003', 'TSE', 1)
            """
        )
        conn.execute(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end, submit_date,
                amendment_flag, doc_info_edit_status, legal_status, accounting_standard,
                document_display_unit, zip_path, xbrl_path, download_status, parse_status,
                created_at, updated_at
            ) VALUES
            ('S100A001', 'E00001', '11110', '030000', '2026-03-31', '2026-04-09 09:00', 0, '0', '1', '', '', 'zip1', '', 'downloaded', 'pending', '2026-04-11 00:00:00', '2026-04-11 00:00:00'),
            ('S100A002', 'E00002', '22220', '030000', '2026-03-31', '2026-04-09 10:00', 0, '0', '1', 'Japan GAAP', '千円', 'zip2', '', 'downloaded', 'derived_metrics_saved', '2026-04-11 00:00:00', '2026-04-11 00:00:00'),
            ('S100A003', 'E00003', '33330', '030000', '2026-03-31', '2026-04-09 11:00', 0, '0', '1', '', '', 'zip3', '', 'downloaded', 'xbrl_extract_error', '2026-04-11 00:00:00', '2026-04-11 00:00:00')
            """
        )
        conn.commit()

        rows = fetch_downloaded_filings_without_xbrl(conn, limit=10)

        self.assertEqual([row["doc_id"] for row in rows], ["S100A001", "S100A003"])

        filtered_rows = fetch_downloaded_filings_without_xbrl(
            conn,
            limit=10,
            exclude_doc_ids={"S100A001"},
        )

        self.assertEqual([row["doc_id"] for row in filtered_rows], ["S100A003"])

    def test_mark_xbrl_extract_success_saves_member_name_when_column_exists(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        create_tables(conn)

        conn.execute(
            """
            INSERT INTO issuer_master (edinet_code, exchange, is_listed)
            VALUES ('E00001', 'TSE', 1)
            """
        )
        conn.execute(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end, submit_date,
                amendment_flag, doc_info_edit_status, legal_status, accounting_standard,
                document_display_unit, zip_path, xbrl_path, xbrl_member_name, download_status, parse_status,
                created_at, updated_at
            )
            VALUES (
                'S100A001', 'E00001', '11110', '030000', '2026-03-31', '2026-04-09 09:00',
                0, '0', '1', '', '', 'zip1', '', '', 'downloaded', 'pending',
                '2026-04-11 00:00:00', '2026-04-11 00:00:00'
            )
            """
        )
        conn.commit()

        mark_xbrl_extract_success(
            conn,
            "S100A001",
            r"D:\EDINET_Data\edinet_monitor\raw\xbrl\2026-04-09\S100A001.xbrl",
            "XBRL/PublicDoc/jpcrp030000-asr-001_E00001-000_2026-03-31_01_2026-06-28.xbrl",
        )

        row = conn.execute(
            "SELECT xbrl_path, xbrl_member_name, parse_status FROM filings WHERE doc_id = ?",
            ("S100A001",),
        ).fetchone()
        self.assertEqual(row["parse_status"], "xbrl_ready")
        self.assertEqual(
            row["xbrl_member_name"],
            "XBRL/PublicDoc/jpcrp030000-asr-001_E00001-000_2026-03-31_01_2026-06-28.xbrl",
        )
        self.assertTrue(str(row["xbrl_path"]).endswith("S100A001.xbrl"))

    def test_mark_derived_metrics_saved_many_chunks_targets_only(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        create_tables(conn)

        conn.execute(
            """
            INSERT INTO issuer_master (edinet_code, exchange, is_listed)
            VALUES ('E00001', 'TSE', 1)
            """
        )
        conn.execute(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end, submit_date,
                amendment_flag, doc_info_edit_status, legal_status, accounting_standard,
                document_display_unit, zip_path, xbrl_path, xbrl_member_name, download_status, parse_status,
                created_at, updated_at
            ) VALUES
            ('DOC1', 'E00001', '11110', '030000', '2026-03-31', '2026-04-09 09:00', 0, '0', '1', '', '', '', '', '', 'downloaded', 'normalized_metrics_saved', '2026-04-11', '2026-04-11'),
            ('DOC2', 'E00001', '11110', '030000', '2026-03-31', '2026-04-09 10:00', 0, '0', '1', '', '', '', '', '', 'downloaded', 'normalized_metrics_saved', '2026-04-11', '2026-04-11'),
            ('DOC3', 'E00001', '11110', '030000', '2026-03-31', '2026-04-09 11:00', 0, '0', '1', '', '', '', '', '', 'downloaded', 'normalized_metrics_saved', '2026-04-11', '2026-04-11'),
            ('DOC4', 'E00001', '11110', '030000', '2026-03-31', '2026-04-09 12:00', 0, '0', '1', '', '', '', '', '', 'downloaded', 'normalized_metrics_saved', '2026-04-11', '2026-04-11')
            """
        )
        conn.commit()

        updated_count = mark_derived_metrics_saved_many(
            conn,
            ["DOC1", "DOC2", "DOC3"],
            chunk_size=2,
        )
        statuses = {
            str(row["doc_id"]): str(row["parse_status"])
            for row in conn.execute(
                "SELECT doc_id, parse_status FROM filings ORDER BY doc_id"
            ).fetchall()
        }

        self.assertEqual(updated_count, 3)
        self.assertEqual(statuses["DOC1"], "derived_metrics_saved")
        self.assertEqual(statuses["DOC2"], "derived_metrics_saved")
        self.assertEqual(statuses["DOC3"], "derived_metrics_saved")
        self.assertEqual(statuses["DOC4"], "normalized_metrics_saved")


if __name__ == "__main__":
    unittest.main()
