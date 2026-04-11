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


if __name__ == "__main__":
    unittest.main()
