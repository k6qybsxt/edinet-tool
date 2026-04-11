from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.collector.filing_parse_status_repair_service import (  # noqa: E402
    repair_filing_parse_statuses,
)


def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
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
        );

        CREATE TABLE raw_facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT NOT NULL,
            tag_name TEXT NOT NULL,
            context_ref TEXT,
            unit_ref TEXT,
            period_type TEXT,
            period_start TEXT,
            period_end TEXT,
            instant_date TEXT,
            consolidation TEXT,
            value_text TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE normalized_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT NOT NULL,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            metric_key TEXT NOT NULL,
            fiscal_year INTEGER,
            period_end TEXT,
            value_num REAL,
            source_tag TEXT,
            consolidation TEXT,
            rule_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE derived_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT NOT NULL,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            metric_key TEXT NOT NULL,
            metric_base TEXT NOT NULL,
            metric_group TEXT NOT NULL,
            fiscal_year INTEGER,
            period_end TEXT,
            period_scope TEXT NOT NULL,
            period_offset INTEGER NOT NULL DEFAULT 0,
            consolidation TEXT,
            accounting_standard TEXT,
            document_display_unit TEXT,
            value_num REAL,
            value_unit TEXT NOT NULL,
            calc_status TEXT NOT NULL,
            formula_name TEXT NOT NULL,
            source_detail_json TEXT,
            rule_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    conn.commit()


class FilingParseStatusRepairServiceTest(unittest.TestCase):
    def test_repair_filing_parse_statuses_promotes_to_highest_completed_stage(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        create_tables(conn)

        conn.execute(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end, submit_date,
                amendment_flag, doc_info_edit_status, legal_status, accounting_standard,
                document_display_unit, zip_path, xbrl_path, download_status, parse_status,
                created_at, updated_at
            ) VALUES
            ('S100A001', 'E00001', '11110', '030000', '2026-03-31', '2026-04-09 09:00', 0, '0', '1', 'Japan GAAP', '千円', 'zip1', '', 'downloaded', 'xbrl_ready', '2026-04-11 00:00:00', '2026-04-11 00:00:00'),
            ('S100A002', 'E00002', '22220', '030000', '2026-03-31', '2026-04-09 10:00', 0, '0', '1', 'Japan GAAP', '千円', 'zip2', '', 'downloaded', 'raw_facts_saved', '2026-04-11 00:00:00', '2026-04-11 00:00:00'),
            ('S100A003', 'E00003', '33330', '030000', '2026-03-31', '2026-04-09 11:00', 0, '0', '1', 'Japan GAAP', '千円', 'zip3', '', 'downloaded', 'normalized_metrics_saved', '2026-04-11 00:00:00', '2026-04-11 00:00:00'),
            ('S100A004', 'E00004', '44440', '030000', '2026-03-31', '2026-04-09 12:00', 0, '0', '1', 'Japan GAAP', '千円', 'zip4', '', 'downloaded', 'derived_metrics_saved', '2026-04-11 00:00:00', '2026-04-11 00:00:00')
            """
        )
        conn.execute(
            "INSERT INTO raw_facts (doc_id, tag_name, created_at) VALUES ('S100A001', 'NetSales', '2026-04-11 00:00:00')"
        )
        conn.execute(
            """
            INSERT INTO normalized_metrics (
                doc_id, edinet_code, security_code, metric_key, fiscal_year, period_end, value_num,
                source_tag, consolidation, rule_version, created_at, updated_at
            ) VALUES
            ('S100A002', 'E00002', '22220', 'NetSalesCurrent', 2025, '2026-03-31', 100.0, 'NetSales', 'consolidated', 'v1', '2026-04-11 00:00:00', '2026-04-11 00:00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO derived_metrics (
                doc_id, edinet_code, security_code, metric_key, metric_base, metric_group, fiscal_year,
                period_end, period_scope, period_offset, consolidation, accounting_standard,
                document_display_unit, value_num, value_unit, calc_status, formula_name,
                source_detail_json, rule_version, created_at, updated_at
            ) VALUES
            ('S100A003', 'E00003', '33330', 'EquityRatioCurrent', 'EquityRatio', 'safety', 2025,
             '2026-03-31', 'annual', 0, 'consolidated', 'Japan GAAP', '千円', 0.5, 'ratio', 'ok',
             'ratio', '{}', 'v1', '2026-04-11 00:00:00', '2026-04-11 00:00:00')
            """
        )
        conn.commit()

        summary = repair_filing_parse_statuses(conn)

        rows = conn.execute(
            "SELECT doc_id, parse_status FROM filings ORDER BY doc_id"
        ).fetchall()
        statuses = {row["doc_id"]: row["parse_status"] for row in rows}

        self.assertEqual(summary["updated_total"], 3)
        self.assertEqual(statuses["S100A001"], "raw_facts_saved")
        self.assertEqual(statuses["S100A002"], "normalized_metrics_saved")
        self.assertEqual(statuses["S100A003"], "derived_metrics_saved")
        self.assertEqual(statuses["S100A004"], "derived_metrics_saved")


if __name__ == "__main__":
    unittest.main()
