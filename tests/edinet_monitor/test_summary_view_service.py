from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.db.schema import ensure_summary_views  # noqa: E402
from edinet_monitor.services.summary_view_service import (  # noqa: E402
    fetch_latest_filing_status_rows,
    fetch_metric_coverage_rows,
    fetch_monthly_collection_status_rows,
    fetch_screening_hit_summary_rows,
    fetch_table_counts,
)


def create_base_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE issuer_master (
            edinet_code TEXT PRIMARY KEY,
            security_code TEXT,
            company_name TEXT NOT NULL,
            market TEXT,
            industry_33 TEXT,
            industry_17 TEXT,
            is_listed INTEGER NOT NULL DEFAULT 1,
            exchange TEXT,
            listing_category_raw TEXT,
            listing_source TEXT,
            updated_at TEXT NOT NULL
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

        CREATE TABLE screening_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            screening_date TEXT NOT NULL,
            rule_name TEXT NOT NULL,
            rule_version TEXT NOT NULL,
            target_count INTEGER NOT NULL DEFAULT 0,
            hit_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );

        CREATE TABLE screening_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            screening_run_id INTEGER NOT NULL,
            screening_date TEXT NOT NULL,
            rule_name TEXT NOT NULL,
            rule_version TEXT NOT NULL,
            edinet_code TEXT NOT NULL,
            security_code TEXT,
            company_name TEXT,
            period_end TEXT,
            result_flag INTEGER NOT NULL,
            score REAL,
            detail_json TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            screening_result_id INTEGER NOT NULL,
            notify_type TEXT NOT NULL,
            notify_status TEXT NOT NULL,
            sent_at TEXT,
            created_at TEXT NOT NULL
        );
        """
    )
    conn.commit()


class SummaryViewServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        create_base_tables(self.conn)

        self.conn.execute(
            """
            INSERT INTO issuer_master (
                edinet_code, security_code, company_name, market, industry_33, industry_17,
                is_listed, exchange, listing_category_raw, listing_source, updated_at
            ) VALUES
            ('E00001', '11110', 'Alpha', 'Prime', 'Tech', 'IT', 1, 'TSE', 'Prime', 'csv', '2026-04-11 00:00:00'),
            ('E00002', '22220', 'Beta', 'Standard', 'Retail', 'Retail', 1, 'TSE', 'Standard', 'csv', '2026-04-11 00:00:00')
            """
        )

        self.conn.execute(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end, submit_date,
                amendment_flag, doc_info_edit_status, legal_status, accounting_standard,
                document_display_unit, zip_path, xbrl_path, download_status, parse_status,
                created_at, updated_at
            ) VALUES
            ('S100A001', 'E00001', '11110', '030000', '2025-03-31', '2025-06-25 15:00', 0, '0', '1', 'Japan GAAP', '百万円', 'zip1', 'xbrl1', 'downloaded', 'derived_metrics_saved', '2026-04-11 00:00:00', '2026-04-11 00:00:00'),
            ('S100A002', 'E00001', '11110', '030000', '2026-03-31', '2026-04-09 14:38', 0, '0', '1', 'Japan GAAP', '千円', 'zip2', 'xbrl2', 'downloaded', 'derived_metrics_saved', '2026-04-11 00:00:00', '2026-04-11 00:00:00'),
            ('S100B001', 'E00002', '22220', '030000', '2025-12-31', '2026-03-31 10:00', 0, '0', '2', 'IFRS', '千円', 'zip3', '', 'downloaded', 'normalized_metrics_saved', '2026-04-11 00:00:00', '2026-04-11 00:00:00')
            """
        )

        self.conn.execute(
            """
            INSERT INTO normalized_metrics (
                doc_id, edinet_code, security_code, metric_key, fiscal_year, period_end,
                value_num, source_tag, consolidation, rule_version, created_at, updated_at
            ) VALUES
            ('S100A002', 'E00001', '11110', 'NetSalesCurrent', 2025, '2026-03-31', 100.0, 'NetSales', 'consolidated', 'v1', '2026-04-11 00:00:00', '2026-04-11 00:00:00'),
            ('S100A002', 'E00001', '11110', 'OrdinaryIncomeCurrent', 2025, '2026-03-31', 10.0, 'OrdinaryIncome', 'consolidated', 'v1', '2026-04-11 00:00:00', '2026-04-11 00:00:00'),
            ('S100B001', 'E00002', '22220', 'NetSalesCurrent', 2025, '2025-12-31', 200.0, 'NetSales', 'consolidated', 'v1', '2026-04-11 00:00:00', '2026-04-11 00:00:00')
            """
        )

        self.conn.execute(
            """
            INSERT INTO derived_metrics (
                doc_id, edinet_code, security_code, metric_key, metric_base, metric_group,
                fiscal_year, period_end, period_scope, period_offset, consolidation,
                accounting_standard, document_display_unit, value_num, value_unit, calc_status,
                formula_name, source_detail_json, rule_version, created_at, updated_at
            ) VALUES
            ('S100A002', 'E00001', '11110', 'EquityRatioCurrent', 'EquityRatio', 'safety', 2025, '2026-03-31', 'annual', 0, 'consolidated', 'Japan GAAP', '千円', 0.5, 'ratio', 'ok', 'ratio', '{}', 'v1', '2026-04-11 00:00:00', '2026-04-11 00:00:00'),
            ('S100A002', 'E00001', '11110', 'FCFCurrent', 'FCF', 'cashflow', 2025, '2026-03-31', 'annual', 0, 'consolidated', 'Japan GAAP', '千円', 1000.0, 'yen', 'ok', 'sum', '{}', 'v1', '2026-04-11 00:00:00', '2026-04-11 00:00:00'),
            ('S100B001', 'E00002', '22220', 'EquityRatioCurrent', 'EquityRatio', 'safety', 2025, '2025-12-31', 'annual', 0, 'consolidated', 'IFRS', '千円', NULL, 'ratio', 'missing_input', 'ratio', '{}', 'v1', '2026-04-11 00:00:00', '2026-04-11 00:00:00')
            """
        )

        self.conn.execute(
            """
            INSERT INTO screening_runs (
                id, screening_date, rule_name, rule_version, target_count, hit_count, created_at
            ) VALUES
            (1, '2026-04-11', 'annual_growth_quality_check', 'v1', 2, 1, '2026-04-11 00:00:00')
            """
        )

        self.conn.execute(
            """
            INSERT INTO screening_results (
                screening_run_id, screening_date, rule_name, rule_version, edinet_code,
                security_code, company_name, period_end, result_flag, score, detail_json, created_at
            ) VALUES
            (1, '2026-04-11', 'annual_growth_quality_check', 'v1', 'E00001', '11110', 'Alpha', '2026-03-31', 1, 3.5, '{}', '2026-04-11 00:00:00'),
            (1, '2026-04-11', 'annual_growth_quality_check', 'v1', 'E00002', '22220', 'Beta', '2025-12-31', 0, 1.0, '{}', '2026-04-11 00:00:00')
            """
        )
        self.conn.commit()
        ensure_summary_views(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def test_fetch_table_counts(self) -> None:
        counts = fetch_table_counts(self.conn)

        self.assertEqual(counts["issuer_master"], 2)
        self.assertEqual(counts["filings"], 3)
        self.assertEqual(counts["normalized_metrics"], 3)
        self.assertEqual(counts["derived_metrics"], 3)
        self.assertEqual(counts["screening_runs"], 1)

    def test_fetch_latest_filing_status_rows_picks_latest_doc(self) -> None:
        rows = fetch_latest_filing_status_rows(self.conn, limit=5)

        self.assertEqual(len(rows), 2)
        alpha_row = next(row for row in rows if row["edinet_code"] == "E00001")
        self.assertEqual(alpha_row["doc_id"], "S100A002")
        self.assertEqual(alpha_row["normalized_metric_count"], 2)
        self.assertEqual(alpha_row["derived_metric_ok_count"], 2)

    def test_fetch_monthly_collection_status_rows_aggregates_by_month(self) -> None:
        rows = fetch_monthly_collection_status_rows(self.conn, limit=5)

        latest_month = rows[0]
        self.assertEqual(latest_month["submit_month"], "2026-04")
        self.assertEqual(latest_month["filing_count"], 1)

    def test_fetch_metric_coverage_rows_supports_source_filter(self) -> None:
        rows = fetch_metric_coverage_rows(
            self.conn,
            metric_source="derived_metrics",
            metric_key_like="Equity%",
            limit=10,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["metric_key"], "EquityRatioCurrent")
        self.assertEqual(rows[0]["doc_count"], 2)
        self.assertEqual(rows[0]["ok_row_count"], 1)

    def test_fetch_screening_hit_summary_rows_returns_hit_ratio(self) -> None:
        rows = fetch_screening_hit_summary_rows(self.conn, limit=5)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["rule_name"], "annual_growth_quality_check")
        self.assertAlmostEqual(rows[0]["hit_ratio"], 0.5)
        self.assertAlmostEqual(rows[0]["avg_hit_score"], 3.5)


if __name__ == "__main__":
    unittest.main()
