from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.company_export_service import export_company_latest_dataset  # noqa: E402


class CompanyExportServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
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
            """
        )
        self.conn.execute(
            """
            INSERT INTO issuer_master (
                edinet_code, security_code, company_name, market, industry_33, industry_17,
                is_listed, exchange, listing_category_raw, listing_source, updated_at
            ) VALUES (
                'E99999', '7203', 'テスト自動車', 'Prime', '輸送用機器', '輸送用機器',
                1, 'TSE', 'Prime', 'csv', '2026-04-11 00:00:00'
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO filings (
                doc_id, edinet_code, security_code, form_type, period_end, submit_date,
                amendment_flag, doc_info_edit_status, legal_status, accounting_standard,
                document_display_unit, zip_path, xbrl_path, download_status, parse_status,
                created_at, updated_at
            ) VALUES (
                'S100TEST', 'E99999', '72030', '030000', '2026-03-31', '2026-06-25 15:00',
                0, '0', '1', 'IFRS', '百万円', 'zip', 'xbrl', 'downloaded', 'derived_metrics_saved',
                '2026-04-11 00:00:00', '2026-04-11 00:00:00'
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO normalized_metrics (
                doc_id, edinet_code, security_code, metric_key, fiscal_year, period_end,
                value_num, source_tag, consolidation, rule_version, created_at, updated_at
            ) VALUES (
                'S100TEST', 'E99999', '72030', 'NetSalesCurrent', 2025, '2026-03-31',
                100000000.0, 'NetSalesIFRS', 'consolidated', 'v1',
                '2026-04-11 00:00:00', '2026-04-11 00:00:00'
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO derived_metrics (
                doc_id, edinet_code, security_code, metric_key, metric_base, metric_group,
                fiscal_year, period_end, period_scope, period_offset, consolidation,
                accounting_standard, document_display_unit, value_num, value_unit, calc_status,
                formula_name, source_detail_json, rule_version, created_at, updated_at
            ) VALUES (
                'S100TEST', 'E99999', '72030', 'EstimatedNetIncomeCurrent', 'EstimatedNetIncome', 'estimated',
                2025, '2026-03-31', 'annual', 0, 'consolidated',
                'IFRS', '百万円', 70000000.0, 'yen', 'ok',
                'estimate', '{}', 'v1', '2026-04-11 00:00:00', '2026-04-11 00:00:00'
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO raw_facts (
                doc_id, tag_name, context_ref, unit_ref, period_type, period_start, period_end,
                instant_date, consolidation, value_text, created_at
            ) VALUES (
                'S100TEST', 'NetSalesIFRS', 'CurrentYearDuration', 'JPY', 'duration', '2025-04-01', '2026-03-31',
                NULL, 'consolidated', '100000000', '2026-04-11 00:00:00'
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO screening_results (
                screening_run_id, screening_date, rule_name, rule_version, edinet_code, security_code,
                company_name, period_end, result_flag, score, detail_json, created_at
            ) VALUES (
                1, '2026-04-11', 'annual_growth_quality_check', 'v1', 'E99999', '7203',
                'テスト自動車', '2026-03-31', 1, 100.0, '{}', '2026-04-11 00:00:00'
            )
            """
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_export_company_latest_dataset_returns_japanese_labels(self) -> None:
        payload = export_company_latest_dataset(
            self.conn,
            security_code="7203",
            years=5,
            screening_limit=5,
        )

        self.assertEqual(payload["会社情報"]["company_name"], "テスト自動車")
        self.assertEqual(payload["件数"]["提出書類"], 1)
        self.assertEqual(payload["件数"]["生ファクト"], 1)

        normalized_row = payload["正規化指標"][0]
        self.assertEqual(normalized_row["metric_label"], "売上高（当期）")
        self.assertEqual(normalized_row["source_tag_label"], "売上高")
        self.assertEqual(normalized_row["display_value_num"], 100.0)

        derived_row = payload["派生指標"][0]
        self.assertEqual(
            derived_row["metric_label"],
            "推定純利益(経常利益*0.7)（当期）",
        )
        self.assertEqual(
            derived_row["metric_base_label"],
            "推定純利益(経常利益*0.7)",
        )
        self.assertEqual(derived_row["display_value_num"], 70.0)

        raw_fact_row = payload["生ファクト"][0]
        self.assertEqual(raw_fact_row["tag_label"], "売上高")


if __name__ == "__main__":
    unittest.main()
