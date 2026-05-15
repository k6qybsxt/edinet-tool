from __future__ import annotations

import shutil
import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_quarter_standalone"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.quarter_standalone_metric_service import (  # noqa: E402
    save_quarter_standalone_metrics,
)


def _create_schema(conn: sqlite3.Connection) -> None:
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

        CREATE TABLE jquants_financial_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            disclosure_number TEXT NOT NULL,
            local_code TEXT NOT NULL,
            security_code TEXT,
            edinet_code TEXT,
            metric_kind TEXT NOT NULL,
            period_scope TEXT NOT NULL,
            period_key TEXT NOT NULL,
            quarter_type TEXT,
            forecast_target TEXT,
            forecast_stage TEXT,
            fiscal_year INTEGER,
            period_start TEXT,
            period_end TEXT,
            disclosed_date TEXT,
            disclosed_time TEXT,
            metric_key TEXT NOT NULL,
            metric_base TEXT NOT NULL,
            metric_group TEXT NOT NULL,
            value_num REAL,
            value_unit TEXT NOT NULL,
            calc_status TEXT NOT NULL,
            source_field TEXT NOT NULL,
            source_detail_json TEXT,
            rule_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE quarter_standalone_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            security_code TEXT NOT NULL,
            edinet_code TEXT,
            fiscal_year INTEGER NOT NULL,
            quarter_type TEXT NOT NULL,
            period_end TEXT,
            metric_key TEXT NOT NULL,
            metric_base TEXT NOT NULL,
            metric_group TEXT NOT NULL,
            value_num REAL,
            value_unit TEXT NOT NULL,
            calc_status TEXT NOT NULL,
            formula_name TEXT NOT NULL,
            source_detail_json TEXT,
            rule_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX uq_quarter_standalone_metrics_scope
        ON quarter_standalone_metrics(security_code, fiscal_year, quarter_type, metric_key);
        """
    )


def _insert_issuer(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT INTO issuer_master (
            edinet_code, security_code, company_name, market, industry_33,
            industry_17, is_listed, exchange, updated_at
        )
        VALUES ('E00001', '7203', 'Test Motors', 'プライム', '輸送用機器', '', 1, 'TSE', 'now')
        """
    )


def _insert_edinet_metric(
    conn: sqlite3.Connection,
    *,
    doc_id: str,
    form_type: str,
    period_end: str,
    value: float,
) -> None:
    conn.execute(
        """
        INSERT INTO filings (
            doc_id, edinet_code, security_code, form_type, period_end, submit_date,
            download_status, parse_status, created_at, updated_at
        )
        VALUES (?, 'E00001', '7203', ?, ?, ?, 'downloaded', 'derived_metrics_saved', 'now', 'now')
        """,
        (doc_id, form_type, period_end, period_end),
    )
    conn.execute(
        """
        INSERT INTO derived_metrics (
            doc_id, edinet_code, security_code, metric_key, metric_base, metric_group,
            fiscal_year, period_end, period_scope, value_num, value_unit, calc_status,
            formula_name, rule_version, created_at, updated_at
        )
        VALUES (?, 'E00001', '7203', 'NetSalesCurrent', 'NetSales', 'sales',
                CAST(substr(?, 1, 4) AS INTEGER), ?, ?, ?, 'yen', 'ok',
                'fixture', 'test', 'now', 'now')
        """,
        (doc_id, period_end, period_end, "annual" if form_type == "030000" else "quarter", value),
    )


def _insert_jquants_metric(
    conn: sqlite3.Connection,
    *,
    disclosure_number: str,
    fiscal_year: int,
    quarter_type: str,
    period_end: str,
    value: float,
) -> None:
    conn.execute(
        """
        INSERT INTO jquants_financial_metrics (
            disclosure_number, local_code, security_code, edinet_code, metric_kind,
            period_scope, period_key, quarter_type, forecast_target, forecast_stage,
            fiscal_year, period_start, period_end, disclosed_date, disclosed_time,
            metric_key, metric_base, metric_group, value_num, value_unit, calc_status,
            source_field, rule_version, created_at, updated_at
        )
        VALUES (?, '7203', '7203', 'E00001', 'actual',
                'quarter', ?, ?, NULL, NULL,
                ?, NULL, ?, ?, '15:00',
                'NetSalesCurrent', 'NetSales', 'sales', ?, 'yen', 'ok',
                'Sales', 'test', 'now', 'now')
        """,
        (disclosure_number, f"actual:{quarter_type}", quarter_type, fiscal_year, period_end, period_end, value),
    )


class QuarterStandaloneMetricServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        _create_schema(self.conn)
        _insert_issuer(self.conn)

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(TMP_ROOT, ignore_errors=True)

    def test_builds_quarter_standalone_values_and_growth(self) -> None:
        _insert_jquants_metric(
            self.conn,
            disclosure_number="fy2025q1",
            fiscal_year=2025,
            quarter_type="1Q",
            period_end="2024-06-30",
            value=50,
        )
        _insert_edinet_metric(self.conn, doc_id="fy2025h1", form_type="043A00", period_end="2024-09-30", value=150)
        _insert_jquants_metric(
            self.conn,
            disclosure_number="fy2025q3",
            fiscal_year=2025,
            quarter_type="3Q",
            period_end="2024-12-31",
            value=260,
        )
        _insert_edinet_metric(self.conn, doc_id="fy2025", form_type="030000", period_end="2025-03-31", value=360)

        _insert_jquants_metric(
            self.conn,
            disclosure_number="fy2026q1",
            fiscal_year=2026,
            quarter_type="1Q",
            period_end="2025-06-30",
            value=100,
        )
        _insert_edinet_metric(self.conn, doc_id="fy2026h1", form_type="043A00", period_end="2025-09-30", value=220)
        _insert_jquants_metric(
            self.conn,
            disclosure_number="fy2026q3",
            fiscal_year=2026,
            quarter_type="3Q",
            period_end="2025-12-31",
            value=370,
        )
        _insert_edinet_metric(self.conn, doc_id="fy2026", form_type="030000", period_end="2026-03-31", value=500)

        result = save_quarter_standalone_metrics(
            self.conn,
            codes=["7203"],
            output_dir=TMP_ROOT,
        )

        rows = {
            (row.fiscal_year, row.quarter_type, row.metric_base): row
            for row in result.rows
            if row.metric_base in {"NetSales", "NetSalesGrowthRate"}
        }
        self.assertEqual(rows[(2026, "1Q", "NetSales")].value_num, 100)
        self.assertEqual(rows[(2026, "2Q", "NetSales")].value_num, 120)
        self.assertEqual(rows[(2026, "3Q", "NetSales")].value_num, 150)
        self.assertEqual(rows[(2026, "4Q", "NetSales")].value_num, 130)
        self.assertAlmostEqual(rows[(2026, "2Q", "NetSalesGrowthRate")].value_num or 0, 1.2)
        self.assertTrue(result.output_path.exists())

    def test_apply_is_idempotent(self) -> None:
        _insert_jquants_metric(
            self.conn,
            disclosure_number="fy2026q1",
            fiscal_year=2026,
            quarter_type="1Q",
            period_end="2025-06-30",
            value=100,
        )

        save_quarter_standalone_metrics(self.conn, codes=["7203"], apply=True, output_dir=TMP_ROOT)
        save_quarter_standalone_metrics(self.conn, codes=["7203"], apply=True, output_dir=TMP_ROOT)

        count = self.conn.execute("SELECT COUNT(*) FROM quarter_standalone_metrics").fetchone()[0]
        self.assertEqual(count, 8)


if __name__ == "__main__":
    unittest.main()
