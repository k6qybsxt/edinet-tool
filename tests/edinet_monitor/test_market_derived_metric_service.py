from __future__ import annotations

import sqlite3
import shutil
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_market_derived_metrics"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.market_derived_metric_service import (  # noqa: E402
    build_market_derived_metrics,
    save_market_derived_metrics,
    upsert_market_derived_metrics,
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
            period_key TEXT,
            quarter_type TEXT,
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

        CREATE TABLE jquants_daily_quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            local_code TEXT NOT NULL,
            security_code TEXT,
            trade_date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            turnover_value REAL,
            adjustment_factor REAL,
            adjustment_open REAL,
            adjustment_high REAL,
            adjustment_low REAL,
            adjustment_close REAL,
            adjustment_close_rounded REAL,
            adjustment_volume REAL,
            raw_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE market_derived_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            edinet_code TEXT,
            security_code TEXT NOT NULL,
            period_scope TEXT NOT NULL,
            period_key TEXT NOT NULL,
            quarter_type TEXT,
            fiscal_year INTEGER,
            period_end TEXT NOT NULL,
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
        CREATE UNIQUE INDEX uq_market_derived_metrics_scope
        ON market_derived_metrics(source_type, source_id, period_key, metric_key);
        """
    )


def _insert_quote(conn: sqlite3.Connection, code: str, trade_date: str, close: float) -> None:
    conn.execute(
        """
        INSERT INTO jquants_daily_quotes (
            local_code, security_code, trade_date, adjustment_close_rounded, created_at, updated_at
        ) VALUES (?, ?, ?, ?, '2026-05-08', '2026-05-08')
        """,
        (f"{code}0", code, trade_date, close),
    )


def _insert_filing(conn: sqlite3.Connection, doc_id: str, period_end: str, form_type: str = "030000") -> None:
    conn.execute(
        """
        INSERT INTO filings (
            doc_id, edinet_code, security_code, form_type, period_end, submit_date,
            download_status, parse_status, created_at, updated_at
        ) VALUES (?, 'E00001', '1111', ?, ?, ?, 'downloaded', 'derived_metrics_saved', '2026-05-08', '2026-05-08')
        """,
        (doc_id, form_type, period_end, period_end),
    )


def _insert_derived(conn: sqlite3.Connection, doc_id: str, base: str, value: float) -> None:
    conn.execute(
        """
        INSERT INTO derived_metrics (
            doc_id, edinet_code, security_code, metric_key, metric_base, metric_group,
            fiscal_year, period_end, period_scope, period_key, quarter_type, period_offset,
            value_num, value_unit, calc_status, formula_name, source_detail_json, rule_version,
            created_at, updated_at
        ) VALUES (
            ?, 'E00001', '1111', ?, ?, 'test', 2026, '2026-03-31', 'annual', 'annual:FY', NULL, 0,
            ?, 'ratio', 'ok', 'test', '{}', 'v1', '2026-05-08', '2026-05-08'
        )
        """,
        (doc_id, f"{base}Current", base, value),
    )


def _insert_jquants_metric(
    conn: sqlite3.Connection,
    disclosure_number: str,
    fiscal_year: int,
    quarter: str,
    period_end: str,
    base: str,
    value: float,
) -> None:
    conn.execute(
        """
        INSERT INTO jquants_financial_metrics (
            disclosure_number, local_code, security_code, edinet_code, metric_kind,
            period_scope, period_key, quarter_type, forecast_target, forecast_stage,
            fiscal_year, period_start, period_end, disclosed_date, disclosed_time,
            metric_key, metric_base, metric_group, value_num, value_unit, calc_status,
            source_field, source_detail_json, rule_version, created_at, updated_at
        ) VALUES (
            ?, '11110', '1111', 'E00001', 'actual',
            'quarter', ?, ?, NULL, NULL,
            ?, ?, ?, ?, '',
            ?, ?, 'test', ?, 'yen', 'ok',
            'fixture', '{}', 'v1', '2026-05-08', '2026-05-08'
        )
        """,
        (
            disclosure_number,
            f"actual:{quarter}",
            quarter,
            fiscal_year,
            f"{fiscal_year}-04-01",
            period_end,
            period_end,
            f"{base}Current",
            base,
            value,
        ),
    )


class MarketDerivedMetricServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        _create_schema(self.conn)
        self.tmp_path = TMP_ROOT / self.id().replace(".", "_")
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.conn.execute(
            """
            INSERT INTO issuer_master (
                edinet_code, security_code, company_name, industry_33, is_listed, exchange, updated_at
            ) VALUES ('E00001', '1111', 'Test', '化学', 1, 'TSE', '2026-05-08')
            """
        )

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_builds_edinet_market_metrics_with_previous_quote(self) -> None:
        _insert_filing(self.conn, "DOC2026", "2026-03-31")
        _insert_filing(self.conn, "DOC2025", "2025-03-31")
        for doc_id, eps, bps, ocfps, shares in (
            ("DOC2026", 100, 1000, 200, 1_000_000),
            ("DOC2025", 80, 800, 160, 1_000_000),
        ):
            _insert_derived(self.conn, doc_id, "EPS", eps)
            _insert_derived(self.conn, doc_id, "BPS", bps)
            _insert_derived(self.conn, doc_id, "OperatingCashPerShare", ocfps)
            _insert_derived(self.conn, doc_id, "OutstandingShares", shares)
        _insert_quote(self.conn, "1111", "2026-03-29", 1200)
        _insert_quote(self.conn, "1111", "2025-03-31", 1000)

        rows, missing_quotes, warnings = build_market_derived_metrics(
            self.conn,
            date_from="2026-01-01",
            date_to="2026-12-31",
        )

        by_base = {row["metric_base"]: row for row in rows if row["source_id"] == "DOC2026"}
        self.assertEqual(missing_quotes, 0)
        self.assertEqual(warnings, [])
        self.assertEqual(by_base["StockPrice"]["value_num"], 1200)
        self.assertEqual(by_base["MarketCapitalization"]["value_num"], 1_200_000_000)
        self.assertEqual(by_base["PBR"]["value_num"], 1.2)
        self.assertEqual(by_base["PER"]["value_num"], 12)
        self.assertEqual(by_base["PCFR"]["value_num"], 6)
        self.assertEqual(by_base["StockPriceGrowthRate"]["value_num"], 1.2)

    def test_jquants_quarter_excludes_pcfr_and_builds_theoretical_metrics(self) -> None:
        for disclosure, fiscal_year, period_end, ordinary_income in (
            ("DISC2026Q1", 2026, "2026-06-30", 1_000_000),
            ("DISC2025Q1", 2025, "2025-06-30", 800_000),
        ):
            for base, value in (
                ("OrdinaryIncome", ordinary_income),
                ("EPS", 70),
                ("TotalAssets", 10_000_000),
                ("NetAssets", 5_000_000),
                ("EquityRatio", 0.5),
                ("BPS", 500),
                ("OperatingCash", 300_000),
                ("OutstandingShares", 10_000),
            ):
                _insert_jquants_metric(self.conn, disclosure, fiscal_year, "1Q", period_end, base, value)
        _insert_quote(self.conn, "1111", "2026-06-30", 900)
        _insert_quote(self.conn, "1111", "2025-06-30", 600)

        rows, _, _ = build_market_derived_metrics(self.conn, period_scopes={"quarter"})
        current_rows = [row for row in rows if row["source_id"] == "DISC2026Q1"]
        by_base = {row["metric_base"]: row for row in current_rows}

        self.assertIn("StockPrice", by_base)
        self.assertIn("PBR", by_base)
        self.assertIn("PER", by_base)
        self.assertIn("TheoreticalSharePrice", by_base)
        self.assertIn("TheoreticalSharePriceGrowthRate", by_base)
        self.assertNotIn("PCFR", by_base)
        self.assertGreater(by_base["TheoreticalSharePrice"]["value_num"], 0)
        self.assertEqual(by_base["TheoreticalSharePriceGrowthRate"]["calc_status"], "ok")

    def test_upsert_is_idempotent_and_report_is_written(self) -> None:
        _insert_filing(self.conn, "DOC2026", "2026-03-31")
        for base, value in (
            ("EPS", 100),
            ("BPS", 1000),
            ("OperatingCashPerShare", 200),
            ("OutstandingShares", 1_000_000),
        ):
            _insert_derived(self.conn, "DOC2026", base, value)
        _insert_quote(self.conn, "1111", "2026-03-31", 1200)

        rows, _, _ = build_market_derived_metrics(self.conn)
        first_count = upsert_market_derived_metrics(self.conn, rows)
        second_count = upsert_market_derived_metrics(self.conn, rows)
        stored_count = self.conn.execute("SELECT COUNT(*) FROM market_derived_metrics").fetchone()[0]

        self.assertEqual(first_count, second_count)
        self.assertEqual(stored_count, first_count)

        result = save_market_derived_metrics(self.conn, output_dir=self.tmp_path)
        self.assertIsNotNone(result.output_path)
        self.assertTrue(result.output_path.exists())


if __name__ == "__main__":
    unittest.main()
