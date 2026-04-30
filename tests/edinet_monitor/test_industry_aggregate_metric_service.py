from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.industry_aggregate_metric_service import (  # noqa: E402
    build_industry_aggregate_metric_rows,
    replace_industry_aggregate_metrics,
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

        CREATE TABLE industry_aggregate_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            industry_33 TEXT NOT NULL,
            period_scope TEXT NOT NULL,
            fiscal_year INTEGER NOT NULL,
            period_bucket_start TEXT,
            period_bucket_end TEXT,
            metric_key TEXT NOT NULL,
            metric_base TEXT NOT NULL,
            metric_group TEXT NOT NULL,
            value_num REAL,
            value_unit TEXT NOT NULL,
            calc_status TEXT NOT NULL,
            formula_name TEXT NOT NULL,
            source_company_count INTEGER NOT NULL DEFAULT 0,
            source_detail_json TEXT,
            rule_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX uq_industry_aggregate_metrics_scope
        ON industry_aggregate_metrics(industry_33, period_scope, fiscal_year, metric_key);
        """
    )


def _insert_company_year(
    conn: sqlite3.Connection,
    *,
    edinet_code: str,
    security_code: str,
    company_name: str,
    period_end: str,
    values: dict[str, float],
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO issuer_master (
            edinet_code, security_code, company_name, market, industry_33, industry_17,
            is_listed, exchange, listing_category_raw, listing_source, updated_at
        ) VALUES (?, ?, ?, 'Prime', '化学', '化学', 1, 'TSE', 'Prime', 'csv', '2026-04-30')
        """,
        (edinet_code, f"{security_code}0", company_name),
    )
    doc_id = f"{edinet_code}_{period_end}"
    conn.execute(
        """
        INSERT INTO filings (
            doc_id, edinet_code, security_code, form_type, period_end, submit_date,
            amendment_flag, doc_info_edit_status, legal_status, accounting_standard,
            document_display_unit, zip_path, xbrl_path, download_status, parse_status,
            created_at, updated_at
        ) VALUES (?, ?, ?, '030000', ?, ?, 0, '0', '1', 'Japan GAAP',
                  '百万円', 'zip', 'xbrl', 'downloaded', 'derived_metrics_saved',
                  '2026-04-30', '2026-04-30')
        """,
        (doc_id, edinet_code, f"{security_code}0", period_end, f"{period_end} 12:00"),
    )
    conn.executemany(
        """
        INSERT INTO normalized_metrics (
            doc_id, edinet_code, security_code, metric_key, fiscal_year, period_end,
            value_num, source_tag, consolidation, rule_version, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'tag', 'consolidated', 'v1', '2026-04-30', '2026-04-30')
        """,
        [
            (
                doc_id,
                edinet_code,
                f"{security_code}0",
                f"{base}Current",
                int(period_end[:4]),
                period_end,
                value,
            )
            for base, value in values.items()
        ],
    )


class IndustryAggregateMetricServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        _create_schema(self.conn)
        _insert_company_year(
            self.conn,
            edinet_code="E00001",
            security_code="1111",
            company_name="A社",
            period_end="2026-03-31",
            values={
                "NetSales": 100.0,
                "OrdinaryIncome": 20.0,
                "TotalAssets": 200.0,
                "NetAssets": 100.0,
                "CashAndCashEquivalents": 50.0,
                "OutstandingShares": 10.0,
                "NumberOfEmployees": 10.0,
                "AverageAge": 40.0,
                "AverageLengthOfService": 10.0,
                "AverageAnnualSalary": 5_000_000.0,
            },
        )
        _insert_company_year(
            self.conn,
            edinet_code="E00002",
            security_code="2222",
            company_name="B社",
            period_end="2026-03-31",
            values={
                "NetSales": 200.0,
                "OrdinaryIncome": 40.0,
                "TotalAssets": 300.0,
                "NetAssets": 150.0,
                "CashAndCashEquivalents": 60.0,
                "OutstandingShares": 20.0,
                "NumberOfEmployees": 30.0,
                "AverageAge": 50.0,
                "AverageLengthOfService": 20.0,
                "AverageAnnualSalary": 6_000_000.0,
            },
        )
        _insert_company_year(
            self.conn,
            edinet_code="E00001",
            security_code="1111",
            company_name="A社",
            period_end="2025-03-31",
            values={
                "NetSales": 50.0,
                "OrdinaryIncome": 10.0,
                "TotalAssets": 100.0,
                "NetAssets": 50.0,
                "CashAndCashEquivalents": 25.0,
                "OutstandingShares": 10.0,
            },
        )
        _insert_company_year(
            self.conn,
            edinet_code="E00002",
            security_code="2222",
            company_name="B社",
            period_end="2025-03-31",
            values={
                "NetSales": 100.0,
                "OrdinaryIncome": 20.0,
                "TotalAssets": 150.0,
                "NetAssets": 75.0,
                "CashAndCashEquivalents": 30.0,
                "OutstandingShares": 20.0,
            },
        )
        for period_end, company_values in [
            (
                "2022-03-31",
                [
                    ("E00001", "1111", "A社", {"OrdinaryIncome": 5.0, "NetAssets": 25.0, "OutstandingShares": 10.0}),
                    ("E00002", "2222", "B社", {"OrdinaryIncome": 10.0, "NetAssets": 50.0, "OutstandingShares": 20.0}),
                ],
            ),
            (
                "2017-03-31",
                [
                    ("E00001", "1111", "A社", {"OrdinaryIncome": 2.0, "NetAssets": 10.0, "OutstandingShares": 10.0}),
                    ("E00002", "2222", "B社", {"OrdinaryIncome": 4.0, "NetAssets": 20.0, "OutstandingShares": 20.0}),
                ],
            ),
        ]:
            for edinet_code, security_code, company_name, values in company_values:
                _insert_company_year(
                    self.conn,
                    edinet_code=edinet_code,
                    security_code=security_code,
                    company_name=company_name,
                    period_end=period_end,
                    values=values,
                )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def _row_by_base_year(self, rows: list[dict], base: str, fiscal_year: int) -> dict:
        return next(row for row in rows if row["metric_base"] == base and row["fiscal_year"] == fiscal_year)

    def test_builds_industry_metrics_without_roic(self) -> None:
        result = build_industry_aggregate_metric_rows(self.conn, rule_version="test")
        rows = result.rows

        self.assertNotIn("ROIC", {row["metric_base"] for row in rows})
        self.assertEqual(result.industry_count, 1)
        self.assertEqual(result.source_company_count, 2)
        self.assertAlmostEqual(self._row_by_base_year(rows, "NetSales", 2026)["value_num"], 300.0)
        self.assertAlmostEqual(self._row_by_base_year(rows, "EPS", 2026)["value_num"], 1.4)
        self.assertAlmostEqual(self._row_by_base_year(rows, "BPS", 2026)["value_num"], 250.0 / 30.0)
        self.assertAlmostEqual(self._row_by_base_year(rows, "EPSGrowthRate", 2026)["value_num"], 2.0)
        self.assertAlmostEqual(self._row_by_base_year(rows, "EPSGrowthRate5Year", 2026)["value_num"], 4.0)
        self.assertAlmostEqual(self._row_by_base_year(rows, "EPSGrowthRate10Year", 2026)["value_num"], 10.0)
        self.assertAlmostEqual(self._row_by_base_year(rows, "BPSGrowthRate", 2026)["value_num"], 2.0)
        self.assertAlmostEqual(self._row_by_base_year(rows, "BPSGrowthRate5Year", 2026)["value_num"], 250.0 / 75.0)
        self.assertAlmostEqual(self._row_by_base_year(rows, "BPSGrowthRate10Year", 2026)["value_num"], 250.0 / 30.0)
        self.assertAlmostEqual(self._row_by_base_year(rows, "NetSalesGrowthRate", 2026)["value_num"], 2.0)
        self.assertAlmostEqual(self._row_by_base_year(rows, "ROA", 2026)["value_num"], 42.0 / 500.0)
        self.assertAlmostEqual(self._row_by_base_year(rows, "ROE", 2026)["value_num"], 42.0 / 250.0)
        self.assertAlmostEqual(self._row_by_base_year(rows, "EquityRatio", 2026)["value_num"], 0.5)
        self.assertAlmostEqual(self._row_by_base_year(rows, "AverageAge", 2026)["value_num"], 47.5)
        self.assertAlmostEqual(
            self._row_by_base_year(rows, "AverageAnnualSalary", 2026)["value_num"],
            5_750_000.0,
        )

    def test_replace_is_idempotent(self) -> None:
        result = build_industry_aggregate_metric_rows(self.conn, rule_version="test")
        first_count = replace_industry_aggregate_metrics(self.conn, result.rows)
        second_count = replace_industry_aggregate_metrics(self.conn, result.rows)
        stored_count = self.conn.execute("SELECT COUNT(*) FROM industry_aggregate_metrics").fetchone()[0]

        self.assertEqual(first_count, second_count)
        self.assertEqual(stored_count, second_count)


if __name__ == "__main__":
    unittest.main()
