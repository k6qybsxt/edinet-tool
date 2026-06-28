from __future__ import annotations

import shutil
import sqlite3
import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_jquants_audit_services"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.db.schema import ensure_summary_views  # noqa: E402
from edinet_monitor.services.jquants.audit_ingestion_service import save_jquants_listed_info  # noqa: E402
from edinet_monitor.services.jquants.client import JQuantsClient  # noqa: E402
from edinet_monitor.services.jquants_quality_audit_service import (  # noqa: E402
    build_jquants_quality_issues,
    export_jquants_quality_audit,
)


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload
        self.status_code = 200
        self.headers = {}

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _FakeSession:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.gets = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.gets.append((url, params, headers, timeout))
        return _FakeResponse(self.payloads.pop(0))


class _FakeAuditClient:
    def __init__(self, *, listed_rows=None):
        self.listed_rows = listed_rows or []

    def iter_equities_master(self, *, date=None, code=None):
        yield from self.listed_rows


def _create_audit_schema(conn: sqlite3.Connection) -> None:
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
        CREATE TABLE jquants_listed_info_raw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_date TEXT NOT NULL,
            local_code TEXT NOT NULL,
            security_code TEXT,
            company_name TEXT,
            company_name_en TEXT,
            sector_17_code TEXT,
            sector_17_name TEXT,
            sector_33_code TEXT,
            sector_33_name TEXT,
            scale_category TEXT,
            market_code TEXT,
            market_name TEXT,
            margin_code TEXT,
            margin_name TEXT,
            raw_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX uq_jquants_listed_info_raw_code_date
        ON jquants_listed_info_raw(local_code, listing_date);
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
        """
    )


def _insert_metric(
    conn: sqlite3.Connection,
    *,
    disclosure: str,
    code: str = "1111",
    fiscal_year: int = 2026,
    period_key: str = "actual:1Q",
    metric_base: str,
    value,
    calc_status: str = "ok",
    disclosed_date: str = "2026-08-01",
    disclosed_time: str = "15:00",
    source_field: str = "fixture",
    source_detail_json: str = "{}",
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
            ?, ?, ?, 'E00001', 'actual',
            'quarter', ?, '1Q', NULL, NULL,
            ?, '2026-04-01', ?, ?, ?,
            ?, ?, 'test', ?, 'yen', ?,
            ?, ?, 'v1', '2026-05-24', '2026-05-24'
        )
        """,
        (
            disclosure,
            f"{code}0",
            code,
            period_key,
            fiscal_year,
            f"{fiscal_year}-06-30",
            disclosed_date,
            disclosed_time,
            f"{metric_base}Current",
            metric_base,
            value,
            calc_status,
            source_field,
            source_detail_json,
        ),
    )


class JQuantsAuditServicesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        _create_audit_schema(self.conn)
        self.tmp_path = TMP_ROOT / self.id().replace(".", "_")
        shutil.rmtree(self.tmp_path, ignore_errors=True)
        self.tmp_path.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_client_fetches_listed_info_pages(self) -> None:
        session = _FakeSession(
            [
                {"data": [{"Code": "11110"}], "pagination_key": None},
            ]
        )
        client = JQuantsClient(api_key="KEY", session=session, request_interval_sec=0)

        listed = list(client.iter_equities_master(date="2026-05-24"))

        self.assertEqual(listed[0]["Code"], "11110")
        self.assertEqual(session.gets[0][0], "https://api.jquants.com/v2/equities/master")

    def test_save_listed_info_is_idempotent_and_reports_master_diffs(self) -> None:
        self.conn.execute(
            """
            INSERT INTO issuer_master (
                edinet_code, security_code, company_name, market, is_listed, exchange, updated_at
            ) VALUES ('E00001', '1111', 'Issuer Name', 'Prime', 1, 'TSE', '2026-05-24')
            """
        )
        client = _FakeAuditClient(
            listed_rows=[
                {
                    "Date": "2026-05-24",
                    "Code": "11110",
                    "CompanyName": "Different Name",
                    "MarketCodeName": "Prime",
                }
            ]
        )

        result1 = save_jquants_listed_info(self.conn, client=client, date_value="2026-05-24")
        result2 = save_jquants_listed_info(self.conn, client=client, date_value="2026-05-24")

        self.assertEqual(result1.saved_total, 1)
        self.assertEqual(result2.saved_total, 1)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM jquants_listed_info_raw").fetchone()[0], 1)
        self.assertTrue(any("company_name_diff" in warning for warning in result1.warnings))

    def test_active_latest_view_selects_latest_disclosure_even_when_missing(self) -> None:
        ensure_summary_views(self.conn)
        _insert_metric(self.conn, disclosure="OLD", metric_base="NetSales", value=100, disclosed_date="2026-08-01")
        _insert_metric(
            self.conn,
            disclosure="NEW",
            metric_base="NetSales",
            value=None,
            calc_status="missing",
            disclosed_date="2026-08-02",
        )

        row = self.conn.execute(
            """
            SELECT disclosure_number, calc_status
            FROM active_latest_jquants_metrics
            WHERE metric_base = 'NetSales'
            """
        ).fetchone()

        self.assertEqual(row["disclosure_number"], "NEW")
        self.assertEqual(row["calc_status"], "missing")

    def test_quality_audit_detects_anomalies_and_writes_reports(self) -> None:
        ensure_summary_views(self.conn)
        _insert_metric(self.conn, disclosure="DISC2025", fiscal_year=2025, metric_base="NetSales", value=100)
        _insert_metric(self.conn, disclosure="DISC2026", fiscal_year=2026, metric_base="NetSales", value=1000)
        _insert_metric(self.conn, disclosure="DISC2026", fiscal_year=2026, metric_base="IssuedShares", value=100)
        _insert_metric(self.conn, disclosure="DISC2026", fiscal_year=2026, metric_base="TreasuryShares", value=100)
        _insert_metric(self.conn, disclosure="DISC2026", fiscal_year=2026, metric_base="EquityRatio", value=45)
        _insert_metric(
            self.conn,
            disclosure="DISC2026",
            fiscal_year=2026,
            metric_base="OrdinaryIncome",
            value=50,
            source_field="ProfitBeforeTax",
            source_detail_json='{"semantic_status":"proxy"}',
        )
        _insert_metric(self.conn, disclosure="DISC2026", fiscal_year=2026, metric_base="ProfitLoss", value=5)
        self.conn.execute(
            """
            INSERT INTO market_derived_metrics (
                source_type, source_id, security_code, period_scope, period_key, fiscal_year,
                period_end, metric_key, metric_base, metric_group, value_num, value_unit,
                calc_status, formula_name, rule_version, created_at, updated_at
            ) VALUES (
                'jquants', 'DISC2026', '1111', 'quarter', 'actual:1Q', 2026,
                '2026-06-30', 'PBRCurrent', 'PBR', 'market', NULL, 'ratio',
                'missing_input', 'pbr', 'v1', 'now', 'now'
            )
            """
        )

        issues = build_jquants_quality_issues(
            self.conn,
            date_from="2025-01-01",
            date_to="2026-12-31",
        )
        checks = {issue["check_name"] for issue in issues}
        by_check = {issue["check_name"]: issue for issue in issues}

        self.assertIn("issued_shares_not_greater_than_treasury", checks)
        self.assertEqual(by_check["issued_shares_not_greater_than_treasury"]["severity"], "warning")
        self.assertIn("equity_ratio_scale_or_range", checks)
        self.assertIn("rapid_yoy_change", checks)
        self.assertIn("ordinary_income_proxy", checks)
        self.assertIn("market_metric_not_ok", checks)

        result = export_jquants_quality_audit(
            self.conn,
            date_from="2025-01-01",
            date_to="2026-12-31",
            output_dir=self.tmp_path,
        )
        self.assertTrue(result.output_path.exists())
        self.assertTrue(result.tsv_path.exists())
        self.assertGreater(result.issue_count, 0)

    def test_quality_audit_does_not_flag_negative_equity_ratio(self) -> None:
        ensure_summary_views(self.conn)
        _insert_metric(self.conn, disclosure="DISC2026", fiscal_year=2026, metric_base="EquityRatio", value=-0.25)

        issues = build_jquants_quality_issues(
            self.conn,
            date_from="2026-01-01",
            date_to="2026-12-31",
        )

        self.assertNotIn(
            "equity_ratio_scale_or_range",
            {issue["check_name"] for issue in issues},
        )


if __name__ == "__main__":
    unittest.main()
