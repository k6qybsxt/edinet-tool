from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.jquants.client import JQuantsClient  # noqa: E402
from edinet_monitor.services.jquants.mapper import (  # noqa: E402
    quote_from_row,
    statement_metrics_from_row,
)
from edinet_monitor.services.jquants.oldest_date_service import discover_oldest_fins_summary_date  # noqa: E402
from edinet_monitor.services.jquants.raw_json_store import (  # noqa: E402
    fins_summary_raw_path,
    write_fins_summary_raw_jsonl,
)
from edinet_monitor.services.jquants.raw_rebuild_service import (  # noqa: E402
    rebuild_jquants_financial_metrics_from_raw,
)
from edinet_monitor.services.jquants.repository import (  # noqa: E402
    record_ingest_progress,
    upsert_financial_metrics,
    upsert_quote,
)


class _FakeResponse:
    def __init__(self, payload, status_code=200, headers=None):
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error")
        return None

    def json(self):
        return self.payload


class _FakeSession:
    def __init__(self):
        self.gets = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.gets.append((url, params, headers, timeout))
        return _FakeResponse({"fin_summary": [], "pagination_key": None})


class _SequenceSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.gets = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.gets.append((url, params, headers, timeout))
        return self.responses.pop(0)


class _FakeJQuantsClient:
    def __init__(self, rows_by_date):
        self.rows_by_date = rows_by_date
        self.requested_dates = []

    def get_fin_summary_page(self, *, date=None, code=None, pagination_key=None):
        self.requested_dates.append(date)
        return type("Page", (), {"items": list(self.rows_by_date.get(date, [])), "pagination_key": None})()


def _statement_row(period: str = "1Q") -> dict:
    return {
        "DiscDate": "2026-05-02",
        "DiscTime": "15:00",
        "Code": "11110",
        "DiscNo": f"D-{period}",
        "DocType": "1QFinancialStatements_Consolidated_JP",
        "CurPerType": period,
        "CurPerSt": "2025-04-01",
        "CurPerEn": "2025-06-30",
        "CurFYSt": "2025-04-01",
        "CurFYEn": "2026-03-31",
        "NxtFYSt": "2026-04-01",
        "NxtFYEn": "2027-03-31",
        "Sales": "100000000",
        "OP": "12000000",
        "OdP": "",
        "NP": "7000000",
        "EPS": "12.34",
        "TA": "900000000",
        "Eq": "400000000",
        "EqAR": "0.444",
        "BPS": "500.12",
        "CFO": "10000000",
        "CFI": "-20000000",
        "CFF": "3000000",
        "CashEq": "60000000",
        "ShOutFY": "1000000",
        "TrShFY": "100",
        "FSales": "400000000",
        "FOP": "50000000",
        "FOdP": "48000000",
        "FNP": "30000000",
        "FEPS": "50.00",
        "NxFSales": "450000000",
        "NxFOP": "55000000",
        "NxFOdP": "53000000",
        "NxFNp": "33000000",
        "FSales2Q": "220000000",
        "FOP2Q": "25000000",
        "FOdP2Q": "24000000",
        "FNP2Q": "15000000",
        "FEPS2Q": "25.00",
    }


class JQuantsServicesTest(unittest.TestCase):
    def _tmp_dir(self, name: str) -> Path:
        path = ROOT_DIR / "tests" / "_tmp_jquants_services" / name
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def test_client_uses_api_key_without_logging_it(self) -> None:
        session = _FakeSession()
        client = JQuantsClient(api_key="SECRET_API_KEY", session=session)

        list(client.iter_fin_summary(date="2026-05-02"))

        self.assertEqual(session.gets[0][0], "https://api.jquants.com/v2/fins/summary")
        self.assertEqual(session.gets[0][1]["date"], "20260502")
        self.assertEqual(session.gets[0][2]["x-api-key"], "SECRET_API_KEY")

    def test_client_waits_and_retries_once_on_rate_limit(self) -> None:
        session = _SequenceSession(
            [
                _FakeResponse({"message": "rate limited"}, status_code=429),
                _FakeResponse({"fin_summary": [{"DiscNo": "D-1Q"}], "pagination_key": None}),
            ]
        )
        client = JQuantsClient(
            api_key="SECRET_API_KEY",
            session=session,
            request_interval_sec=0,
            rate_limit_cooldown_sec=1,
            max_retries=1,
        )

        with patch("edinet_monitor.services.jquants.client.time.sleep") as sleep_mock:
            rows = list(client.iter_fin_summary(date="2026-05-02"))

        self.assertEqual(rows[0]["DiscNo"], "D-1Q")
        self.assertEqual(len(session.gets), 2)
        sleep_mock.assert_called_once_with(1.0)

    def test_statement_mapper_keeps_1q_actual_and_forecasts(self) -> None:
        metrics = statement_metrics_from_row(_statement_row("1Q"), include_forecasts=True)
        by_key = {(metric.period_key, metric.forecast_stage, metric.metric_base): metric for metric in metrics}

        self.assertEqual(by_key[("actual:1Q", None, "NetSales")].value_num, 100000000.0)
        self.assertEqual(by_key[("actual:1Q", None, "OrdinaryIncome")].calc_status, "missing")
        self.assertEqual(by_key[("actual:1Q", None, "OutstandingShares")].value_num, 1000000.0)
        self.assertEqual(by_key[("actual:1Q", None, "BPS")].value_num, 400.0)
        self.assertEqual(by_key[("actual:1Q", None, "OfficialBPS")].value_num, 500.12)
        self.assertEqual(by_key[("actual:1Q", None, "OfficialEPS")].value_num, 12.34)
        self.assertEqual(by_key[("actual:1Q", None, "AverageShares")].calc_status, "missing")
        self.assertEqual(by_key[("actual:1Q", None, "EPS")].calc_status, "missing")
        self.assertEqual(by_key[("forecast:FY", "1Q", "NetSales")].value_num, 400000000.0)
        self.assertEqual(by_key[("forecast:FY", "1Q", "OperatingIncome")].value_num, 50000000.0)
        self.assertEqual(by_key[("forecast:2Q", "1Q", "NetSales")].value_num, 220000000.0)
        self.assertNotIn(("forecast:FY", "1Q", "EPS"), by_key)
        self.assertNotIn(("forecast:2Q", "1Q", "EPS"), by_key)

    def test_statement_mapper_keeps_profit_before_tax_separate_and_uses_it_for_eps(self) -> None:
        row = _statement_row("1Q")
        row["ProfitBeforeTax"] = "48000000"
        row["EPS"] = "999.99"
        row["BPS"] = "999.99"

        metrics = statement_metrics_from_row(row, include_forecasts=False)
        by_key = {(metric.period_key, metric.forecast_stage, metric.metric_base): metric for metric in metrics}

        self.assertEqual(by_key[("actual:1Q", None, "OrdinaryIncome")].calc_status, "missing")
        self.assertEqual(by_key[("actual:1Q", None, "ProfitBeforeTax")].value_num, 48000000.0)
        self.assertEqual(by_key[("actual:1Q", None, "ProfitBeforeTax")].source_field, "ProfitBeforeTax")
        self.assertAlmostEqual(by_key[("actual:1Q", None, "EPS")].value_num, 33.6)
        self.assertEqual(
            by_key[("actual:1Q", None, "EPS")].source_field,
            "calculated:ProfitBeforeTax*0.7/OutstandingShares",
        )
        self.assertEqual(
            json.loads(by_key[("actual:1Q", None, "EPS")].source_detail_json)["selected_profit_base"],
            "ProfitBeforeTax",
        )
        self.assertEqual(by_key[("actual:1Q", None, "BPS")].value_num, 400.0)
        self.assertEqual(by_key[("actual:1Q", None, "BPS")].source_field, "calculated:Eq/OutstandingShares")

    def test_statement_mapper_prefers_odp_for_eps_when_available(self) -> None:
        row = _statement_row("1Q")
        row["OdP"] = "30000000"
        row["ProfitBeforeTax"] = "48000000"

        metrics = statement_metrics_from_row(row, include_forecasts=False)
        by_key = {(metric.period_key, metric.forecast_stage, metric.metric_base): metric for metric in metrics}

        self.assertEqual(by_key[("actual:1Q", None, "OrdinaryIncome")].value_num, 30000000.0)
        self.assertEqual(by_key[("actual:1Q", None, "ProfitBeforeTax")].value_num, 48000000.0)
        self.assertAlmostEqual(by_key[("actual:1Q", None, "EPS")].value_num, 21.0)
        self.assertEqual(
            json.loads(by_key[("actual:1Q", None, "EPS")].source_detail_json)["selected_profit_base"],
            "OrdinaryIncome",
        )

    def test_statement_mapper_keeps_2q_actuals(self) -> None:
        metrics = statement_metrics_from_row(_statement_row("2Q"), include_forecasts=False)
        by_key = {(metric.period_key, metric.forecast_stage, metric.metric_base): metric for metric in metrics}

        self.assertEqual(by_key[("actual:2Q", None, "NetSales")].value_num, 100000000.0)
        self.assertEqual(by_key[("actual:2Q", None, "OutstandingShares")].value_num, 1000000.0)

    def test_statement_mapper_keeps_2q_full_year_forecasts(self) -> None:
        metrics = statement_metrics_from_row(_statement_row("2Q"), include_forecasts=True)
        by_key = {(metric.period_key, metric.forecast_stage, metric.metric_base): metric for metric in metrics}

        self.assertEqual(by_key[("forecast:FY", "2Q", "NetSales")].value_num, 400000000.0)
        self.assertEqual(by_key[("forecast:FY", "2Q", "OperatingIncome")].value_num, 50000000.0)
        self.assertEqual(by_key[("forecast:FY", "2Q", "OrdinaryIncome")].value_num, 48000000.0)
        self.assertEqual(by_key[("forecast:FY", "2Q", "ProfitLoss")].value_num, 30000000.0)
        self.assertEqual(by_key[("forecast:2Q", "2Q", "NetSales")].value_num, 220000000.0)
        self.assertNotIn(("forecast:FY", "2Q", "EPS"), by_key)

    def test_statement_mapper_treats_4q_forecast_as_initial(self) -> None:
        metrics = statement_metrics_from_row(_statement_row("4Q"), include_forecasts=True)
        by_key = {(metric.period_key, metric.forecast_stage, metric.metric_base): metric for metric in metrics}

        self.assertEqual(by_key[("forecast:FY", "initial", "NetSales")].value_num, 450000000.0)
        self.assertEqual(by_key[("forecast:FY", "initial", "ProfitLoss")].value_num, 33000000.0)
        self.assertNotIn(("forecast:FY", "initial", "EPS"), by_key)
        self.assertEqual(by_key[("forecast:FY", "initial", "NetSales")].fiscal_year, 2027)
        self.assertEqual(by_key[("forecast:FY", "initial", "NetSales")].period_start, "2026-04-01")
        self.assertEqual(by_key[("forecast:FY", "initial", "NetSales")].period_end, "2027-03-31")
        self.assertNotIn(("actual:4Q", None, "NetSales"), by_key)

    def test_statement_mapper_uses_non_consolidated_forecast_fallback(self) -> None:
        row = _statement_row("1Q")
        row["FSales"] = ""
        row["FNCSales"] = "123000000"

        metrics = statement_metrics_from_row(row, include_forecasts=True)
        by_key = {(metric.period_key, metric.forecast_stage, metric.metric_base): metric for metric in metrics}

        self.assertEqual(by_key[("forecast:FY", "1Q", "NetSales")].value_num, 123000000.0)
        self.assertEqual(by_key[("forecast:FY", "1Q", "NetSales")].source_field, "FNCSales")

    def test_statement_mapper_ignores_unsupported_period_forecasts(self) -> None:
        metrics = statement_metrics_from_row(_statement_row("5Q"), include_forecasts=True)

        self.assertEqual(metrics, [])

    def test_quote_mapper_rounds_adjustment_close(self) -> None:
        quote = quote_from_row(
            {
                "Date": "2026-05-01",
                "Code": "11110",
                "AdjC": "123.455",
                "AdjO": "120.1",
            }
        )

        self.assertEqual(quote.security_code, "1111")
        self.assertEqual(quote.adjustment_close_rounded, 123.46)

    def test_raw_json_store_is_idempotent_by_disclosure_number(self) -> None:
        tmp_dir = self._tmp_dir("raw_json_idempotent")
        row = _statement_row("1Q")
        write_fins_summary_raw_jsonl([row], storage_root=tmp_dir)
        write_fins_summary_raw_jsonl([row], storage_root=tmp_dir)

        path = fins_summary_raw_path("2026-05-02", storage_root=tmp_dir)
        lines = path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["DiscNo"], "D-1Q")

    def test_discover_oldest_fins_summary_date_writes_report_and_manifest(self) -> None:
        tmp_dir = self._tmp_dir("oldest_date")
        client = _FakeJQuantsClient({"2020-01-03": [_statement_row("1Q")]})
        result = discover_oldest_fins_summary_date(
            client=client,
            date_from="2020-01-01",
            date_to="2020-01-05",
            output_dir=tmp_dir,
            storage_root=tmp_dir,
        )

        self.assertEqual(result.oldest_date, "2020-01-03")
        self.assertEqual(result.checked_days, 3)
        self.assertTrue(result.output_path and result.output_path.exists())
        self.assertTrue(result.manifest_path and result.manifest_path.exists())

    def test_repository_records_ingest_progress_idempotently(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE jquants_ingest_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                run_type TEXT NOT NULL,
                target_kind TEXT NOT NULL,
                target_value TEXT NOT NULL,
                status TEXT NOT NULL,
                fetched_count INTEGER NOT NULL DEFAULT 0,
                saved_count INTEGER NOT NULL DEFAULT 0,
                skipped_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                started_at TEXT,
                finished_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX uq_jquants_ingest_progress_target
            ON jquants_ingest_progress(run_id, run_type, target_kind, target_value);
            """
        )

        record_ingest_progress(
            conn,
            run_id="run-1",
            run_type="jquants_daily_quotes",
            target_kind="date",
            target_value="2026-05-01",
            status="running",
            started_at="2026-05-07T09:00:00",
        )
        record_ingest_progress(
            conn,
            run_id="run-1",
            run_type="jquants_daily_quotes",
            target_kind="date",
            target_value="2026-05-01",
            status="completed",
            fetched_count=10,
            saved_count=10,
            started_at="2026-05-07T09:00:00",
            finished_at="2026-05-07T09:01:00",
        )

        row = conn.execute("SELECT * FROM jquants_ingest_progress").fetchone()
        self.assertEqual(row["status"], "completed")
        self.assertEqual(row["fetched_count"], 10)
        self.assertEqual(row["saved_count"], 10)
        self.assertEqual(row["started_at"], "2026-05-07T09:00:00")

    def test_repository_upserts_metrics_and_quotes_idempotently(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE issuer_master (
                edinet_code TEXT PRIMARY KEY,
                security_code TEXT
            );
            INSERT INTO issuer_master VALUES ('E00001', '11110');
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
            CREATE UNIQUE INDEX uq_jquants_financial_metrics_scope
            ON jquants_financial_metrics(disclosure_number, period_key, metric_key);
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
            CREATE UNIQUE INDEX uq_jquants_daily_quotes_code_date
            ON jquants_daily_quotes(local_code, trade_date);
            """
        )
        metrics = statement_metrics_from_row(_statement_row("1Q"), include_forecasts=False)
        quote = quote_from_row({"Date": "2026-05-01", "Code": "11110", "AdjC": "123.45"})

        upsert_financial_metrics(conn, metrics)
        upsert_financial_metrics(conn, metrics)
        upsert_quote(conn, quote)
        upsert_quote(conn, quote)

        self.assertEqual(conn.execute("SELECT COUNT(*) FROM jquants_financial_metrics").fetchone()[0], len(metrics))
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM jquants_daily_quotes").fetchone()[0], 1)
        conn.close()

    def test_rebuild_metrics_from_raw_adds_forecast_operating_income(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE issuer_master (
                edinet_code TEXT PRIMARY KEY,
                security_code TEXT
            );
            INSERT INTO issuer_master VALUES ('E00001', '11110');
            CREATE TABLE jquants_statement_raw (
                disclosure_number TEXT PRIMARY KEY,
                disclosed_date TEXT,
                local_code TEXT,
                security_code TEXT,
                type_of_current_period TEXT,
                raw_json TEXT
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
            CREATE UNIQUE INDEX uq_jquants_financial_metrics_scope
            ON jquants_financial_metrics(disclosure_number, period_key, metric_key);
            """
        )
        row = _statement_row("3Q")
        conn.execute(
            """
            INSERT INTO jquants_statement_raw (
                disclosure_number, disclosed_date, local_code, security_code,
                type_of_current_period, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                row["DiscNo"],
                row["DiscDate"],
                row["Code"],
                "1111",
                row["CurPerType"],
                json.dumps(row, ensure_ascii=False),
            ),
        )

        dry_run = rebuild_jquants_financial_metrics_from_raw(
            conn,
            date_from="2026-05-01",
            date_to="2026-05-31",
            periods={"3Q"},
            apply=False,
        )
        self.assertGreater(dry_run.metrics_built, 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM jquants_financial_metrics").fetchone()[0], 0)

        result = rebuild_jquants_financial_metrics_from_raw(
            conn,
            date_from="2026-05-01",
            date_to="2026-05-31",
            periods={"3Q"},
            apply=True,
        )

        metric = conn.execute(
            """
            SELECT value_num, source_field
            FROM jquants_financial_metrics
            WHERE metric_kind = 'forecast'
              AND forecast_stage = '3Q'
              AND metric_base = 'OperatingIncome'
            """
        ).fetchone()
        self.assertEqual(result.raw_rows, 1)
        self.assertEqual(metric["value_num"], 50000000.0)
        self.assertEqual(metric["source_field"], "FOP")
        conn.close()


if __name__ == "__main__":
    unittest.main()
