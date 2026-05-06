from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.jquants.client import JQuantsClient  # noqa: E402
from edinet_monitor.services.jquants.mapper import (  # noqa: E402
    quote_from_row,
    statement_metrics_from_row,
)
from edinet_monitor.services.jquants.repository import (  # noqa: E402
    upsert_financial_metrics,
    upsert_quote,
)


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _FakeSession:
    def __init__(self):
        self.gets = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.gets.append((url, params, headers, timeout))
        return _FakeResponse({"fin_summary": [], "pagination_key": None})


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
        "FSales2Q": "220000000",
        "FOP2Q": "25000000",
        "FOdP2Q": "24000000",
        "FNP2Q": "15000000",
        "FEPS2Q": "25.00",
    }


class JQuantsServicesTest(unittest.TestCase):
    def test_client_uses_api_key_without_logging_it(self) -> None:
        session = _FakeSession()
        client = JQuantsClient(api_key="SECRET_API_KEY", session=session)

        list(client.iter_fin_summary(date="2026-05-02"))

        self.assertEqual(session.gets[0][0], "https://api.jquants.com/v2/fins/summary")
        self.assertEqual(session.gets[0][1]["date"], "20260502")
        self.assertEqual(session.gets[0][2]["x-api-key"], "SECRET_API_KEY")

    def test_statement_mapper_keeps_1q_actual_and_forecasts(self) -> None:
        metrics = statement_metrics_from_row(_statement_row("1Q"), include_forecasts=True)
        by_key = {(metric.period_key, metric.metric_base): metric for metric in metrics}

        self.assertEqual(by_key[("actual:1Q", "NetSales")].value_num, 100000000.0)
        self.assertEqual(by_key[("actual:1Q", "OrdinaryIncome")].calc_status, "missing")
        self.assertEqual(by_key[("actual:1Q", "OutstandingShares")].value_num, 1000000.0)
        self.assertEqual(by_key[("forecast:FY", "NetSales")].value_num, 400000000.0)
        self.assertEqual(by_key[("forecast:2Q", "EPS")].value_num, 25.0)

    def test_statement_mapper_excludes_2q_actuals(self) -> None:
        metrics = statement_metrics_from_row(_statement_row("2Q"), include_forecasts=False)

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


if __name__ == "__main__":
    unittest.main()
