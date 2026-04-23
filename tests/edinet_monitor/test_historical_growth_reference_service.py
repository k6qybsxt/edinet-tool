from __future__ import annotations

import sqlite3
import unittest

from edinet_monitor.services.derived_metrics.historical_growth_reference_service import (
    fetch_historical_growth_values,
)


class HistoricalGrowthReferenceServiceTest(unittest.TestCase):
    def test_fetch_historical_growth_values_uses_exact_nine_year_period_end(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.executescript(
                """
                CREATE TABLE filings (
                    doc_id TEXT PRIMARY KEY,
                    edinet_code TEXT,
                    form_type TEXT,
                    period_end TEXT
                );
                CREATE TABLE normalized_metrics (
                    doc_id TEXT,
                    metric_key TEXT,
                    value_num REAL,
                    period_end TEXT,
                    consolidation TEXT
                );
                """
            )
            conn.execute(
                "INSERT INTO filings VALUES (?, ?, ?, ?)",
                ("S100OLD", "E00001", "030000", "2017-03-31"),
            )
            conn.execute(
                "INSERT INTO filings VALUES (?, ?, ?, ?)",
                ("S100OTHER", "E00001", "030000", "2018-03-31"),
            )
            rows = [
                ("S100OLD", "NetSalesCurrent", 300_000, "2017-03-31", "Consolidated"),
                ("S100OLD", "OrdinaryIncomeCurrent", 80_000, "2017-03-31", "Consolidated"),
                ("S100OLD", "CashAndCashEquivalentsCurrent", 100_000, "2017-03-31", "Consolidated"),
                ("S100OLD", "IssuedSharesCurrent", 1_000_000, "2017-03-31", ""),
                ("S100OLD", "TreasurySharesCurrent", 50_000, "2017-03-31", ""),
                ("S100OTHER", "NetSalesCurrent", 999_999, "2018-03-31", "Consolidated"),
            ]
            conn.executemany("INSERT INTO normalized_metrics VALUES (?, ?, ?, ?, ?)", rows)

            values = fetch_historical_growth_values(
                conn,
                {
                    "doc_id": "S100CURRENT",
                    "edinet_code": "E00001",
                    "period_end": "2026-03-31",
                },
            )
        finally:
            conn.close()

        self.assertEqual(values["NetSales"][9]["value_num"], 300_000)
        self.assertEqual(values["OrdinaryIncome"][9]["value_num"], 80_000)
        self.assertEqual(values["CashAndCashEquivalents"][9]["value_num"], 100_000)
        self.assertEqual(values["OutstandingShares"][9]["value_num"], 950_000)
        self.assertEqual(values["NetSales"][9]["doc_id"], "S100OLD")


if __name__ == "__main__":
    unittest.main()
