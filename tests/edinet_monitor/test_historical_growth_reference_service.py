from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.derived_metrics.historical_growth_reference_service import (
    fetch_half_progress_annual_values,
    fetch_half_progress_annual_values_bulk,
    fetch_historical_growth_values,
    fetch_historical_growth_values_bulk,
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
                    period_end TEXT,
                    submit_date TEXT
                );
                CREATE TABLE normalized_metrics (
                    doc_id TEXT,
                    metric_key TEXT,
                    value_num REAL,
                    period_end TEXT,
                    consolidation TEXT
                );
                CREATE TABLE derived_metrics (
                    doc_id TEXT,
                    metric_key TEXT,
                    value_num REAL,
                    period_end TEXT,
                    consolidation TEXT,
                    calc_status TEXT
                );
                """
            )
            conn.execute(
                "INSERT INTO filings VALUES (?, ?, ?, ?, ?)",
                ("S100OLD", "E00001", "030000", "2017-03-31", "2017-06-20"),
            )
            conn.execute(
                "INSERT INTO filings VALUES (?, ?, ?, ?, ?)",
                ("S100OTHER", "E00001", "030000", "2018-03-31", "2018-06-20"),
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
            conn.executemany(
                "INSERT INTO derived_metrics VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        "S100OLD",
                        "EPSCurrent",
                        0.2,
                        "2017-03-31",
                        "Consolidated",
                        "ok",
                    ),
                    (
                        "S100OLD",
                        "BPSCurrent",
                        4.0,
                        "2017-03-31",
                        "Consolidated",
                        "ok",
                    ),
                    (
                        "S100OLD",
                        "TheoreticalSharePriceCurrent",
                        1200,
                        "2017-03-31",
                        "Consolidated",
                        "ok",
                    ),
                ],
            )

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
        self.assertEqual(values["EPS"][9]["value_num"], 0.2)
        self.assertEqual(values["BPS"][9]["value_num"], 4.0)
        self.assertEqual(values["TheoreticalSharePrice"][9]["value_num"], 1200)
        self.assertEqual(values["NetSales"][9]["doc_id"], "S100OLD")

    def test_fetch_historical_growth_values_bulk_matches_single_for_full_and_half(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.executescript(
                """
                CREATE TABLE filings (
                    doc_id TEXT PRIMARY KEY,
                    edinet_code TEXT,
                    form_type TEXT,
                    period_end TEXT,
                    submit_date TEXT
                );
                CREATE TABLE normalized_metrics (
                    doc_id TEXT,
                    metric_key TEXT,
                    value_num REAL,
                    period_end TEXT,
                    consolidation TEXT
                );
                CREATE TABLE derived_metrics (
                    doc_id TEXT,
                    metric_key TEXT,
                    value_num REAL,
                    period_end TEXT,
                    consolidation TEXT,
                    calc_status TEXT
                );
                """
            )
            conn.executemany(
                "INSERT INTO filings VALUES (?, ?, ?, ?, ?)",
                [
                    ("FULL_REF", "E00001", "030000", "2025-03-31", "2025-06-20"),
                    ("HALF_REF", "E00002", "043000", "2025-09-30", "2025-11-12"),
                ],
            )
            conn.executemany(
                "INSERT INTO normalized_metrics VALUES (?, ?, ?, ?, ?)",
                [
                    ("FULL_REF", "NetSalesCurrent", 1000, "2025-03-31", "Consolidated"),
                    ("FULL_REF", "IssuedSharesCurrent", 100, "2025-03-31", ""),
                    ("FULL_REF", "TreasurySharesCurrent", 10, "2025-03-31", ""),
                    ("HALF_REF", "NetSalesCurrent", 500, "2025-09-30", "Consolidated"),
                    ("HALF_REF", "IssuedSharesCurrent", 80, "2025-09-30", ""),
                    ("HALF_REF", "TreasurySharesCurrent", 5, "2025-09-30", ""),
                ],
            )
            conn.executemany(
                "INSERT INTO derived_metrics VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("FULL_REF", "EPSCurrent", 12, "2025-03-31", "Consolidated", "ok"),
                    ("HALF_REF", "EPSCurrent", 8, "2025-09-30", "Consolidated", "ok"),
                ],
            )
            filings = [
                {
                    "doc_id": "FULL_CURRENT",
                    "edinet_code": "E00001",
                    "form_type": "030000",
                    "period_end": "2026-03-31",
                },
                {
                    "doc_id": "HALF_CURRENT",
                    "edinet_code": "E00002",
                    "form_type": "043000",
                    "period_end": "2026-09-30",
                },
            ]

            bulk = fetch_historical_growth_values_bulk(conn, filings, offsets=(1,))
            single_full = fetch_historical_growth_values(conn, filings[0], offsets=(1,))
            single_half = fetch_historical_growth_values(conn, filings[1], offsets=(1,))
        finally:
            conn.close()

        self.assertEqual(bulk["FULL_CURRENT"], single_full)
        self.assertEqual(bulk["HALF_CURRENT"], single_half)
        self.assertEqual(bulk["FULL_CURRENT"]["OutstandingShares"][1]["value_num"], 100)
        self.assertEqual(bulk["HALF_CURRENT"]["NetSales"][1]["value_num"], 500)

    def test_fetch_half_progress_annual_values_bulk_matches_single(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.executescript(
                """
                CREATE TABLE filings (
                    doc_id TEXT PRIMARY KEY,
                    edinet_code TEXT,
                    form_type TEXT,
                    period_end TEXT,
                    submit_date TEXT
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
            conn.executemany(
                "INSERT INTO filings VALUES (?, ?, ?, ?, ?)",
                [
                    ("HALF_CURRENT", "E00001", "043000", "2025-09-30", "2025-11-12"),
                    ("ANNUAL_REF", "E00001", "030000", "2026-03-31", "2026-06-20"),
                    ("ANNUAL_LATE", "E00001", "030000", "2026-12-31", "2027-03-20"),
                ],
            )
            conn.executemany(
                "INSERT INTO normalized_metrics VALUES (?, ?, ?, ?, ?)",
                [
                    ("ANNUAL_REF", "NetSalesCurrent", 2000, "2026-03-31", "Consolidated"),
                    ("ANNUAL_REF", "OrdinaryIncomeCurrent", 300, "2026-03-31", "Consolidated"),
                    ("ANNUAL_REF", "ProfitLossCurrent", 210, "2026-03-31", "Consolidated"),
                    ("ANNUAL_LATE", "NetSalesCurrent", 9999, "2026-12-31", "Consolidated"),
                ],
            )
            filing = {
                "doc_id": "HALF_CURRENT",
                "edinet_code": "E00001",
                "form_type": "043000",
                "period_end": "2025-09-30",
            }

            single = fetch_half_progress_annual_values(conn, filing)
            bulk = fetch_half_progress_annual_values_bulk(conn, [filing])
        finally:
            conn.close()

        self.assertEqual(bulk["HALF_CURRENT"], single)
        self.assertEqual(bulk["HALF_CURRENT"]["NetSales"]["value_num"], 2000)


if __name__ == "__main__":
    unittest.main()
