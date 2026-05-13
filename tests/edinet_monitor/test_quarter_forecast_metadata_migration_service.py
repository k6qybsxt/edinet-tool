from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.quarter_forecast_metadata_migration_service import (
    migrate_quarter_forecast_metadata,
)


class QuarterForecastMetadataMigrationServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE filings (
                doc_id TEXT PRIMARY KEY,
                form_type TEXT
            );
            CREATE TABLE derived_metrics (
                doc_id TEXT,
                period_scope TEXT,
                period_key TEXT,
                quarter_type TEXT
            );
            CREATE TABLE jquants_statement_raw (
                disclosure_number TEXT PRIMARY KEY,
                type_of_current_period TEXT
            );
            CREATE TABLE jquants_financial_metrics (
                disclosure_number TEXT,
                metric_kind TEXT,
                period_key TEXT,
                forecast_target TEXT,
                forecast_stage TEXT,
                metric_key TEXT,
                metric_base TEXT
            );
            """
        )
        self.conn.execute("INSERT INTO filings VALUES ('DOC_HALF', '043A00')")
        self.conn.execute("INSERT INTO filings VALUES ('DOC_ANNUAL', '030000')")
        self.conn.execute("INSERT INTO derived_metrics VALUES ('DOC_HALF', 'half', NULL, NULL)")
        self.conn.execute("INSERT INTO derived_metrics VALUES ('DOC_ANNUAL', 'annual', NULL, NULL)")
        self.conn.execute("INSERT INTO jquants_statement_raw VALUES ('DISC_1Q', '1Q')")
        self.conn.execute("INSERT INTO jquants_statement_raw VALUES ('DISC_4Q', '4Q')")
        self.conn.execute("INSERT INTO jquants_statement_raw VALUES ('DISC_5Q', '5Q')")
        self.conn.execute(
            """
            INSERT INTO jquants_financial_metrics
            VALUES ('DISC_1Q', 'forecast', 'forecast:FY', 'FY', NULL, 'NetSalesCurrent', 'NetSales')
            """
        )
        self.conn.execute(
            """
            INSERT INTO jquants_financial_metrics
            VALUES ('DISC_1Q', 'forecast', 'forecast:FY', 'FY', NULL, 'EPSCurrent', 'EPS')
            """
        )
        self.conn.execute(
            """
            INSERT INTO jquants_financial_metrics
            VALUES ('DISC_1Q', 'forecast', 'forecast:2Q', '2Q', NULL, 'NetSalesCurrent', 'NetSales')
            """
        )
        self.conn.execute(
            """
            INSERT INTO jquants_financial_metrics
            VALUES ('DISC_4Q', 'forecast', 'forecast:FY', 'FY', NULL, 'ProfitLossCurrent', 'ProfitLoss')
            """
        )
        self.conn.execute(
            """
            INSERT INTO jquants_financial_metrics
            VALUES ('DISC_5Q', 'forecast', 'forecast:FY', 'FY', NULL, 'ProfitLossCurrent', 'ProfitLoss')
            """
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_dry_run_reports_candidates_without_changes(self) -> None:
        result = migrate_quarter_forecast_metadata(self.conn)

        self.assertFalse(result.apply)
        self.assertEqual(result.annual_derived_candidates, 1)
        self.assertEqual(result.q2_derived_candidates, 1)
        self.assertEqual(result.forecast_stage_candidates, 4)
        self.assertEqual(result.obsolete_forecast_candidates, 3)
        self.assertEqual(
            self.conn.execute("SELECT period_scope FROM derived_metrics WHERE doc_id = 'DOC_HALF'").fetchone()[0],
            "half",
        )

    def test_apply_updates_2q_metadata_and_cleans_forecast_rows(self) -> None:
        result = migrate_quarter_forecast_metadata(self.conn, apply=True)

        self.assertTrue(result.apply)
        self.assertEqual(result.annual_derived_updated, 1)
        self.assertEqual(result.q2_derived_updated, 1)
        self.assertEqual(result.forecast_stage_updated, 4)
        self.assertEqual(result.obsolete_forecast_deleted, 3)
        half_row = self.conn.execute(
            "SELECT period_scope, period_key, quarter_type FROM derived_metrics WHERE doc_id = 'DOC_HALF'"
        ).fetchone()
        self.assertEqual(tuple(half_row), ("quarter", "actual:2Q", "2Q"))
        forecast_rows = self.conn.execute(
            "SELECT metric_base, period_key, forecast_stage FROM jquants_financial_metrics"
        ).fetchall()
        self.assertEqual(len(forecast_rows), 2)
        self.assertEqual(tuple(forecast_rows[0]), ("NetSales", "forecast:FY", "1Q"))
        self.assertEqual(tuple(forecast_rows[1]), ("ProfitLoss", "forecast:FY", "initial"))


if __name__ == "__main__":
    unittest.main()
