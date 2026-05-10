from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_jquants_period_prune"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.jquants_period_prune_service import (  # noqa: E402
    fetch_old_jquants_quarter_candidates,
    prune_old_jquants_quarter_data,
)


class JQuantsPeriodPruneServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self._create_schema()
        self._insert_fixtures()

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _create_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE jquants_statement_raw (
                disclosure_number TEXT PRIMARY KEY,
                local_code TEXT,
                security_code TEXT,
                type_of_current_period TEXT,
                current_period_end_date TEXT,
                current_fiscal_year_end_date TEXT,
                fiscal_year INTEGER,
                disclosed_date TEXT,
                disclosed_time TEXT
            );

            CREATE TABLE jquants_financial_metrics (
                id INTEGER PRIMARY KEY,
                disclosure_number TEXT,
                metric_key TEXT
            );

            CREATE TABLE market_derived_metrics (
                id INTEGER PRIMARY KEY,
                source_type TEXT,
                source_id TEXT
            );
            """
        )

    def _insert_fixtures(self) -> None:
        for quarter in ("1Q", "3Q"):
            for index in range(12):
                year = 2026 - index
                disclosure = f"DISC_{quarter}_{index:02d}"
                period_end = f"{year}-06-30" if quarter == "1Q" else f"{year}-12-31"
                self.conn.execute(
                    """
                    INSERT INTO jquants_statement_raw (
                        disclosure_number, local_code, security_code, type_of_current_period,
                        current_period_end_date, current_fiscal_year_end_date,
                        fiscal_year, disclosed_date, disclosed_time
                    ) VALUES (?, '72030', '7203', ?, ?, ?, ?, ?, '15:00')
                    """,
                    (disclosure, quarter, period_end, f"{year + 1}-03-31", year, period_end),
                )
                self.conn.execute(
                    "INSERT INTO jquants_financial_metrics (disclosure_number, metric_key) VALUES (?, 'NetSalesCurrent')",
                    (disclosure,),
                )
                self.conn.execute(
                    "INSERT INTO market_derived_metrics (source_type, source_id) VALUES ('jquants', ?)",
                    (disclosure,),
                )
        self.conn.execute(
            """
            INSERT INTO jquants_statement_raw (
                disclosure_number, local_code, security_code, type_of_current_period,
                current_period_end_date, current_fiscal_year_end_date,
                fiscal_year, disclosed_date, disclosed_time
            ) VALUES ('DISC_FY', '72030', '7203', 'FY', '2026-03-31', '2026-03-31', 2026, '2026-05-10', '15:00')
            """
        )
        self.conn.commit()

    def test_fetch_candidates_keeps_latest_eleven_per_quarter(self) -> None:
        candidates = fetch_old_jquants_quarter_candidates(self.conn, keep_latest=11)

        self.assertEqual({candidate.disclosure_number for candidate in candidates}, {"DISC_1Q_11", "DISC_3Q_11"})
        self.assertEqual({candidate.quarter_type for candidate in candidates}, {"1Q", "3Q"})

    def test_tenbagger_learning_security_is_excluded_from_prune_by_default(self) -> None:
        for index in range(12):
            year = 2026 - index
            disclosure = f"TEN_1Q_{index:02d}"
            self.conn.execute(
                """
                INSERT INTO jquants_statement_raw (
                    disclosure_number, local_code, security_code, type_of_current_period,
                    current_period_end_date, current_fiscal_year_end_date,
                    fiscal_year, disclosed_date, disclosed_time
                ) VALUES (?, '69200', '6920', '1Q', ?, ?, ?, ?, '15:00')
                """,
                (disclosure, f"{year}-06-30", f"{year + 1}-03-31", year, f"{year}-06-30"),
            )
        self.conn.commit()

        candidates = fetch_old_jquants_quarter_candidates(self.conn, keep_latest=11)
        included = fetch_old_jquants_quarter_candidates(
            self.conn,
            keep_latest=11,
            exclude_security_codes=frozenset(),
        )

        self.assertNotIn("TEN_1Q_11", {candidate.disclosure_number for candidate in candidates})
        self.assertIn("TEN_1Q_11", {candidate.disclosure_number for candidate in included})

    def test_dry_run_reports_counts_without_deleting(self) -> None:
        result = prune_old_jquants_quarter_data(
            self.conn,
            keep_latest=11,
            apply=False,
            output_dir=self.tmp_path,
        )

        self.assertFalse(result.apply)
        self.assertEqual(result.candidate_count, 2)
        self.assertEqual(result.deleted_counts["jquants_statement_raw"], 2)
        self.assertEqual(result.deleted_counts["jquants_financial_metrics"], 2)
        self.assertEqual(result.deleted_counts["market_derived_metrics"], 2)
        self.assertIsNotNone(result.output_path)
        self.assertTrue(result.output_path.exists())
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM jquants_statement_raw").fetchone()[0],
            25,
        )

    def test_apply_deletes_quarter_data_but_keeps_fy_and_quotes_out_of_scope(self) -> None:
        result = prune_old_jquants_quarter_data(
            self.conn,
            keep_latest=11,
            apply=True,
            output_dir=self.tmp_path,
        )

        self.assertTrue(result.apply)
        self.assertEqual(result.candidate_count, 2)
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM jquants_statement_raw WHERE disclosure_number IN ('DISC_1Q_11', 'DISC_3Q_11')"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM jquants_financial_metrics WHERE disclosure_number IN ('DISC_1Q_11', 'DISC_3Q_11')"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*)
                FROM market_derived_metrics
                WHERE source_type = 'jquants'
                  AND source_id IN ('DISC_1Q_11', 'DISC_3Q_11')
                """
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM jquants_statement_raw WHERE disclosure_number = 'DISC_FY'"
            ).fetchone()[0],
            1,
        )

    def test_apply_with_delete_files_removes_matching_raw_json_records(self) -> None:
        raw_path = self.tmp_path / "raw_json" / "fins_summary" / "2015" / "2015-12-31.jsonl"
        raw_path.parent.mkdir(parents=True)
        raw_path.write_text(
            "\n".join(
                [
                    json.dumps({"DiscNo": "DISC_3Q_11", "DiscDate": "2015-12-31"}),
                    json.dumps({"DiscNo": "KEEP_ME", "DiscDate": "2015-12-31"}),
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        result = prune_old_jquants_quarter_data(
            self.conn,
            keep_latest=11,
            quarter_types=("3Q",),
            delete_files=True,
            apply=True,
            output_dir=self.tmp_path,
            storage_root=self.tmp_path,
        )

        self.assertEqual(result.file_counts["raw_json_records"], 1)
        self.assertEqual(result.file_counts["raw_json_files_updated"], 1)
        text = raw_path.read_text(encoding="utf-8")
        self.assertIn("KEEP_ME", text)
        self.assertNotIn("DISC_3Q_11", text)


if __name__ == "__main__":
    unittest.main()
