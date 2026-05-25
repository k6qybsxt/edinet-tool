from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_jquants_official_cli_compare"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.jquants_official_cli_compare_service import (  # noqa: E402
    OfficialCliCompareError,
    run_jquants_official_cli_compare,
)


def _runner_for(payload):
    def runner(command):
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=json.dumps(payload, ensure_ascii=False),
            stderr="",
        )

    return runner


class JQuantsOfficialCliCompareServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE jquants_statement_raw (
                disclosure_number TEXT PRIMARY KEY,
                disclosed_date TEXT,
                disclosed_time TEXT,
                local_code TEXT,
                security_code TEXT,
                raw_json TEXT NOT NULL
            );
            CREATE TABLE jquants_daily_quotes (
                local_code TEXT NOT NULL,
                security_code TEXT,
                trade_date TEXT NOT NULL,
                raw_json TEXT NOT NULL
            );
            """
        )
        self.tmp_path = TMP_ROOT / self.id().replace(".", "_")
        shutil.rmtree(self.tmp_path, ignore_errors=True)
        self.tmp_path.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_fins_summary_matches_official_cli_rows(self) -> None:
        row = {
            "DiscDate": "2026-05-07",
            "DiscTime": "15:00",
            "Code": "11110",
            "DiscNo": "DISC1",
            "DocType": "3Q",
            "Sales": 100,
            "OP": 20,
            "OdP": 18,
            "NP": 12,
        }
        self.conn.execute(
            """
            INSERT INTO jquants_statement_raw (
                disclosure_number, disclosed_date, disclosed_time, local_code, security_code, raw_json
            ) VALUES ('DISC1', '2026-05-07', '15:00', '11110', '1111', ?)
            """,
            (json.dumps(row),),
        )

        result = run_jquants_official_cli_compare(
            self.conn,
            endpoint="fins.summary",
            date_value="2026-05-07",
            output_dir=self.tmp_path,
            official_cli="jquants",
            runner=_runner_for([row]),
        )

        self.assertEqual(result.official_rows, 1)
        self.assertEqual(result.db_rows, 1)
        self.assertEqual(result.matched_rows, 1)
        self.assertEqual(result.diff_count, 0)
        self.assertTrue(result.txt_path.exists())
        self.assertTrue(result.tsv_path.exists())

    def test_fins_summary_reports_missing_and_field_diff(self) -> None:
        db_row = {
            "DiscDate": "2026-05-07",
            "DiscTime": "15:00",
            "Code": "11110",
            "DiscNo": "DISC1",
            "DocType": "3Q",
            "Sales": 99,
        }
        official_rows = [
            {**db_row, "Sales": 100},
            {"DiscDate": "2026-05-07", "Code": "22220", "DiscNo": "DISC2", "Sales": 200},
        ]
        self.conn.execute(
            """
            INSERT INTO jquants_statement_raw (
                disclosure_number, disclosed_date, disclosed_time, local_code, security_code, raw_json
            ) VALUES ('DISC1', '2026-05-07', '15:00', '11110', '1111', ?)
            """,
            (json.dumps(db_row),),
        )

        result = run_jquants_official_cli_compare(
            self.conn,
            endpoint="fins.summary",
            date_value="2026-05-07",
            output_dir=self.tmp_path,
            official_cli="jquants",
            runner=_runner_for(official_rows),
        )

        self.assertEqual(result.missing_in_db, 1)
        self.assertEqual(result.field_diff_rows, 1)
        self.assertEqual(result.diff_count, 1)

    def test_eq_daily_compares_quote_raw_json(self) -> None:
        row = {
            "Date": "2026-05-07",
            "Code": "11110",
            "O": 100,
            "H": 110,
            "L": 90,
            "C": 105,
            "Vo": 12345,
            "AdjC": 105,
        }
        self.conn.execute(
            """
            INSERT INTO jquants_daily_quotes (local_code, security_code, trade_date, raw_json)
            VALUES ('11110', '1111', '2026-05-07', ?)
            """,
            (json.dumps(row),),
        )

        result = run_jquants_official_cli_compare(
            self.conn,
            endpoint="eq.daily",
            date_value="2026-05-07",
            code="1111",
            output_dir=self.tmp_path,
            official_cli="jquants",
            runner=_runner_for([row]),
        )

        self.assertEqual(result.diff_count, 0)
        self.assertEqual(result.matched_rows, 1)

    def test_fins_details_is_rejected_for_standard_plan(self) -> None:
        with self.assertRaises(OfficialCliCompareError):
            run_jquants_official_cli_compare(
                self.conn,
                endpoint="fins.details",
                date_value="2026-05-07",
                output_dir=self.tmp_path,
                official_cli="jquants",
                runner=_runner_for([]),
            )


if __name__ == "__main__":
    unittest.main()
