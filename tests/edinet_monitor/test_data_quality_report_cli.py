from __future__ import annotations

import io
import shutil
import sqlite3
import sys
import unittest
import uuid
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_data_quality_report_cli"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import data_quality_report as cli  # noqa: E402
from edinet_monitor.db.schema import create_tables  # noqa: E402
from tests.edinet_monitor.test_data_quality_report_service import _insert_fixture  # noqa: E402


class DataQualityReportCliTest(unittest.TestCase):
    def test_main_prints_summary_and_writes_excel(self) -> None:
        tmp_path = TMP_ROOT / uuid.uuid4().hex
        tmp_path.mkdir(parents=True, exist_ok=True)
        db_path = tmp_path / "monitor.db"
        output_dir = tmp_path / "reports"
        try:
            create_tables(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                _insert_fixture(conn)
            finally:
                conn.close()

            stdout = io.StringIO()
            with (
                patch.object(
                    sys,
                    "argv",
                    [
                        "data_quality_report",
                        "--db-path",
                        str(db_path),
                        "--output-dir",
                        str(output_dir),
                        "--date-from",
                        "2025-01-01",
                        "--date-to",
                        "2025-12-31",
                        "--codes",
                        "1111",
                        "--limit-preview",
                        "3",
                    ],
                ),
                redirect_stdout(stdout),
            ):
                cli.main()

            output = stdout.getvalue()
            self.assertIn("report_id=", output)
            self.assertIn("issue_count=", output)
            self.assertIn("critical=", output)
            self.assertIn("warning=", output)
            self.assertIn("previous_run_id=", output)
            self.assertIn("excel_path=", output)
            self.assertEqual(len(list(output_dir.glob("data_quality_report_*.xlsx"))), 1)
        finally:
            shutil.rmtree(tmp_path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
