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
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_performance_report_cli"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import performance_report as cli  # noqa: E402
from edinet_monitor.db.schema import create_tables  # noqa: E402
from edinet_monitor.services.performance_log_service import save_performance_run  # noqa: E402


class PerformanceReportCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"
        create_tables(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        save_performance_run(
            self.conn,
            run_id="previous-run",
            command_name="save_raw_facts",
            stage_name="save_raw_facts",
            started_at="2026-05-30T10:00:00",
            finished_at="2026-05-30T10:00:20",
            elapsed_seconds=20.0,
            status="success",
            target_total=5,
            success_total=5,
        )
        save_performance_run(
            self.conn,
            run_id="latest-run",
            command_name="save_raw_facts",
            stage_name="save_raw_facts",
            started_at="2026-05-30T10:01:00",
            finished_at="2026-05-30T10:01:10",
            elapsed_seconds=10.0,
            status="success",
            target_total=5,
            success_total=5,
        )

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _run_cli(self, *argv: str) -> str:
        stdout = io.StringIO()
        with (
            patch.object(sys, "argv", ["performance_report", "--db-path", str(self.db_path), *argv]),
            redirect_stdout(stdout),
        ):
            cli.main()
        return stdout.getvalue()

    def test_list_prints_recent_runs(self) -> None:
        output = self._run_cli("list", "--command-name", "save_raw_facts", "--limit", "1")

        self.assertIn("run_id\tcommand_name\tstatus", output)
        self.assertIn("latest-run\tsave_raw_facts\tsuccess", output)
        self.assertNotIn("previous-run\tsave_raw_facts\tsuccess", output)

    def test_latest_prints_previous_diff(self) -> None:
        output = self._run_cli("latest", "--command-name", "save_raw_facts")

        self.assertIn("run_id=latest-run", output)
        self.assertIn("previous_run_id=previous-run", output)
        self.assertIn("elapsed_delta_seconds=-10.0", output)
        self.assertIn("processed_per_minute_delta=15.0", output)
        self.assertIn("error_delta=0", output)

    def test_latest_handles_no_runs(self) -> None:
        output = self._run_cli("latest", "--command-name", "missing_command")

        self.assertEqual(output.strip(), "no_runs=1")


if __name__ == "__main__":
    unittest.main()
