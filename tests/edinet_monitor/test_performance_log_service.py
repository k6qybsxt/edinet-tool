from __future__ import annotations

import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_performance_log_service"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.db.schema import create_tables  # noqa: E402
from edinet_monitor.services.performance_log_service import (  # noqa: E402
    PerformanceSpan,
    save_performance_run,
)


class PerformanceLogServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"
        create_tables(self.db_path)
        self.conn = sqlite3.connect(self.db_path)

    def tearDown(self) -> None:
        self.conn.close()
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_save_performance_run_persists_run_and_spans(self) -> None:
        run = save_performance_run(
            self.conn,
            run_id="run-1",
            command_name="save_raw_facts",
            stage_name="save_raw_facts",
            started_at="2026-05-30T10:00:00",
            finished_at="2026-05-30T10:00:10",
            elapsed_seconds=10.0,
            status="success",
            workers=1,
            batch_size=20,
            target_total=5,
            success_total=5,
            output_rows_total=100,
            parameters={"form_codes": ["043000"]},
            summary={"saved_rows_total": 100},
            spans=[
                PerformanceSpan("db_read", "fetch_targets", 1.25),
                PerformanceSpan("parse", "parse_xbrl", 6.0),
                PerformanceSpan("db_write", "insert_raw_facts", 2.0),
            ],
        )

        self.assertEqual(run.command_name, "save_raw_facts")
        self.assertEqual(run.status, "success")
        self.assertEqual(run.db_read_elapsed_seconds, 1.25)
        self.assertEqual(run.parse_elapsed_seconds, 6.0)
        self.assertEqual(run.db_write_elapsed_seconds, 2.0)
        self.assertEqual(run.processed_per_minute, 30.0)

        span_count = self.conn.execute(
            "SELECT COUNT(*) FROM pipeline_performance_spans WHERE run_id = 'run-1'"
        ).fetchone()[0]
        self.assertEqual(span_count, 3)

    def test_save_performance_run_persists_error_summary(self) -> None:
        run = save_performance_run(
            self.conn,
            run_id="run-error",
            command_name="save_raw_facts",
            stage_name="save_raw_facts",
            started_at="2026-05-30T10:00:00",
            finished_at="2026-05-30T10:00:01",
            elapsed_seconds=1.0,
            status="error",
            error_total=1,
            error_summary={"unhandled_error": "RuntimeError('boom')"},
        )

        self.assertEqual(run.status, "error")
        self.assertEqual(run.error_summary["unhandled_error"], "RuntimeError('boom')")

    def test_prunes_to_latest_100_runs_per_command(self) -> None:
        for index in range(101):
            save_performance_run(
                self.conn,
                run_id=f"run-{index:03d}",
                command_name="save_raw_facts",
                stage_name="save_raw_facts",
                started_at=f"2026-05-30T10:{index // 60:02d}:{index % 60:02d}",
                finished_at=f"2026-05-30T10:{index // 60:02d}:{index % 60:02d}",
                elapsed_seconds=1.0,
                status="success",
                success_total=1,
                spans=[PerformanceSpan("compute", "work", 0.1)],
            )

        run_count = self.conn.execute(
            "SELECT COUNT(*) FROM pipeline_performance_runs WHERE command_name = 'save_raw_facts'"
        ).fetchone()[0]
        old_run = self.conn.execute(
            "SELECT 1 FROM pipeline_performance_runs WHERE run_id = 'run-000'"
        ).fetchone()
        old_span = self.conn.execute(
            "SELECT 1 FROM pipeline_performance_spans WHERE run_id = 'run-000'"
        ).fetchone()

        self.assertEqual(run_count, 100)
        self.assertIsNone(old_run)
        self.assertIsNone(old_span)


if __name__ == "__main__":
    unittest.main()
