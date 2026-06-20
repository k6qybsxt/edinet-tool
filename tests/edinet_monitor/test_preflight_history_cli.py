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
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_preflight_history_cli"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import cleanup_preflight_history, review_preflight_history  # noqa: E402
from edinet_monitor.db.migrations import apply_schema_migrations  # noqa: E402


def _create_history_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        apply_schema_migrations(conn)
        conn.execute(
            """
            INSERT INTO preflight_runs (
                preflight_id, generated_at, cli_name, command_names_json,
                pipeline_failure_policy, db_reflection_blocked, status,
                pending_count, matched_pending_count, critical_count, warning_count,
                json_path, excel_path, completed_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "pf_warning",
                "2026-06-20T00:00:00",
                "save_derived_metrics",
                '["save_derived_metrics"]',
                "block_on_critical",
                0,
                "passed_with_warnings",
                1,
                1,
                0,
                1,
                "pf_warning.json",
                "pf_warning.xlsx",
                "",
                "2026-06-20T00:00:00",
                "2026-06-20T00:00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO preflight_run_issues (
                preflight_id, severity, category, check_name, item_id,
                title, message, detail_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "pf_warning",
                "warning",
                "verification_sql",
                "weak_sql",
                "1",
                "item",
                "warning",
                "{}",
                "2026-06-20T00:00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO preflight_runs (
                preflight_id, generated_at, cli_name, command_names_json,
                pipeline_failure_policy, db_reflection_blocked, status,
                pending_count, matched_pending_count, critical_count, warning_count,
                json_path, excel_path, completed_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "pf_old",
                "2020-01-01T00:00:00",
                "save_raw_facts",
                '["save_raw_facts"]',
                "block_on_critical",
                0,
                "completed",
                1,
                1,
                0,
                0,
                "pf_old.json",
                "pf_old.xlsx",
                "2020-01-01T00:10:00",
                "2020-01-01T00:00:00",
                "2020-01-01T00:10:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()


class PreflightHistoryCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"
        self.output_dir = self.tmp_path / "reports"
        _create_history_db(self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_review_preflight_history_filters_and_writes_reports(self) -> None:
        stdout = io.StringIO()
        with (
            patch.object(
                sys,
                "argv",
                [
                    "review_preflight_history",
                    "--db-path",
                    str(self.db_path),
                    "--days",
                    "9999",
                    "--warnings-only",
                    "--output-dir",
                    str(self.output_dir),
                ],
            ),
            redirect_stdout(stdout),
        ):
            review_preflight_history.main()

        output = stdout.getvalue()
        self.assertIn("review_id=preflight_history_review_", output)
        self.assertIn("run_count=1", output)
        self.assertIn("warning_run_count=1", output)
        self.assertIn("json_path=", output)
        self.assertIn("excel_path=", output)
        self.assertEqual(len(list(self.output_dir.glob("*.json"))), 1)
        self.assertEqual(len(list(self.output_dir.glob("*.xlsx"))), 1)

    def test_cleanup_preflight_history_dry_run_and_apply(self) -> None:
        stdout = io.StringIO()
        with (
            patch.object(
                sys,
                "argv",
                [
                    "cleanup_preflight_history",
                    "--db-path",
                    str(self.db_path),
                    "--keep-days",
                    "180",
                ],
            ),
            redirect_stdout(stdout),
        ):
            cleanup_preflight_history.main()

        output = stdout.getvalue()
        self.assertIn("mode=dry_run", output)
        self.assertIn("target_count=1", output)
        conn = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM preflight_runs").fetchone()[0], 2)
        finally:
            conn.close()

        stdout = io.StringIO()
        with (
            patch.object(
                sys,
                "argv",
                [
                    "cleanup_preflight_history",
                    "--db-path",
                    str(self.db_path),
                    "--keep-days",
                    "180",
                    "--apply",
                ],
            ),
            redirect_stdout(stdout),
        ):
            cleanup_preflight_history.main()

        output = stdout.getvalue()
        self.assertIn("mode=apply", output)
        self.assertIn("deleted_count=1", output)
        conn = sqlite3.connect(self.db_path)
        try:
            rows = conn.execute("SELECT preflight_id FROM preflight_runs").fetchall()
            self.assertEqual([row[0] for row in rows], ["pf_warning"])
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
