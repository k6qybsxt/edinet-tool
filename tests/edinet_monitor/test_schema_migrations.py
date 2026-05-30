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
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_schema_migrations"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import apply_schema_migrations as cli  # noqa: E402
from edinet_monitor.db.migrations import apply_schema_migrations  # noqa: E402
from edinet_monitor.db.schema import create_tables  # noqa: E402


class SchemaMigrationsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_create_tables_applies_schema_migrations_once(self) -> None:
        create_tables(self.db_path)
        create_tables(self.db_path)

        conn = sqlite3.connect(self.db_path)
        try:
            rows = conn.execute(
                """
                SELECT migration_id
                FROM schema_migrations
                ORDER BY migration_id
                """
            ).fetchall()
            self.assertEqual(
                [row[0] for row in rows],
                [
                    "001_baseline_current_schema",
                    "002_add_data_quality_report_tables",
                    "003_add_db_reflection_items",
                    "004_add_pipeline_performance_logs",
                ],
            )
            count = conn.execute(
                """
                SELECT COUNT(*)
                FROM data_quality_report_runs
                """
            ).fetchone()[0]
            self.assertEqual(count, 0)
            reflection_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM db_reflection_items
                """
            ).fetchone()[0]
            self.assertEqual(reflection_count, 0)
            performance_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM pipeline_performance_runs
                """
            ).fetchone()[0]
            self.assertEqual(performance_count, 0)
        finally:
            conn.close()

    def test_dry_run_reports_pending_without_creating_migration_table(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            statuses = apply_schema_migrations(conn, dry_run=True)
            self.assertTrue(all(not status.applied for status in statuses))
            row = conn.execute(
                """
                SELECT 1
                FROM sqlite_master
                WHERE type = 'table' AND name = 'schema_migrations'
                """
            ).fetchone()
            self.assertIsNone(row)
        finally:
            conn.close()

    def test_apply_schema_migrations_cli_dry_run_prints_pending(self) -> None:
        stdout = io.StringIO()
        with (
            patch.object(sys, "argv", ["apply_schema_migrations", "--db-path", str(self.db_path), "--dry-run"]),
            redirect_stdout(stdout),
        ):
            cli.main()

        output = stdout.getvalue()
        self.assertIn("001_baseline_current_schema\tpending", output)
        self.assertIn("002_add_data_quality_report_tables\tpending", output)
        self.assertIn("003_add_db_reflection_items\tpending", output)
        self.assertIn("004_add_pipeline_performance_logs\tpending", output)

    def test_apply_schema_migrations_cli_limits_applied_display_without_deleting_rows(self) -> None:
        create_tables(self.db_path)

        stdout = io.StringIO()
        with (
            patch.object(
                sys,
                "argv",
                [
                    "apply_schema_migrations",
                    "--db-path",
                    str(self.db_path),
                    "--applied-limit",
                    "1",
                ],
            ),
            redirect_stdout(stdout),
        ):
            cli.main()

        output = stdout.getvalue()
        self.assertEqual(output.count("\tapplied\t"), 1)

        conn = sqlite3.connect(self.db_path)
        try:
            row_count = conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
            self.assertEqual(row_count, 4)
        finally:
            conn.close()

    def test_apply_schema_migrations_cli_all_displays_all_applied_rows(self) -> None:
        create_tables(self.db_path)

        stdout = io.StringIO()
        with (
            patch.object(
                sys,
                "argv",
                [
                    "apply_schema_migrations",
                    "--db-path",
                    str(self.db_path),
                    "--applied-limit",
                    "1",
                    "--all",
                ],
            ),
            redirect_stdout(stdout),
        ):
            cli.main()

        output = stdout.getvalue()
        self.assertEqual(output.count("\tapplied\t"), 4)


if __name__ == "__main__":
    unittest.main()
