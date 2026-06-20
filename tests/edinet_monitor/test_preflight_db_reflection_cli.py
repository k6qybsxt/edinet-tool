from __future__ import annotations

import io
import json
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
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_preflight_db_reflection_cli"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import preflight_db_reflection as cli  # noqa: E402


def _write_catalog(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "items": [
                    {
                        "id": "db_reflection",
                        "title": "DB reflection",
                        "status": "active",
                        "severity": "critical",
                        "areas": ["db_reflection"],
                        "triggers": ["pre_db_reflection"],
                        "problem": "problem",
                        "prevention": "prevention",
                        "review_points": ["point"],
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _create_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE db_reflection_items (
                item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                category TEXT NOT NULL,
                description TEXT NOT NULL,
                required_commands_json TEXT NOT NULL,
                verification_sql_json TEXT NOT NULL,
                related_migration_ids_json TEXT NOT NULL,
                source_path TEXT,
                source_key TEXT,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO db_reflection_items (
                title, category, description, required_commands_json,
                verification_sql_json, related_migration_ids_json, source_path,
                source_key, notes, created_at, updated_at
            ) VALUES (
                'Missing checks', 'recalculation', 'description', '[]',
                '[]', '[]', '', '', '', '2026-06-14', '2026-06-14'
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


class PreflightDbReflectionCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"
        self.catalog_path = self.tmp_path / "catalog.json"
        self.output_dir = self.tmp_path / "reports"
        _create_db(self.db_path)
        _write_catalog(self.catalog_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_main_prints_report_only_summary_with_critical_issues(self) -> None:
        stdout = io.StringIO()
        with (
            patch.object(
                sys,
                "argv",
                [
                    "preflight_db_reflection",
                    "--db-path",
                    str(self.db_path),
                    "--catalog-path",
                    str(self.catalog_path),
                    "--output-dir",
                    str(self.output_dir),
                ],
            ),
            redirect_stdout(stdout),
        ):
            cli.main()

        output = stdout.getvalue()
        self.assertIn("preflight_id=db_reflection_preflight_", output)
        self.assertIn("status=review_required", output)
        self.assertIn("pipeline_failure_policy=report_only", output)
        self.assertIn("db_reflection_blocked=False", output)
        self.assertIn("pending_count=1", output)
        self.assertIn("critical=2", output)
        self.assertIn("json_path=", output)
        self.assertIn("excel_path=", output)
        self.assertIn("history_saved=True", output)
        self.assertIn("history_status=report_only", output)
        self.assertIn("issue=critical|db_reflection_item|missing_required_command", output)

        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT status, critical_count FROM preflight_runs"
            ).fetchone()
            self.assertEqual(row[0], "report_only")
            self.assertEqual(row[1], 2)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
