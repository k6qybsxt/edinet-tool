from __future__ import annotations

import contextlib
import io
import json
import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_db_reflection_preflight_guard_service"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.db_reflection_preflight_guard_service import (  # noqa: E402
    mark_db_reflection_preflight_guard_success,
    run_db_reflection_preflight_guard,
)
from edinet_monitor.services.prevention_catalog_service import load_prevention_catalog  # noqa: E402


def _catalog_item(item_id: str, *, status: str = "active") -> dict[str, object]:
    return {
        "id": item_id,
        "title": item_id,
        "status": status,
        "severity": "critical",
        "areas": ["db_reflection"],
        "triggers": ["pre_db_reflection"],
        "problem": "problem",
        "prevention": "prevention",
        "review_points": ["point"],
    }


def _write_catalog(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "items": [
                    _catalog_item("active_item", status="active"),
                    _catalog_item("monitoring_item", status="monitoring"),
                    _catalog_item("retired_item", status="retired"),
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _create_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
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
    conn.commit()
    return conn


def _insert_item(
    conn: sqlite3.Connection,
    *,
    commands: list[str] | None = None,
    verification_sql: list[str] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO db_reflection_items (
            title, category, description, required_commands_json,
            verification_sql_json, related_migration_ids_json, source_path,
            source_key, notes, created_at, updated_at
        ) VALUES (?, 'recalculation', 'description', ?, ?, '[]', '', '', '', '2026-06-20', '2026-06-20')
        """,
        (
            "Reflection item",
            json.dumps(commands or [], ensure_ascii=False),
            json.dumps(verification_sql or [], ensure_ascii=False),
        ),
    )
    conn.commit()


class DbReflectionPreflightGuardServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"
        self.catalog_path = self.tmp_path / "catalog.json"
        self.output_dir = self.tmp_path / "reports"
        _write_catalog(self.catalog_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _run_guard(self):
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = run_db_reflection_preflight_guard(
                cli_name="test_cli",
                db_path=self.db_path,
                catalog_path=self.catalog_path,
                output_dir=self.output_dir,
            )
        return result, stdout.getvalue()

    def test_warning_only_does_not_block_and_success_moves_triggered_items_to_monitoring(self) -> None:
        conn = _create_db(self.db_path)
        try:
            _insert_item(
                conn,
                commands=["python -m edinet_monitor.cli.save_derived_metrics --run-all"],
                verification_sql=[
                    "SELECT COUNT(*) AS target_count FROM derived_metrics WHERE calc_status = 'ok' AND value_num IS NOT NULL"
                ],
            )
            before = conn.execute("SELECT COUNT(*) FROM db_reflection_items").fetchone()[0]
        finally:
            conn.close()

        result, output = self._run_guard()

        self.assertFalse(result.blocked)
        self.assertGreaterEqual(result.preflight.counts_by_severity["warning"], 1)
        self.assertIn("pipeline_failure_policy=block_on_critical", output)
        self.assertIn("db_reflection_blocked=False", output)
        by_id = {item.item_id: item.status for item in load_prevention_catalog(self.catalog_path)}
        self.assertEqual(by_id["active_item"], "triggered")
        self.assertEqual(by_id["monitoring_item"], "triggered")
        self.assertEqual(by_id["retired_item"], "retired")

        mark_db_reflection_preflight_guard_success(result, catalog_path=self.catalog_path)

        by_id = {item.item_id: item.status for item in load_prevention_catalog(self.catalog_path)}
        self.assertEqual(by_id["active_item"], "monitoring")
        self.assertEqual(by_id["monitoring_item"], "monitoring")
        self.assertEqual(by_id["retired_item"], "retired")
        conn = sqlite3.connect(self.db_path)
        try:
            after = conn.execute("SELECT COUNT(*) FROM db_reflection_items").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(before, after)
        self.assertTrue(result.preflight.json_path.exists())
        self.assertTrue(result.preflight.excel_path.exists())

    def test_critical_blocks_and_leaves_triggered_status(self) -> None:
        conn = _create_db(self.db_path)
        try:
            _insert_item(conn, commands=[], verification_sql=[])
        finally:
            conn.close()

        stdout = io.StringIO()
        with self.assertRaises(SystemExit) as context, contextlib.redirect_stdout(stdout):
            run_db_reflection_preflight_guard(
                cli_name="test_cli",
                db_path=self.db_path,
                catalog_path=self.catalog_path,
                output_dir=self.output_dir,
            )

        self.assertEqual(context.exception.code, 2)
        output = stdout.getvalue()
        self.assertIn("db_reflection_blocked=True", output)
        self.assertIn("preflight_blocked=critical", output)
        by_id = {item.item_id: item.status for item in load_prevention_catalog(self.catalog_path)}
        self.assertEqual(by_id["active_item"], "triggered")
        self.assertEqual(by_id["monitoring_item"], "triggered")
        self.assertEqual(by_id["retired_item"], "retired")


if __name__ == "__main__":
    unittest.main()
