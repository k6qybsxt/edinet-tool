from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path
from unittest import mock


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_db_reflection_preflight_service"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.db_reflection_preflight_service import (  # noqa: E402
    DbReflectionPreflightOptions,
    build_db_reflection_preflight,
)


def _write_catalog(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "items": [
                    {
                        "id": "db_reflection_preflight",
                        "title": "DB reflection preflight",
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


def _create_reflection_table(conn: sqlite3.Connection) -> None:
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


def _insert_item(
    conn: sqlite3.Connection,
    *,
    title: str = "Rebuild metrics",
    commands: list[str] | None = None,
    verification_sql: list[str] | None = None,
    description: str = "description",
    notes: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO db_reflection_items (
            title, category, description, required_commands_json,
            verification_sql_json, related_migration_ids_json, source_path,
            source_key, notes, created_at, updated_at
        ) VALUES (?, 'recalculation', ?, ?, ?, '[]', '', '', ?, '2026-06-14', '2026-06-14')
        """,
        (
            title,
            description,
            json.dumps(commands or [], ensure_ascii=False),
            json.dumps(verification_sql or [], ensure_ascii=False),
            notes,
        ),
    )
    conn.commit()


class DbReflectionPreflightServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"
        self.catalog_path = self.tmp_path / "catalog.json"
        _write_catalog(self.catalog_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _options(self) -> DbReflectionPreflightOptions:
        return DbReflectionPreflightOptions(
            db_path=self.db_path,
            catalog_path=self.catalog_path,
            output_dir=self.tmp_path / "reports",
        )

    def _guard_options(self, *command_names: str) -> DbReflectionPreflightOptions:
        return DbReflectionPreflightOptions(
            db_path=self.db_path,
            catalog_path=self.catalog_path,
            output_dir=self.tmp_path / "reports",
            pipeline_failure_policy="block_on_critical",
            guard_cli_name=command_names[0] if command_names else "",
            command_names=tuple(command_names),
        )

    def test_pending_none_is_ok(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            _create_reflection_table(conn)
            result = build_db_reflection_preflight(conn, self._options())
        finally:
            conn.close()

        self.assertEqual(result.status, "ok")
        self.assertEqual(result.pending_items, [])
        self.assertEqual(result.issues, [])
        self.assertEqual(len(result.catalog_items), 1)

    def test_missing_required_command_and_verification_sql_are_critical(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            _create_reflection_table(conn)
            _insert_item(conn, commands=[], verification_sql=[])
            result = build_db_reflection_preflight(conn, self._options())
        finally:
            conn.close()

        checks = {issue.check_name for issue in result.issues if issue.severity == "critical"}
        self.assertIn("missing_required_command", checks)
        self.assertIn("missing_verification_sql", checks)
        self.assertEqual(result.counts_by_severity["critical"], 2)

    def test_run_all_without_scope_and_weak_verification_sql_are_warnings(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            _create_reflection_table(conn)
            _insert_item(
                conn,
                commands=["python -m edinet_monitor.cli.save_derived_metrics --run-all"],
                verification_sql=["SELECT COUNT(*) FROM derived_metrics"],
            )
            result = build_db_reflection_preflight(conn, self._options())
        finally:
            conn.close()

        warnings = {issue.check_name for issue in result.issues if issue.severity == "warning"}
        self.assertIn("full_scope_command_without_scope", warnings)
        self.assertIn("verification_calc_status_missing", warnings)
        self.assertIn("verification_value_num_condition_missing", warnings)
        self.assertNotIn("target_count_not_confirmed", warnings)

    def test_read_only_connection_does_not_modify_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            _create_reflection_table(conn)
            _insert_item(
                conn,
                commands=["python -m edinet_monitor.cli.save_derived_metrics --date-from 2026-01-01"],
                verification_sql=[
                    "SELECT COUNT(*) FROM derived_metrics WHERE calc_status = 'ok' AND value_num IS NOT NULL"
                ],
            )
            before = conn.execute("SELECT COUNT(*) FROM db_reflection_items").fetchone()[0]
        finally:
            conn.close()

        uri = self.db_path.resolve().as_uri() + "?mode=ro"
        ro_conn = sqlite3.connect(uri, uri=True)
        ro_conn.row_factory = sqlite3.Row
        ro_conn.execute("PRAGMA query_only = ON")
        try:
            result = build_db_reflection_preflight(ro_conn, self._options())
        finally:
            ro_conn.close()

        conn = sqlite3.connect(self.db_path)
        try:
            after = conn.execute("SELECT COUNT(*) FROM db_reflection_items").fetchone()[0]
        finally:
            conn.close()

        self.assertEqual(before, after)
        self.assertEqual(len(result.pending_items), 1)
        self.assertTrue(result.json_path.exists())
        self.assertTrue(result.excel_path.exists())

    def test_command_names_filter_pending_items_and_summary_counts(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            _create_reflection_table(conn)
            _insert_item(
                conn,
                title="Raw facts",
                commands=["python -m edinet_monitor.cli.save_raw_facts --run-all"],
                verification_sql=[
                    "SELECT COUNT(*) AS target_count FROM raw_facts"
                ],
            )
            _insert_item(
                conn,
                title="Derived",
                commands=["python -m edinet_monitor.cli.save_derived_metrics --run-all"],
                verification_sql=[
                    "SELECT COUNT(*) AS target_count FROM derived_metrics WHERE calc_status = 'ok' AND value_num IS NOT NULL"
                ],
            )
            result = build_db_reflection_preflight(conn, self._guard_options("save_raw_facts"))
        finally:
            conn.close()

        self.assertEqual(result.summary["pending_count"], 2)
        self.assertEqual(result.summary["matched_pending_count"], 1)
        self.assertEqual([item.title for item in result.pending_items], ["Raw facts"])
        self.assertEqual(result.summary["command_names"], ("save_raw_facts",))

    def test_unrelated_critical_pending_item_does_not_block_command_guard(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            _create_reflection_table(conn)
            _insert_item(
                conn,
                title="Unrelated critical",
                commands=["python -m edinet_monitor.cli.save_derived_metrics --run-all"],
                verification_sql=[],
            )
            result = build_db_reflection_preflight(conn, self._guard_options("save_raw_facts"))
        finally:
            conn.close()

        self.assertEqual(result.summary["pending_count"], 1)
        self.assertEqual(result.summary["matched_pending_count"], 0)
        self.assertFalse(result.summary["db_reflection_blocked"])
        self.assertEqual(result.counts_by_severity["critical"], 0)

    def test_related_critical_pending_item_blocks_command_guard(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            _create_reflection_table(conn)
            _insert_item(
                conn,
                title="Related critical",
                commands=["python -m edinet_monitor.cli.save_raw_facts --run-all"],
                verification_sql=[],
            )
            result = build_db_reflection_preflight(conn, self._guard_options("save_raw_facts"))
        finally:
            conn.close()

        self.assertEqual(result.summary["pending_count"], 1)
        self.assertEqual(result.summary["matched_pending_count"], 1)
        self.assertTrue(result.summary["db_reflection_blocked"])
        self.assertGreater(result.counts_by_severity["critical"], 0)

    def test_large_db_size_alone_does_not_warn(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            _create_reflection_table(conn)
            _insert_item(
                conn,
                title="Scoped update",
                commands=[
                    "python -m edinet_monitor.cli.save_derived_metrics --doc-id S100AAAA --batch-size 100"
                ],
                verification_sql=[
                    "SELECT COUNT(*) AS target_count FROM derived_metrics WHERE calc_status = 'ok' AND value_num IS NOT NULL"
                ],
            )
            with mock.patch(
                "edinet_monitor.services.db_reflection_preflight_service._db_file_size_bytes",
                return_value=101 * 1024 * 1024 * 1024,
            ):
                result = build_db_reflection_preflight(conn, self._options())
        finally:
            conn.close()

        warnings = {issue.check_name for issue in result.issues if issue.severity == "warning"}
        self.assertNotIn("large_db_full_scope_command", warnings)
        self.assertEqual(result.summary["db_size_gb"], 101.0)
        self.assertEqual(result.counts_by_severity["critical"], 0)

    def test_large_db_full_scope_command_is_warning_when_target_count_exists(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            _create_reflection_table(conn)
            _insert_item(
                conn,
                title="All derived metrics",
                commands=[
                    "python -m edinet_monitor.cli.save_derived_metrics --run-all --batch-size 100 --form-codes 030000"
                ],
                verification_sql=[
                    "SELECT COUNT(*) AS target_count FROM derived_metrics WHERE calc_status = 'ok' AND value_num IS NOT NULL"
                ],
            )
            with mock.patch(
                "edinet_monitor.services.db_reflection_preflight_service._db_file_size_bytes",
                return_value=101 * 1024 * 1024 * 1024,
            ):
                result = build_db_reflection_preflight(conn, self._options())
        finally:
            conn.close()

        warnings = {issue.check_name for issue in result.issues if issue.severity == "warning"}
        self.assertIn("large_db_full_scope_command", warnings)
        self.assertEqual(result.counts_by_severity["critical"], 0)

    def test_full_scope_without_target_count_is_critical(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            _create_reflection_table(conn)
            _insert_item(
                conn,
                title="All market metrics",
                commands=[
                    "python -m edinet_monitor.cli.save_market_derived_metrics --codes all --apply"
                ],
                verification_sql=["SELECT 1"],
            )
            result = build_db_reflection_preflight(conn, self._guard_options("save_market_derived_metrics"))
        finally:
            conn.close()

        critical = {issue.check_name for issue in result.issues if issue.severity == "critical"}
        self.assertIn("full_scope_command_without_target_count", critical)
        self.assertTrue(result.summary["db_reflection_blocked"])


if __name__ == "__main__":
    unittest.main()
