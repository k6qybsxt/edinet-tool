from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_daily_review_service"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.db.migrations import SCHEMA_MIGRATIONS  # noqa: E402
from edinet_monitor.services.daily_review_service import (  # noqa: E402
    DailyReviewOptions,
    build_daily_review,
)


def _create_review_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE schema_migrations (
            migration_id TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            applied_at TEXT NOT NULL
        );
        CREATE TABLE db_reflection_items (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            category TEXT NOT NULL,
            description TEXT NOT NULL,
            source_key TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE data_quality_report_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL UNIQUE,
            generated_at TEXT NOT NULL,
            date_from TEXT NOT NULL,
            date_to TEXT NOT NULL,
            condition_key TEXT NOT NULL,
            codes_json TEXT NOT NULL,
            industry_33_json TEXT NOT NULL,
            output_path TEXT,
            previous_run_id TEXT,
            total_items INTEGER NOT NULL DEFAULT 0,
            issue_count INTEGER NOT NULL DEFAULT 0,
            critical_count INTEGER NOT NULL DEFAULT 0,
            warning_count INTEGER NOT NULL DEFAULT 0,
            info_count INTEGER NOT NULL DEFAULT 0,
            summary_json TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE data_quality_report_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            item_key TEXT NOT NULL,
            category TEXT NOT NULL,
            severity TEXT NOT NULL,
            check_name TEXT NOT NULL,
            subject TEXT NOT NULL,
            current_value REAL,
            previous_value REAL,
            delta_value REAL,
            value_unit TEXT,
            message TEXT NOT NULL,
            detail_json TEXT,
            created_at TEXT NOT NULL
        );
        """
    )
    for migration in SCHEMA_MIGRATIONS[:-1]:
        conn.execute(
            """
            INSERT INTO schema_migrations (migration_id, description, applied_at)
            VALUES (?, ?, ?)
            """,
            (migration.migration_id, migration.description, "2026-06-06T00:00:00"),
        )
    conn.execute(
        """
        INSERT INTO db_reflection_items (
            title, category, description, source_key, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "Apply pending migration",
            "schema",
            "Apply the remaining schema migration.",
            "test::migration",
            "2026-06-06T01:00:00",
            "2026-06-06T01:00:00",
        ),
    )
    conn.execute(
        """
        INSERT INTO data_quality_report_runs (
            run_id, generated_at, date_from, date_to, condition_key,
            codes_json, industry_33_json, output_path, previous_run_id,
            total_items, issue_count, critical_count, warning_count, info_count,
            summary_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "dq_1",
            "2026-06-06T02:00:00",
            "2025-01-01",
            "2026-06-06",
            "all",
            "[]",
            "[]",
            "logs/data_quality.xlsx",
            "",
            3,
            1,
            0,
            1,
            2,
            json.dumps({"warning": 1}),
            "2026-06-06T02:00:00",
        ),
    )
    conn.execute(
        """
        INSERT INTO data_quality_report_items (
            run_id, item_key, category, severity, check_name, subject,
            current_value, previous_value, delta_value, value_unit, message,
            detail_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "dq_1",
            "coverage:warning:1111",
            "coverage",
            "warning",
            "missing_metric",
            "1111",
            1,
            None,
            None,
            "count",
            "missing metric",
            "{}",
            "2026-06-06T02:00:00",
        ),
    )
    conn.commit()


class DailyReviewServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"
        self.output_dir = self.tmp_path / "daily_review"
        self.normal_excel = self.tmp_path / "normal.xlsx"
        self.known_excel = self.tmp_path / "known.xlsx"
        self.normal_golden = self.tmp_path / "normal.normalized.json"
        self.known_golden = self.tmp_path / "known.normalized.json"
        self.catalog_path = self.tmp_path / "prevention_catalog.json"
        for path in (self.normal_excel, self.known_excel):
            path.write_bytes(b"placeholder")
        for path in (self.normal_golden, self.known_golden):
            path.write_text("{}", encoding="utf-8")
        self.catalog_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "items": [
                        {
                            "id": "active_item",
                            "title": "Active",
                            "status": "active",
                            "severity": "warning",
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

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        _create_review_tables(conn)
        return conn

    def test_build_daily_review_summarizes_sections_and_writes_reports(self) -> None:
        conn = self._connect()

        def fake_audit(_conn: sqlite3.Connection, options: object) -> SimpleNamespace:
            target_set = getattr(options, "target_set")
            critical = 1 if target_set == "known_issue" else 0
            return SimpleNamespace(
                audit_id=f"audit_{target_set}",
                generated_at="2026-06-06T03:00:00",
                excel_path=getattr(options, "excel_path"),
                json_path=self.output_dir / f"audit_{target_set}.json",
                report_excel_path=self.output_dir / f"audit_{target_set}.xlsx",
                target_set=target_set,
                targets=[object()],
                expected_rows=10,
                actual_rows=10,
                issues=[],
                errors=[],
                warnings=[],
                counts_by_severity={"critical": critical, "warning": 0},
                issue_count=critical,
            )

        def fake_diff(**kwargs: object) -> SimpleNamespace:
            actual_excel_path = Path(str(kwargs["actual_excel_path"]))
            warning = 1 if actual_excel_path.name == "normal.xlsx" else 0
            return SimpleNamespace(
                comparison_id=f"diff_{actual_excel_path.stem}",
                generated_at="2026-06-06T03:10:00",
                golden_json_path=Path(str(kwargs["golden_json_path"])),
                actual_excel_path=actual_excel_path,
                actual_json_path=self.output_dir / f"{actual_excel_path.stem}.actual.json",
                report_json_path=self.output_dir / f"{actual_excel_path.stem}.diff.json",
                report_excel_path=self.output_dir / f"{actual_excel_path.stem}.diff.xlsx",
                issues=[],
                counts_by_severity={"critical": 0, "warning": warning},
                issue_count=warning,
            )

        try:
            with (
                patch("edinet_monitor.services.daily_review_service.audit_metric_excel", fake_audit),
                patch("edinet_monitor.services.daily_review_service.compare_metric_excel_golden_master", fake_diff),
            ):
                result = build_daily_review(
                    conn,
                    DailyReviewOptions(
                        db_path=self.db_path,
                        normal_excel_path=self.normal_excel,
                        known_issue_excel_path=self.known_excel,
                        normal_golden_json_path=self.normal_golden,
                        known_issue_golden_json_path=self.known_golden,
                        catalog_path=self.catalog_path,
                        output_dir=self.output_dir,
                        retention_count=20,
                    ),
                )
        finally:
            remaining_reflection = conn.execute("SELECT COUNT(*) FROM db_reflection_items").fetchone()[0]
            conn.close()

        self.assertEqual(result.status, "review_required")
        self.assertTrue(result.json_path.exists())
        self.assertTrue(result.excel_path.exists())
        self.assertEqual(result.summary["pipeline_failure_policy"], "report_only")
        self.assertFalse(result.summary["pipeline_failed"])
        self.assertEqual(result.summary["schema_missing_count"], 1)
        self.assertEqual(result.summary["db_reflection_pending_count"], 1)
        self.assertEqual(result.summary["preflight_blocked_count"], 0)
        self.assertEqual(result.summary["preflight_catalog_triggered_count"], 0)
        self.assertEqual(result.summary["data_quality_warning_count"], 1)
        self.assertEqual(result.summary["excel_audit_critical_count"], 1)
        self.assertEqual(result.summary["golden_master_warning_count"], 1)
        self.assertEqual(remaining_reflection, 1)
        payload = json.loads(result.json_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["summary"]["status"], "review_required")
        self.assertEqual(len(payload["excel_audit_results"]["results"]), 2)

    def test_retention_keeps_latest_daily_review_files(self) -> None:
        conn = self._connect()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        for index in range(5):
            for suffix in ("json", "xlsx"):
                path = self.output_dir / f"daily_review_20260606_00000{index}.{suffix}"
                path.write_text("old", encoding="utf-8")
                os.utime(path, (index + 1, index + 1))

        def fake_audit(_conn: sqlite3.Connection, options: object) -> SimpleNamespace:
            return SimpleNamespace(
                audit_id="audit",
                generated_at="2026-06-06T03:00:00",
                excel_path=getattr(options, "excel_path"),
                json_path=self.output_dir / "audit.json",
                report_excel_path=self.output_dir / "audit.xlsx",
                targets=[],
                expected_rows=0,
                actual_rows=0,
                issues=[],
                errors=[],
                warnings=[],
                counts_by_severity={"critical": 0, "warning": 0},
                issue_count=0,
            )

        def fake_diff(**kwargs: object) -> SimpleNamespace:
            return SimpleNamespace(
                comparison_id="diff",
                generated_at="2026-06-06T03:10:00",
                golden_json_path=Path(str(kwargs["golden_json_path"])),
                actual_excel_path=Path(str(kwargs["actual_excel_path"])),
                actual_json_path=self.output_dir / "actual.json",
                report_json_path=self.output_dir / "diff.json",
                report_excel_path=self.output_dir / "diff.xlsx",
                issues=[],
                counts_by_severity={"critical": 0, "warning": 0},
                issue_count=0,
            )

        try:
            with (
                patch("edinet_monitor.services.daily_review_service.audit_metric_excel", fake_audit),
                patch("edinet_monitor.services.daily_review_service.compare_metric_excel_golden_master", fake_diff),
            ):
                build_daily_review(
                    conn,
                    DailyReviewOptions(
                        db_path=self.db_path,
                        normal_excel_path=self.normal_excel,
                        normal_golden_json_path=self.normal_golden,
                        catalog_path=self.catalog_path,
                        output_dir=self.output_dir,
                        retention_count=3,
                    ),
                )
        finally:
            conn.close()

        self.assertLessEqual(len(list(self.output_dir.glob("daily_review_*.json"))), 3)
        self.assertLessEqual(len(list(self.output_dir.glob("daily_review_*.xlsx"))), 3)

    def test_missing_excel_configuration_is_reported_as_review_error(self) -> None:
        conn = self._connect()
        try:
            result = build_daily_review(
                conn,
                DailyReviewOptions(
                    db_path=self.db_path,
                    catalog_path=self.catalog_path,
                    output_dir=self.output_dir,
                ),
            )
        finally:
            conn.close()

        self.assertEqual(result.status, "review_required")
        self.assertEqual(result.summary["review_error_count"], 2)
        self.assertEqual(result.excel_audit_results["status"], "not_configured")
        self.assertEqual(result.golden_master_diff_results["status"], "not_configured")

    def test_preflight_history_and_triggered_catalog_are_reported(self) -> None:
        conn = self._connect()
        conn.executescript(
            """
            CREATE TABLE preflight_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                preflight_id TEXT NOT NULL UNIQUE,
                generated_at TEXT NOT NULL,
                cli_name TEXT NOT NULL,
                command_names_json TEXT NOT NULL,
                pipeline_failure_policy TEXT NOT NULL,
                db_reflection_blocked INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                pending_count INTEGER NOT NULL DEFAULT 0,
                matched_pending_count INTEGER NOT NULL DEFAULT 0,
                critical_count INTEGER NOT NULL DEFAULT 0,
                warning_count INTEGER NOT NULL DEFAULT 0,
                json_path TEXT NOT NULL,
                excel_path TEXT NOT NULL,
                completed_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE preflight_run_issues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                preflight_id TEXT NOT NULL,
                severity TEXT NOT NULL,
                category TEXT NOT NULL,
                check_name TEXT NOT NULL,
                item_id TEXT,
                title TEXT,
                message TEXT NOT NULL,
                detail_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
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
                "pf_blocked",
                "2999-01-01T00:00:00",
                "save_derived_metrics",
                "[\"save_derived_metrics\"]",
                "block_on_critical",
                1,
                "blocked",
                1,
                1,
                1,
                0,
                "pf.json",
                "pf.xlsx",
                "",
                "2999-01-01T00:00:00",
                "2999-01-01T00:00:00",
            ),
        )
        self.catalog_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "items": [
                        {
                            "id": "triggered_item",
                            "title": "Triggered",
                            "status": "triggered",
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
        try:
            result = build_daily_review(
                conn,
                DailyReviewOptions(
                    db_path=self.db_path,
                    catalog_path=self.catalog_path,
                    output_dir=self.output_dir,
                    run_excel_audit=False,
                    run_golden_master_diff=False,
                ),
            )
        finally:
            conn.close()

        self.assertEqual(result.summary["preflight_blocked_count"], 1)
        self.assertEqual(result.summary["preflight_catalog_triggered_count"], 1)
        self.assertEqual(result.preflight_history["run_count"], 1)
        payload = json.loads(result.json_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["preflight_history"]["catalog_triggered_count"], 1)


if __name__ == "__main__":
    unittest.main()
