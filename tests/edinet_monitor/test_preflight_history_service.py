from __future__ import annotations

import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_preflight_history_service"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.db_reflection_preflight_service import (  # noqa: E402
    DbReflectionPreflightIssue,
    DbReflectionPreflightResult,
)
from edinet_monitor.services.preflight_history_service import (  # noqa: E402
    PreflightHistoryCleanupOptions,
    mark_preflight_history_completed,
    cleanup_preflight_history,
    save_preflight_history,
)


def _result(
    preflight_id: str,
    *,
    generated_at: str = "2026-06-20T00:00:00",
    status: str = "review_required",
    blocked: bool = False,
    critical: int = 0,
    warning: int = 1,
) -> DbReflectionPreflightResult:
    issues = []
    if warning:
        issues.append(
            DbReflectionPreflightIssue(
                severity="warning",
                category="verification_sql",
                check_name="weak_sql",
                item_id=1,
                title="item",
                message="warning",
                detail={"sql": "SELECT 1"},
            )
        )
    if critical:
        issues.append(
            DbReflectionPreflightIssue(
                severity="critical",
                category="db_reflection_item",
                check_name="missing_required_command",
                item_id=1,
                title="item",
                message="critical",
            )
        )
    return DbReflectionPreflightResult(
        preflight_id=preflight_id,
        generated_at=generated_at,
        status=status,
        json_path=Path("reports") / f"{preflight_id}.json",
        excel_path=Path("reports") / f"{preflight_id}.xlsx",
        summary={
            "guard_cli_name": "save_derived_metrics",
            "command_names": ("save_derived_metrics",),
            "pipeline_failure_policy": "block_on_critical",
            "db_reflection_blocked": blocked,
            "pending_count": 2,
            "matched_pending_count": 1,
        },
        pending_items=[],
        catalog_items=[],
        issues=issues,
        counts_by_severity={"critical": critical, "warning": warning, "info": 0},
    )


class PreflightHistoryServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp_path / "monitor.db"

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def test_save_is_idempotent_and_mark_completed_updates_status(self) -> None:
        conn = self._connect()
        try:
            save_result = save_preflight_history(conn, _result("pf1"))
            save_preflight_history(conn, _result("pf1"))
            self.assertEqual(save_result.status, "passed_with_warnings")

            run_count = conn.execute("SELECT COUNT(*) FROM preflight_runs").fetchone()[0]
            issue_count = conn.execute("SELECT COUNT(*) FROM preflight_run_issues").fetchone()[0]
            self.assertEqual(run_count, 1)
            self.assertEqual(issue_count, 1)

            self.assertTrue(mark_preflight_history_completed(conn, preflight_id="pf1"))
            row = conn.execute(
                "SELECT status, completed_at FROM preflight_runs WHERE preflight_id = 'pf1'"
            ).fetchone()
            self.assertEqual(row["status"], "completed")
            self.assertTrue(row["completed_at"])
        finally:
            conn.close()

    def test_cleanup_dry_run_and_apply_respect_critical_retention(self) -> None:
        conn = self._connect()
        try:
            save_preflight_history(
                conn,
                _result("normal_old", generated_at="2020-01-01T00:00:00", warning=0),
                status="completed",
            )
            save_preflight_history(
                conn,
                _result("blocked_recent_enough", generated_at="2025-01-01T00:00:00", blocked=True, critical=1, warning=0),
            )
            dry_run = cleanup_preflight_history(
                conn,
                options=PreflightHistoryCleanupOptions(
                    keep_days=180,
                    keep_critical_days=730,
                    apply=False,
                ),
            )
            self.assertEqual(dry_run.target_count, 1)
            self.assertEqual(dry_run.deleted_count, 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM preflight_runs").fetchone()[0], 2)

            applied = cleanup_preflight_history(
                conn,
                options=PreflightHistoryCleanupOptions(
                    keep_days=180,
                    keep_critical_days=730,
                    apply=True,
                ),
            )
            self.assertEqual(applied.target_count, 1)
            self.assertEqual(applied.deleted_count, 1)
            remaining = [
                row["preflight_id"]
                for row in conn.execute("SELECT preflight_id FROM preflight_runs ORDER BY preflight_id")
            ]
            self.assertEqual(remaining, ["blocked_recent_enough"])
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
