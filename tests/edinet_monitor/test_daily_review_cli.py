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
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_daily_review_cli"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import daily_review as cli  # noqa: E402
from edinet_monitor.services.daily_review_service import DailyReviewResult  # noqa: E402


class DailyReviewCliTest(unittest.TestCase):
    def test_main_prints_daily_review_summary(self) -> None:
        tmp_path = TMP_ROOT / uuid.uuid4().hex
        tmp_path.mkdir(parents=True, exist_ok=True)
        db_path = tmp_path / "monitor.db"
        sqlite3.connect(db_path).close()
        output_dir = tmp_path / "reports"
        try:
            fake_result = DailyReviewResult(
                review_id="daily_review_test",
                generated_at="2026-06-06T04:00:00",
                status="review_required",
                json_path=output_dir / "daily_review_test.json",
                excel_path=output_dir / "daily_review_test.xlsx",
                summary={
                    "pipeline_failure_policy": "report_only",
                    "pipeline_failed": False,
                    "schema_missing_count": 1,
                    "db_reflection_pending_count": 2,
                    "data_quality_critical_count": 3,
                    "data_quality_warning_count": 4,
                    "excel_audit_critical_count": 5,
                    "excel_audit_warning_count": 6,
                    "golden_master_critical_count": 7,
                    "golden_master_warning_count": 8,
                    "review_error_count": 9,
                },
                schema_migrations={},
                db_reflection_items={},
                data_quality_report={},
                excel_audit_results={},
                golden_master_diff_results={},
            )
            stdout = io.StringIO()
            with (
                patch.object(
                    sys,
                    "argv",
                    [
                        "daily_review",
                        "--db-path",
                        str(db_path),
                        "--normal-excel",
                        str(tmp_path / "normal.xlsx"),
                        "--known-issue-excel",
                        str(tmp_path / "known.xlsx"),
                        "--output-dir",
                        str(output_dir),
                    ],
                ),
                patch("edinet_monitor.cli.daily_review.build_daily_review", return_value=fake_result),
                redirect_stdout(stdout),
            ):
                cli.main()

            output = stdout.getvalue()
            self.assertIn("review_id=daily_review_test", output)
            self.assertIn("status=review_required", output)
            self.assertIn("pipeline_failure_policy=report_only", output)
            self.assertIn("pipeline_failed=False", output)
            self.assertIn("schema_missing=1", output)
            self.assertIn("db_reflection_pending=2", output)
            self.assertIn("data_quality_critical=3", output)
            self.assertIn("excel_audit_critical=5", output)
            self.assertIn("golden_master_warning=8", output)
            self.assertIn("review_error_count=9", output)
            self.assertIn("json_path=", output)
            self.assertIn("excel_path=", output)
        finally:
            shutil.rmtree(tmp_path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
