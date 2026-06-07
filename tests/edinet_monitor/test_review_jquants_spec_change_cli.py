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
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_review_jquants_spec_change_cli"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import review_jquants_spec_change as cli  # noqa: E402
from edinet_monitor.services.jquants_spec_review_service import (  # noqa: E402
    JQuantsSpecReviewIssue,
    JQuantsSpecReviewResult,
)


class ReviewJQuantsSpecChangeCliTest(unittest.TestCase):
    def test_main_prints_summary(self) -> None:
        tmp_path = TMP_ROOT / uuid.uuid4().hex
        tmp_path.mkdir(parents=True, exist_ok=True)
        db_path = tmp_path / "monitor.db"
        sqlite3.connect(db_path).close()
        output_dir = tmp_path / "reports"
        try:
            fake_result = JQuantsSpecReviewResult(
                review_id="jquants_spec_review_test",
                generated_at="2026-06-07T10:00:00",
                status="review_required",
                json_path=output_dir / "jquants_spec_review_test.json",
                excel_path=output_dir / "jquants_spec_review_test.xlsx",
                counts_by_severity={"critical": 1, "warning": 2, "info": 0},
                issues=[
                    JQuantsSpecReviewIssue(
                        severity="critical",
                        category="schema",
                        check_name="removed_field",
                        endpoint="fins.summary",
                        field_name="Sales",
                        message="field removed",
                    )
                ],
                summary={},
                schema_diff=[],
                raw_compare=[],
                official_cli_commands=[],
            )
            stdout = io.StringIO()
            with (
                patch.object(
                    sys,
                    "argv",
                    [
                        "review_jquants_spec_change",
                        "--db-path",
                        str(db_path),
                        "--endpoints",
                        "fins.summary,eq.daily",
                        "--date",
                        "2026-05-07",
                        "--code",
                        "72030",
                        "--baseline-dir",
                        str(tmp_path / "baseline"),
                        "--output-dir",
                        str(output_dir),
                    ],
                ),
                patch(
                    "edinet_monitor.cli.review_jquants_spec_change.build_jquants_spec_review",
                    return_value=fake_result,
                ),
                redirect_stdout(stdout),
            ):
                cli.main()

            output = stdout.getvalue()
            self.assertIn("review_id=jquants_spec_review_test", output)
            self.assertIn("status=review_required", output)
            self.assertIn("critical=1", output)
            self.assertIn("warning=2", output)
            self.assertIn("json_path=", output)
            self.assertIn("excel_path=", output)
            self.assertIn("issue=critical|schema|removed_field|fins.summary|Sales|field removed", output)
        finally:
            shutil.rmtree(tmp_path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
