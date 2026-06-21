from __future__ import annotations

from contextlib import redirect_stdout
import io
import shutil
import sys
import unittest
import uuid
from pathlib import Path
from unittest import mock

from openpyxl import Workbook


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_review_excel_issue_intake_cli"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import review_excel_issue_intake  # noqa: E402


def _write_workbook(path: Path) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Data"
    sheet.append(["証券コード", "企業名", "決算種別", "指標", "2026 数値"])
    sheet.append(["7203", "トヨタ", "2Q", "2Q税引前利益", None])
    workbook.save(path)


class ReviewExcelIssueIntakeCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_cli_prints_summary_and_report_paths(self) -> None:
        excel_path = self.tmp_path / "input.xlsx"
        issue_text_path = self.tmp_path / "issue.txt"
        output_dir = self.tmp_path / "reports"
        _write_workbook(excel_path)
        issue_text_path.write_text("2Q税引前利益が出力されていない", encoding="utf-8")

        stdout = io.StringIO()
        with mock.patch(
            "sys.argv",
            [
                "review_excel_issue_intake",
                "--excel-path",
                str(excel_path),
                "--issue-text-path",
                str(issue_text_path),
                "--output-dir",
                str(output_dir),
                "--limit-preview",
                "2",
            ],
        ), redirect_stdout(stdout):
            review_excel_issue_intake.main()

        output = stdout.getvalue()
        self.assertIn("intake_id=excel_issue_intake_", output)
        self.assertIn("status=review_required", output)
        self.assertIn("blank_cell_count=1", output)
        self.assertIn("matched_issue_row_count=1", output)
        self.assertIn("json_path=", output)
        self.assertIn("excel_path=", output)
        self.assertEqual(len(list(output_dir.glob("excel_issue_intake_*.json"))), 1)
        self.assertEqual(len(list(output_dir.glob("excel_issue_intake_*.xlsx"))), 1)


if __name__ == "__main__":
    unittest.main()
