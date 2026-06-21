from __future__ import annotations

import json
import shutil
import sys
import unittest
import uuid
from pathlib import Path

from openpyxl import Workbook


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_excel_issue_intake_service"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.excel_issue_intake_service import (  # noqa: E402
    ExcelIssueIntakeOptions,
    build_excel_issue_intake,
)


def _write_sample_workbook(path: Path) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Data"
    sheet.append(["証券コード", "企業名", "決算種別", "年度", "指標", "2025 数値", "2026 数値"])
    sheet.append(["7203", "トヨタ", "2Q", "2026", "2Q売上総利益", 100, None])
    sheet.append(["7203", "トヨタ", "2Q", "2026", "2Q株価", 0, 1000])
    meta = workbook.create_sheet("Meta")
    meta.append(["note"])
    meta.append(["not a metric sheet"])
    workbook.save(path)


class ExcelIssueIntakeServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def test_builds_report_and_treats_zero_as_value(self) -> None:
        excel_path = self.tmp_path / "issue.xlsx"
        issue_text_path = self.tmp_path / "issue.txt"
        _write_sample_workbook(excel_path)
        issue_text_path.write_text("2Q売上総利益が出力されていません。", encoding="utf-8")

        result = build_excel_issue_intake(
            ExcelIssueIntakeOptions(
                excel_path=excel_path,
                issue_text_path=issue_text_path,
                output_dir=self.tmp_path / "reports",
            )
        )

        self.assertEqual(result.status, "review_required")
        self.assertEqual(result.summary["sheet_count"], 2)
        self.assertEqual(result.summary["priority_sheet_count"], 1)
        self.assertEqual(result.summary["blank_cell_count"], 1)
        self.assertEqual(len(result.blank_cells), 1)
        self.assertEqual(result.blank_cells[0].header, "2026 数値")
        self.assertEqual(result.blank_cells[0].metric, "2Q売上総利益")
        self.assertEqual(len(result.matched_rows), 1)
        self.assertTrue(result.json_path.exists())
        self.assertTrue(result.report_excel_path.exists())

        payload = json.loads(result.json_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["summary"]["blank_cell_count"], 1)


if __name__ == "__main__":
    unittest.main()
