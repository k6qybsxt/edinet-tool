from __future__ import annotations

import json
import shutil
import sys
import unittest
import uuid
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_metric_excel_golden_master_service"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.metric_excel_export_service import (  # noqa: E402
    GENERAL_SHEET,
    MetricExcelCondition,
    MetricExcelRow,
    ROW_KIND_DETAIL,
    write_metric_excel,
)
from edinet_monitor.services.metric_excel_golden_master_service import (  # noqa: E402
    compare_metric_excel_golden_master,
    compare_metric_excel_normalized_payloads,
    normalize_metric_excel_workbook,
    write_metric_excel_normalized_json,
)


class MetricExcelGoldenMasterServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_path = TMP_ROOT / uuid.uuid4().hex
        self.tmp_path.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_path, ignore_errors=True)

    def _write_workbook(self, path: Path) -> None:
        rows = [
            MetricExcelRow(
                sheet_name=GENERAL_SHEET,
                security_code="1111",
                company_name="A\u793e",
                industry_33="\u5316\u5b66",
                market="Prime",
                period_scope="annual",
                current_period_end="2026-03-31",
                metric_base="NetSales",
                metric_label="4Q \u58f2\u4e0a\u9ad8",
                periods_by_offset={1: "\u901a\u671f 2025-03", 0: ""},
                values_by_offset={1: 123.0, 0: None},
                units_by_offset={1: "\u767e\u4e07\u5186", 0: ""},
                ratios_by_offset={1: None, 0: None},
                row_kind=ROW_KIND_DETAIL,
            )
        ]
        write_metric_excel(
            rows=rows,
            condition=MetricExcelCondition(period_offsets=[1, 0], segment_mode="none"),
            output_path=path,
            db_path=":memory:",
            errors=[],
            warnings=[],
            target_companies=1,
        )

    def test_normalize_excludes_generated_at_and_blank_period_cells(self) -> None:
        workbook_path = self.tmp_path / "input.xlsx"
        self._write_workbook(workbook_path)

        normalized = normalize_metric_excel_workbook(workbook_path)

        self.assertNotIn("generated_at", normalized["summary"])
        self.assertEqual(normalized["summary"]["periods"], "\u524d\u671f, \u5f53\u671f")
        rows = normalized["sheets"][0]["rows"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["metric_label"], "4Q \u58f2\u4e0a\u9ad8")
        self.assertEqual(rows[0]["periods"], [{"label": "\u524d\u671f", "offset": 1, "period": "\u901a\u671f 2025-03", "unit": "\u767e\u4e07\u5186", "value": 123}])

    def test_write_metric_excel_normalized_json(self) -> None:
        workbook_path = self.tmp_path / "input.xlsx"
        output_dir = self.tmp_path / "out"
        self._write_workbook(workbook_path)

        result = write_metric_excel_normalized_json(workbook_path, output_dir=output_dir)

        self.assertEqual(result.sheet_count, 4)
        self.assertEqual(result.row_count, 1)
        self.assertTrue(result.output_path.exists())
        payload = json.loads(result.output_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["source_excel_name"], "input.xlsx")

    def test_compare_normalized_payloads_reports_no_diff_for_same_payload(self) -> None:
        workbook_path = self.tmp_path / "input.xlsx"
        self._write_workbook(workbook_path)
        normalized = normalize_metric_excel_workbook(workbook_path)

        issues = compare_metric_excel_normalized_payloads(normalized, normalized)

        self.assertEqual(issues, [])

    def test_compare_normalized_payloads_reports_period_value_change(self) -> None:
        workbook_path = self.tmp_path / "input.xlsx"
        self._write_workbook(workbook_path)
        expected = normalize_metric_excel_workbook(workbook_path)
        actual = json.loads(json.dumps(expected, ensure_ascii=False))
        actual["sheets"][0]["rows"][0]["periods"][0]["value"] = 456

        issues = compare_metric_excel_normalized_payloads(expected, actual)

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].severity, "warning")
        self.assertEqual(issues[0].check_name, "period_field_changed")
        self.assertEqual(issues[0].field_name, "value")
        self.assertEqual(issues[0].expected_value, 123)
        self.assertEqual(issues[0].actual_value, 456)

    def test_compare_metric_excel_golden_master_writes_reports(self) -> None:
        workbook_path = self.tmp_path / "input.xlsx"
        output_dir = self.tmp_path / "reports"
        self._write_workbook(workbook_path)
        golden = write_metric_excel_normalized_json(
            workbook_path,
            output_path=self.tmp_path / "golden.json",
        )

        result = compare_metric_excel_golden_master(
            golden_json_path=golden.output_path,
            actual_excel_path=workbook_path,
            output_dir=output_dir,
        )

        self.assertEqual(result.issue_count, 0)
        self.assertTrue(result.actual_json_path.exists())
        self.assertTrue(result.report_json_path.exists())
        self.assertTrue(result.report_excel_path.exists())


if __name__ == "__main__":
    unittest.main()
