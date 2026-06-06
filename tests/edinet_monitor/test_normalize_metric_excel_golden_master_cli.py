from __future__ import annotations

import io
import shutil
import sys
import unittest
import uuid
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_normalize_metric_excel_golden_master_cli"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import normalize_metric_excel_golden_master as cli  # noqa: E402
from edinet_monitor.services.metric_excel_export_service import (  # noqa: E402
    GENERAL_SHEET,
    MetricExcelCondition,
    MetricExcelRow,
    ROW_KIND_DETAIL,
    write_metric_excel,
)


def _write_workbook(path: Path) -> None:
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
            periods_by_offset={1: "\u901a\u671f 2025-03"},
            values_by_offset={1: 123.0},
            units_by_offset={1: "\u767e\u4e07\u5186"},
            ratios_by_offset={1: None},
            row_kind=ROW_KIND_DETAIL,
        )
    ]
    write_metric_excel(
        rows=rows,
        condition=MetricExcelCondition(period_offsets=[1]),
        output_path=path,
        db_path=":memory:",
        errors=[],
        warnings=[],
        target_companies=1,
    )


class NormalizeMetricExcelGoldenMasterCliTest(unittest.TestCase):
    def test_main_writes_normalized_json(self) -> None:
        tmp_path = TMP_ROOT / uuid.uuid4().hex
        tmp_path.mkdir(parents=True, exist_ok=True)
        workbook_path = tmp_path / "input.xlsx"
        output_dir = tmp_path / "json"
        _write_workbook(workbook_path)
        try:
            stdout = io.StringIO()
            with (
                patch.object(
                    sys,
                    "argv",
                    [
                        "normalize_metric_excel_golden_master",
                        "--excel-path",
                        str(workbook_path),
                        "--output-dir",
                        str(output_dir),
                    ],
                ),
                redirect_stdout(stdout),
            ):
                cli.main()

            output = stdout.getvalue()
            self.assertIn("json_path=", output)
            self.assertIn("sheet_count=4", output)
            self.assertIn("row_count=1", output)
            self.assertTrue((output_dir / "input.normalized.json").exists())
        finally:
            shutil.rmtree(tmp_path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
