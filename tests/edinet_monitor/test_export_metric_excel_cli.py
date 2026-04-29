from __future__ import annotations

import io
import sqlite3
import shutil
import sys
import unittest
import uuid
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from openpyxl import Workbook


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_export_metric_excel_cli"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import export_metric_excel as cli  # noqa: E402
from tests.edinet_monitor.test_metric_excel_export_service import (  # noqa: E402
    _create_schema,
    _insert_company,
)


def _create_condition_workbook(path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "条件"
    ws.append(["証券コード", "1111"])
    ws.append(["指標", "売上高"])
    ws.append(["期間", "当期"])
    wb.save(path)


class ExportMetricExcelCliTest(unittest.TestCase):
    def test_main_writes_workbook_and_prints_preview_summary(self) -> None:
        tmp_path = TMP_ROOT / uuid.uuid4().hex
        tmp_path.mkdir(parents=True, exist_ok=True)
        try:
            condition_path = tmp_path / "condition.xlsx"
            output_dir = tmp_path / "out"
            _create_condition_workbook(condition_path)

            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            _create_schema(conn)
            _insert_company(
                conn,
                edinet_code="E00001",
                security_code="1111",
                company_name="A社",
                industry_33="化学",
                net_sales_values=[100_000_000.0, 80_000_000.0, 70_000_000.0],
            )
            conn.commit()

            stdout = io.StringIO()
            with (
                patch.object(
                    sys,
                    "argv",
                    [
                        "export_metric_excel",
                        "--condition-xlsx",
                        str(condition_path),
                        "--output-dir",
                        str(output_dir),
                        "--output-name",
                        "result.xlsx",
                    ],
                ),
                patch("edinet_monitor.cli.export_metric_excel.create_tables"),
                patch("edinet_monitor.cli.export_metric_excel.get_connection", return_value=conn),
                redirect_stdout(stdout),
            ):
                cli.main()

            output = stdout.getvalue()
            self.assertIn("output_path=", output)
            self.assertIn("target_companies=1", output)
            self.assertIn("output_rows=3", output)
            self.assertIn("preview_rows=3", output)
            self.assertIn("errors=0", output)
            self.assertTrue((output_dir / "result.xlsx").exists())
        finally:
            shutil.rmtree(tmp_path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
