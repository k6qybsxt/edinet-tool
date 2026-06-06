from __future__ import annotations

import io
import json
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
TMP_ROOT = ROOT_DIR / "tests" / "_tmp_audit_metric_excel_cli"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import audit_metric_excel as cli  # noqa: E402
from edinet_monitor.services.metric_excel_export_service import (  # noqa: E402
    MetricExcelCondition,
    build_metric_excel_rows,
    write_metric_excel,
)
from tests.edinet_monitor.test_metric_excel_audit_service import _write_target_config  # noqa: E402
from tests.edinet_monitor.test_metric_excel_export_service import (  # noqa: E402
    _create_schema,
    _insert_company,
)


class AuditMetricExcelCliTest(unittest.TestCase):
    def test_main_prints_summary_and_writes_json_and_excel(self) -> None:
        tmp_path = TMP_ROOT / uuid.uuid4().hex
        tmp_path.mkdir(parents=True, exist_ok=True)
        db_path = tmp_path / "monitor.db"
        output_dir = tmp_path / "reports"
        target_config = tmp_path / "targets.json"
        excel_path = tmp_path / "metric.xlsx"
        try:
            _write_target_config(target_config, "1111")
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            _create_schema(conn)
            _insert_company(
                conn,
                edinet_code="E00001",
                security_code="1111",
                company_name="A\u793e",
                industry_33="\u5316\u5b66",
                net_sales_values=[100_000_000.0, 80_000_000.0, 70_000_000.0],
            )
            conn.commit()
            condition = MetricExcelCondition(
                security_codes=["1111"],
                period_scopes=["annual"],
                period_offsets=[0],
            )
            rows, errors, warnings, _preview, target_companies = build_metric_excel_rows(conn, condition)
            self.assertEqual(errors, [])
            write_metric_excel(
                rows=rows,
                condition=condition,
                output_path=excel_path,
                db_path=db_path,
                errors=errors,
                warnings=warnings,
                target_companies=target_companies,
            )
            conn.close()

            stdout = io.StringIO()
            with (
                patch.object(
                    sys,
                    "argv",
                    [
                        "audit_metric_excel",
                        "--excel-path",
                        str(excel_path),
                        "--db-path",
                        str(db_path),
                        "--target-config",
                        str(target_config),
                        "--output-dir",
                        str(output_dir),
                        "--limit-preview",
                        "2",
                    ],
                ),
                redirect_stdout(stdout),
            ):
                cli.main()

            output = stdout.getvalue()
            self.assertIn("audit_id=", output)
            self.assertIn("critical=", output)
            self.assertIn("warning=", output)
            self.assertIn("json_path=", output)
            self.assertIn("excel_path=", output)
            self.assertEqual(len(list(output_dir.glob("metric_excel_audit_*.json"))), 1)
            self.assertEqual(len(list(output_dir.glob("metric_excel_audit_*.xlsx"))), 1)
        finally:
            shutil.rmtree(tmp_path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
