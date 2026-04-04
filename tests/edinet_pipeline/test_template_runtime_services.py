from __future__ import annotations

import logging
import shutil
import sys
import unittest
from pathlib import Path

from openpyxl import load_workbook


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_pipeline.config.runtime import RuntimeConfig  # noqa: E402
from edinet_pipeline.config.settings import TEMPLATE_DIR, TEMPLATE_WORKBOOK_NAME  # noqa: E402
from edinet_pipeline.services.excel_service import (  # noqa: E402
    build_namedrange_cache,
    write_data_to_workbook_namedranges,
)
from edinet_pipeline.services.main_setup_service import create_main_parse_cache  # noqa: E402
from edinet_pipeline.services.summary_service import write_batch_reports  # noqa: E402
from edinet_pipeline.services.template_contract_service import validate_template_contract  # noqa: E402


TEMPLATE_PATH = TEMPLATE_DIR / TEMPLATE_WORKBOOK_NAME


class TemplateRuntimeServicesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger("edinet-pipeline-test")
        self.logger.handlers.clear()
        self.logger.addHandler(logging.NullHandler())

    def test_template_contract_matches_current_template(self) -> None:
        report = validate_template_contract(
            TEMPLATE_PATH,
            include_stock_ranges=True,
        )

        self.assertEqual(report["missing_sheets"], [])
        self.assertEqual(report["missing_named_ranges"], [])
        self.assertGreaterEqual(
            report["defined_name_count"],
            report["required_named_range_count"],
        )

    def test_named_range_write_scales_values_and_reports_optional_missing_names(self) -> None:
        workbook = load_workbook(
            TEMPLATE_PATH,
            keep_vba=True,
        )
        try:
            namedrange_cache = build_namedrange_cache(workbook)

            self.assertIn("NetSales_Current", namedrange_cache)

            result = write_data_to_workbook_namedranges(
                workbook,
                {
                    "NetSalesCurrent": 1_000_000,
                    "CurrentPeriodEndDateDEI": "2026-03-31",
                },
                display_unit="百万円",
            )

            written_cells = namedrange_cache["NetSales_Current"]
            self.assertTrue(any(cell.value == 1 for cell in written_cells))
            self.assertIn("CurrentPeriodEndDateDEI", result["missing"])
            self.assertEqual(workbook["決算入力"]["J2"].value, "百万円")
        finally:
            try:
                if getattr(workbook, "vba_archive", None) is not None:
                    workbook.vba_archive.close()
                    workbook.vba_archive = None
            except Exception:
                pass
            workbook.close()

    def test_create_main_parse_cache_respects_runtime_config(self) -> None:
        runtime = RuntimeConfig(parse_cache_max_items=23)

        cache = create_main_parse_cache(
            logger=self.logger,
            runtime=runtime,
        )

        self.assertEqual(cache.max_items, 23)

    def test_write_batch_reports_respects_write_company_jobs_flag(self) -> None:
        job_inputs = [
            {
                "slot": 1,
                "company_code": "12340",
                "company_name": "テスト株式会社",
                "has_half": False,
                "file1": "sample1.xbrl",
                "file2": "sample2.xbrl",
                "file3": "",
            }
        ]
        batch_results = [
            {
                "slot": 1,
                "company_code": "12340",
                "company_name": "テスト株式会社",
                "status": "success",
                "stock_status": "success",
                "failure_reason": None,
                "error_detail": None,
                "output_excel": "sample.xlsm",
            }
        ]

        output_root = ROOT_DIR / "tests" / "_tmp_reports"
        if output_root.exists():
            shutil.rmtree(output_root)
        output_root.mkdir(parents=True, exist_ok=True)

        try:
            disabled_report = write_batch_reports(
                output_root=output_root,
                job_inputs=job_inputs,
                batch_results=batch_results,
                logger=self.logger,
                runtime=RuntimeConfig(write_company_jobs_csv=False),
            )

            self.assertIsNone(disabled_report["jobs_csv"])
            self.assertFalse((output_root / "reports" / "company_jobs.csv").exists())
            self.assertTrue(disabled_report["summary_csv"].exists())
            self.assertTrue(disabled_report["failed_csv"].exists())

            enabled_report = write_batch_reports(
                output_root=output_root,
                job_inputs=job_inputs,
                batch_results=batch_results,
                logger=self.logger,
                runtime=RuntimeConfig(write_company_jobs_csv=True),
            )

            self.assertIsNotNone(enabled_report["jobs_csv"])
            self.assertTrue((output_root / "reports" / "company_jobs.csv").exists())
        finally:
            if output_root.exists():
                shutil.rmtree(output_root)


if __name__ == "__main__":
    unittest.main()
