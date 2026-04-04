from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_pipeline.services.loop_processor import process_one_loop  # noqa: E402


class _DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def debug(self, *args, **kwargs):
        return None


class ProcessOneLoopTest(unittest.TestCase):
    def test_process_one_loop_returns_failed_result_when_excel_stage_fails(self) -> None:
        loop = {
            "slot": 1,
            "company_code": "12340",
            "company_name": "Test",
            "has_half": False,
            "source_zips": [],
            "output_root": "D:\\out",
            "xbrl_file_paths": {"file1": [], "file2": [], "file3": []},
        }

        with patch("edinet_pipeline.services.loop_processor.prepare_excel_stage") as prepare_excel_stage:
            prepare_excel_stage.return_value = {
                "selected_file": None,
                "excel_file_path": None,
                "excel_base_name": "missing.xlsm",
                "failed_result": {
                    "slot": 1,
                    "company_code": "12340",
                    "company_name": "Test",
                    "status": "failed",
                    "stock_status": None,
                    "output_excel": None,
                },
            }

            result = process_one_loop(
                loop=loop,
                date_pairs=[],
                skipped_files=[],
                logger=_DummyLogger(),
                parse_cache=None,
                runtime=None,
            )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["company_code"], "12340")

    def test_process_one_loop_orchestrates_stage_services(self) -> None:
        loop = {
            "slot": 1,
            "company_code": "12340",
            "company_name": "Test",
            "has_half": False,
            "source_zips": ["a.zip"],
            "output_root": "D:\\out",
            "xbrl_file_paths": {"file1": ["f1"], "file2": ["f2"], "file3": []},
        }
        parse_stage_result = {
            "x1": {"CompanyNameDEI": "Test"},
            "x2": None,
            "meta2": None,
            "security_code": "12340",
            "base_year": 2026,
            "use_half": False,
        }

        with (
            patch("edinet_pipeline.services.loop_processor.prepare_excel_stage") as prepare_excel_stage,
            patch("edinet_pipeline.services.loop_processor.run_parse_stages") as run_parse_stages,
            patch("edinet_pipeline.services.loop_processor.build_excel_write_inputs_stage") as build_excel_write_inputs_stage,
            patch("edinet_pipeline.services.loop_processor.build_raw_rows_stage") as build_raw_rows_stage,
            patch("edinet_pipeline.services.loop_processor.resolve_runtime_flags") as resolve_runtime_flags,
            patch("edinet_pipeline.services.loop_processor.run_workbook_output_stages") as run_workbook_output_stages,
            patch("edinet_pipeline.services.loop_processor.finalize_company_result_stage") as finalize_company_result_stage,
        ):
            prepare_excel_stage.return_value = {
                "selected_file": "template.xlsm",
                "excel_file_path": "C:\\temp\\workbook.xlsm",
                "excel_base_name": "template.xlsm",
                "failed_result": None,
            }
            run_parse_stages.return_value = parse_stage_result
            build_excel_write_inputs_stage.return_value = ({"NetSalesCurrent": 100}, "百万円")
            build_raw_rows_stage.return_value = [{"metric_key": "NetSales"}]
            resolve_runtime_flags.return_value = {"write_raw_sheet": False, "enable_stock": True}
            run_workbook_output_stages.return_value = {"stock_status": "success"}
            finalize_company_result_stage.return_value = {
                "slot": 1,
                "company_code": "12340",
                "company_name": "Test",
                "status": "success",
                "stock_status": "success",
                "output_excel": "D:\\out\\excel\\12340_Test_2026-03-31.xlsm",
            }

            result = process_one_loop(
                loop=loop,
                date_pairs=[],
                skipped_files=[],
                logger=_DummyLogger(),
                parse_cache=None,
                runtime=object(),
            )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["stock_status"], "success")
        run_parse_stages.assert_called_once()
        build_excel_write_inputs_stage.assert_called_once()
        build_raw_rows_stage.assert_called_once()
        resolve_runtime_flags.assert_called_once()
        run_workbook_output_stages.assert_called_once()
        finalize_company_result_stage.assert_called_once()


if __name__ == "__main__":
    unittest.main()
