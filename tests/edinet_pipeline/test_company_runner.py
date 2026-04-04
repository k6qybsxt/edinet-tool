from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_pipeline.config.settings import TEMPLATE_WORKBOOK_NAME  # noqa: E402
from edinet_pipeline.services.company_runner import (  # noqa: E402
    build_loop_input,
    run_company_job,
)


class _DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def debug(self, *args, **kwargs):
        return None


class CompanyRunnerTest(unittest.TestCase):
    def test_build_loop_input_creates_expected_xbrl_lists(self) -> None:
        loop = build_loop_input(
            {
                "slot": 1,
                "company_code": "12340",
                "company_name": "Test",
                "has_half": False,
                "source_zips": ["a.zip"],
                "file1": "f1.xbrl",
                "file2": "",
                "file3": None,
            },
            output_root=Path("D:/output"),
            template_dir=Path("C:/templates"),
        )

        self.assertEqual(loop["slot"], 1)
        self.assertEqual(loop["xbrl_file_paths"]["file1"], ["f1.xbrl"])
        self.assertEqual(loop["xbrl_file_paths"]["file2"], [])
        self.assertEqual(loop["xbrl_file_paths"]["file3"], [])
        self.assertTrue(loop["excel_file_path"].endswith(TEMPLATE_WORKBOOK_NAME))

    def test_run_company_job_wraps_process_result_in_dataclass(self) -> None:
        job = {
            "slot": 1,
            "company_code": "12340",
            "company_name": "Test",
            "has_half": False,
            "source_zips": [],
            "file1": "f1.xbrl",
            "file2": "",
            "file3": "",
        }

        with patch("edinet_pipeline.services.company_runner.process_one_loop") as process_one_loop:
            process_one_loop.return_value = {
                "slot": 1,
                "company_code": "12340",
                "company_name": "Test",
                "status": "success",
                "stock_status": "success",
                "output_excel": "D:/output/excel/test.xlsm",
            }

            result = run_company_job(
                job=job,
                date_pairs=[],
                output_root=Path("D:/output"),
                template_dir=Path("C:/templates"),
                skipped_files=[],
                logger=_DummyLogger(),
                parse_cache=None,
                runtime=None,
            )

        self.assertEqual(result.slot, 1)
        self.assertEqual(result.company_code, "12340")
        self.assertEqual(result.status, "success")
        self.assertEqual(result.stock_status, "success")


if __name__ == "__main__":
    unittest.main()
