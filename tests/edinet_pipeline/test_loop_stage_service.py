from __future__ import annotations

import shutil
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_pipeline.services.loop_stage_service import (  # noqa: E402
    build_excel_output_payload,
    build_stock_write_context,
    finalize_output_excel,
    pick_company_name,
    pick_period_end,
    resolve_document_display_unit,
)


class _FakeParsedDocument:
    def __init__(self, document_display_unit: str) -> None:
        self.document_display_unit = document_display_unit


class _FakeParseCache:
    def __init__(self, *documents: _FakeParsedDocument) -> None:
        self._documents = list(documents)
        self.calls: list[str] = []

    def get_or_create(self, path, parser_func):
        self.calls.append(path)
        if self._documents:
            return self._documents.pop(0)
        return parser_func(path)


class _DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None


class LoopStageServiceTest(unittest.TestCase):
    def test_build_excel_output_payload_adds_dates_and_filters_quarters(self) -> None:
        payload = build_excel_output_payload(
            {
                "NetSalesCurrent": 100,
                "NetSalesQuarter": 10,
            },
            x1={
                "CurrentFiscalYearEndDateDEI": "2026/03/31",
                "CurrentPeriodEndDateDEI": "2026/03/31",
                "CurrentFiscalYearStartDateDEI": "2025/04/01",
            },
            use_half=False,
        )

        self.assertEqual(payload["NetSalesCurrent"], 100)
        self.assertNotIn("NetSalesQuarter", payload)
        self.assertEqual(payload["CurrentFiscalYearEndDateDEI"], "2026-03-31")
        self.assertEqual(payload["CurrentFiscalYearEndDateDEIyear"], "2026")
        self.assertEqual(payload["CurrentFiscalYearEndDateDEImonth"], "03")
        self.assertEqual(payload["CurrentPeriodEndDateDEI"], "2026-03-31")
        self.assertEqual(payload["CurrentFiscalYearStartDateDEI"], "2025-04-01")

    def test_resolve_document_display_unit_uses_parse_cache_result(self) -> None:
        fake_cache = _FakeParseCache(_FakeParsedDocument("千円"))

        result = resolve_document_display_unit(
            xbrl_file_paths={"file1": ["sample1.xbrl"], "file2": []},
            x1={"DocumentDisplayUnit": "百万円"},
            x2=None,
            use_half=False,
            parse_cache=fake_cache,
            logger=_DummyLogger(),
            parse_document_func=lambda path, mode, logger: _FakeParsedDocument("百万円"),
        )

        self.assertEqual(result, "千円")
        self.assertEqual(fake_cache.calls, ["sample1.xbrl"])

    def test_build_stock_write_context_shifts_half_mode_year(self) -> None:
        context = build_stock_write_context(
            out_buffer_dict={"CurrentFiscalYearEndDateDEI": "2026-03-31"},
            x1=None,
            use_half=True,
            security_code="1234",
        )

        self.assertEqual(context["stock_code"], "1234.T")
        self.assertEqual(context["fiscal_year_end"], "2025-03-31")
        self.assertEqual(context["stock_date_pairs"][-1]["name"], "StockPrice_Q4")
        self.assertEqual(context["stock_date_pairs"][-1]["target_date"], "2025-03-31")

    def test_pick_helpers_fall_back_to_xbrl_metadata(self) -> None:
        period_end = pick_period_end(
            None,
            {"CurrentFiscalYearEndDateDEI": "2026/03/31"},
            None,
        )
        company_name = pick_company_name(
            None,
            {"CompanyNameInJapaneseDEI": "テスト株式会社"},
            None,
            None,
        )

        self.assertEqual(period_end, "2026-03-31")
        self.assertEqual(company_name, "テスト株式会社")

    def test_finalize_output_excel_moves_file_under_output_root(self) -> None:
        temp_root = ROOT_DIR / "tests" / "_tmp_loop_stage"
        source_file = temp_root / "source.xlsm"
        temp_root.mkdir(parents=True, exist_ok=True)
        source_file.write_bytes(b"dummy")

        try:
            final_path = finalize_output_excel(
                excel_file_path=str(source_file),
                output_root=str(temp_root),
                security_code="12340",
                company_name="テスト株式会社",
                period_end_date="2026-03-31",
                logger=_DummyLogger(),
            )

            self.assertTrue(Path(final_path).exists())
            self.assertEqual(Path(final_path).parent, temp_root / "excel")
            self.assertFalse(source_file.exists())
        finally:
            if temp_root.exists():
                shutil.rmtree(temp_root)


if __name__ == "__main__":
    unittest.main()
