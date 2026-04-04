from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_pipeline.domain.filters import (  # noqa: E402
    filter_for_annual,
    filter_for_annual_old,
    filter_for_half,
)
from edinet_pipeline.domain.output_buffer import OutputBuffer  # noqa: E402
from edinet_pipeline.domain.raw_builder import (  # noqa: E402
    append_missing_annual_ytd_rows,
    attach_run_info,
    build_raw_rows_from_out,
    split_metric_timeslot,
)
from edinet_pipeline.domain.security_code import (  # noqa: E402
    ensure_security_code,
    normalize_security_code,
    pick_security_code,
)
from edinet_pipeline.domain.year_shift import (  # noqa: E402
    get_fy_end_year,
    shift_out_meta_by_yeargap,
    shift_suffixes_by_yeargap,
    shift_with_keep,
)


class FilterDomainTest(unittest.TestCase):
    def test_filter_for_annual_full_skips_quarter_and_expands_dei_parts(self) -> None:
        result = filter_for_annual(
            {
                "NetSalesCurrent": 100,
                "NetSalesQuarter": 25,
                "CurrentFiscalYearEndDateDEI": "2026-03-31",
            },
            use_half=False,
        )

        self.assertEqual(result["NetSalesCurrent"], 100)
        self.assertNotIn("NetSalesQuarter", result)
        self.assertEqual(result["UseHalfModeFlag"], 0)
        self.assertEqual(result["CurrentFiscalYearEndDateDEIyear"], "2026")
        self.assertEqual(result["CurrentFiscalYearEndDateDEImonth"], "3")

    def test_filter_for_annual_half_mode_shifts_suffixes(self) -> None:
        result = filter_for_annual(
            {
                "NetSalesCurrent": 100,
                "NetSalesPrior1": 90,
                "TotalNumberCurrent": 10,
                "TotalNumberPrior1": 9,
                "OperatingIncomeQuarter": 5,
            },
            use_half=True,
        )

        self.assertEqual(result["NetSalesPrior1"], 100)
        self.assertEqual(result["NetSalesPrior2"], 90)
        self.assertEqual(result["TotalNumberPrior1"], 10)
        self.assertEqual(result["OperatingIncomeQuarter"], 5)
        self.assertNotIn("TotalNumberPrior2", result)
        self.assertEqual(result["UseHalfModeFlag"], 0)

    def test_filter_for_half_removes_current_and_sets_flag(self) -> None:
        result = filter_for_half(
            {
                "NetSalesCurrent": 100,
                "NetSalesPrior1": 90,
                "CurrentFiscalYearEndDateDEI": "2026-03-31",
            }
        )

        self.assertNotIn("NetSalesCurrent", result)
        self.assertEqual(result["NetSalesPrior1"], 90)
        self.assertEqual(result["UseHalfModeFlag"], 1)
        self.assertEqual(result["CurrentFiscalYearEndDateDEIyear"], "2026")

    def test_filter_for_annual_old_keeps_non_empty_values(self) -> None:
        result = filter_for_annual_old(
            {
                "NetSalesCurrent": 100,
                "OperatingIncomeCurrent": "",
            }
        )

        self.assertEqual(result["NetSalesCurrent"], 100)
        self.assertNotIn("OperatingIncomeCurrent", result)
        self.assertEqual(result["UseHalfModeFlag"], 0)


class YearShiftDomainTest(unittest.TestCase):
    def test_get_fy_end_year_reads_first_current_like_value(self) -> None:
        self.assertEqual(
            get_fy_end_year({"CurrentFiscalYearEndDateDEI": "2026-03-31", "Other": "2025"}),
            2026,
        )
        self.assertIsNone(get_fy_end_year({"Prior1Date": "2025-03-31"}))

    def test_shift_suffixes_by_yeargap_handles_overflow(self) -> None:
        self.assertEqual(shift_suffixes_by_yeargap("NetSalesCurrent", 1), "NetSalesPrior1")
        self.assertEqual(shift_suffixes_by_yeargap("NetSalesPrior2", 2), "NetSalesPrior4")
        self.assertIsNone(shift_suffixes_by_yeargap("NetSalesPrior4", 1))
        self.assertEqual(shift_suffixes_by_yeargap("Metric", 2), "Metric")

    def test_shift_with_keep_and_out_meta_follow_suffix_shift(self) -> None:
        shifted_out = shift_with_keep(
            {"NetSalesCurrent": 100, "NetSalesPrior4": 1, "Metric": 5},
            1,
        )
        shifted_meta = shift_out_meta_by_yeargap(
            {"NetSalesCurrent": {"unit": "JPY"}, "Metric": {"unit": "JPY"}},
            2,
        )

        self.assertEqual(shifted_out["NetSalesPrior1"], 100)
        self.assertNotIn("NetSalesPrior4", shifted_out)
        self.assertEqual(shifted_out["Metric"], 5)
        self.assertIn("NetSalesPrior2", shifted_meta)
        self.assertEqual(shifted_meta["Metric"]["unit"], "JPY")


class SecurityCodeDomainTest(unittest.TestCase):
    def test_normalize_and_pick_security_code(self) -> None:
        self.assertEqual(normalize_security_code("1234-T"), "1234")
        self.assertIsNone(normalize_security_code("ABC"))
        self.assertEqual(pick_security_code(None, "ABC", "5678"), "5678")

    def test_ensure_security_code_prefers_meta_then_fallbacks(self) -> None:
        self.assertEqual(ensure_security_code({"security_code": "1234-T"}, "5678"), "1234")
        self.assertEqual(ensure_security_code({}, None, "5678"), "5678")
        self.assertIsNone(ensure_security_code(None, "ABC"))


class OutputBufferDomainTest(unittest.TestCase):
    def test_output_buffer_respects_priority_and_tracks_collisions(self) -> None:
        buffer = OutputBuffer()
        buffer.put("NetSalesCurrent", 100, "file3_annual")
        buffer.put("NetSalesCurrent", 120, "file2_annual")
        buffer.put("NetSalesCurrent", 140, "half_final")
        buffer.put("NetSalesCurrent", 80, "file3_annual")

        self.assertEqual(buffer.to_dict()["NetSalesCurrent"], 140)
        self.assertEqual(buffer.winner_of("NetSalesCurrent"), "half_final")
        self.assertEqual(len(buffer.collisions()), 3)

    def test_output_buffer_pop_and_empty_values(self) -> None:
        buffer = OutputBuffer()
        buffer.put("Metric", "", "file2_annual")
        buffer.put("Metric", None, "file2_annual")
        self.assertFalse(buffer.has("Metric"))

        buffer.put("Metric", 1, "file2_annual")
        self.assertTrue(buffer.has("Metric"))
        buffer.pop("Metric")
        self.assertFalse(buffer.has("Metric"))


class RawBuilderDomainTest(unittest.TestCase):
    def test_split_metric_timeslot(self) -> None:
        self.assertEqual(split_metric_timeslot("NetSales_YTD"), ("NetSales", "YTD"))
        self.assertEqual(split_metric_timeslot("NetSales_Quarter"), ("NetSales", "Quarter"))
        self.assertEqual(split_metric_timeslot("NetSalesCurrent"), ("NetSalesCurrent", None))

    def test_build_raw_rows_from_out_dedupes_same_raw_key(self) -> None:
        out = {
            "NetSalesCurrent": 100,
            "NetSalesCurrentDuplicate": 110,
        }
        out_meta = {
            "NetSalesCurrent": {
                "metric_key": "NetSalesCurrent",
                "consolidation": "C",
                "period_kind": "duration",
                "period_start": "2025-04-01",
                "period_end": "2026-03-31",
                "unit": "JPY",
                "tag_used": "NetSales",
                "tag_rank": 1,
                "status": "OK",
            },
            "NetSalesCurrentDuplicate": {
                "metric_key": "NetSalesCurrent",
                "consolidation": "C",
                "period_kind": "duration",
                "period_start": "2025-04-01",
                "period_end": "2026-03-31",
                "unit": "JPY",
                "tag_used": "NetSalesDup",
                "tag_rank": 2,
                "status": "OK",
            },
        }

        rows = build_raw_rows_from_out("1234", "doc1", "annual", out, out_meta)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["metric_key"], "NetSales")
        self.assertEqual(rows[0]["time_slot"], "Current")

    def test_append_missing_annual_ytd_rows_and_attach_run_info(self) -> None:
        raw_rows = [
            {
                "company_code": "1234",
                "doc_id": "doc1",
                "doc_type": "annual",
                "consolidation": "",
                "metric_key": "OperatingIncome",
                "time_slot": "Current",
                "period_kind": "duration",
            }
        ]

        append_missing_annual_ytd_rows(
            raw_rows,
            company_code="1234",
            doc_id="doc1",
            out_meta={},
            duration_metric_keys=["OperatingIncome", "NetSales"],
        )
        attach_run_info(raw_rows, "run1")

        self.assertEqual(len(raw_rows), 3)
        missing_rows = [row for row in raw_rows if row.get("time_slot") == "YTD"]
        self.assertEqual(len(missing_rows), 2)
        self.assertTrue(all(row["run_id"] == "run1" for row in raw_rows))
        self.assertTrue(all("dup_key" in row for row in raw_rows))


if __name__ == "__main__":
    unittest.main()
