from __future__ import annotations

import unittest

from edinet_monitor.services.normalizer.metric_normalize_service import (
    normalize_raw_fact_row,
    normalize_raw_fact_rows,
)


def build_raw_fact(*, doc_id: str = "DOC1", tag_name: str, value_text: str = "123", context_ref: str = "CurrentYearDuration_ConsolidatedMember", period_type: str = "duration", period_end: str = "2025-03-31", consolidation: str = "Consolidated") -> dict:
    return {
        "doc_id": doc_id,
        "tag_name": tag_name,
        "context_ref": context_ref,
        "unit_ref": "JPY",
        "period_type": period_type,
        "period_start": "2024-04-01",
        "period_end": period_end,
        "instant_date": None,
        "consolidation": consolidation,
        "value_text": value_text,
    }


class MetricNormalizeServiceTest(unittest.TestCase):
    def test_operating_gross_profit_maps_to_gross_profit(self) -> None:
        row = build_raw_fact(tag_name="OperatingGrossProfit")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "GrossProfitCurrent")
        self.assertEqual(normalized["source_tag"], "OperatingGrossProfit")

    def test_operating_cost_maps_to_cost_of_sales(self) -> None:
        row = build_raw_fact(tag_name="OperatingCost")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9501",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "CostOfSalesCurrent")
        self.assertEqual(normalized["source_tag"], "OperatingCost")

    def test_cost_of_raw_materials_maps_to_cost_of_sales(self) -> None:
        row = build_raw_fact(tag_name="CostOfRawMaterialsCOS")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="9708",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "CostOfSalesCurrent")
        self.assertEqual(normalized["source_tag"], "CostOfRawMaterialsCOS")

    def test_cost_of_completed_work_maps_to_cost_of_sales(self) -> None:
        row = build_raw_fact(tag_name="CostOfCompletedWorkCOSExpOA")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="2153",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["metric_key"], "CostOfSalesCurrent")
        self.assertEqual(normalized["source_tag"], "CostOfCompletedWorkCOSExpOA")

    def test_business_expenses_maps_to_combined_cost_and_sga(self) -> None:
        row = build_raw_fact(tag_name="BusinessExpenses")

        normalized = normalize_raw_fact_row(
            row,
            edinet_code="E00000",
            security_code="4579",
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(
            normalized["metric_key"],
            "CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent",
        )
        self.assertEqual(normalized["source_tag"], "BusinessExpenses")

    def test_combined_cost_and_sga_prefers_total_operating_expenses_tag(self) -> None:
        rows = [
            build_raw_fact(
                doc_id="DOC2",
                tag_name="OperatingExpenses",
                value_text="100",
            ),
            build_raw_fact(
                doc_id="DOC2",
                tag_name="OperatingExpensesIFRS",
                value_text="120",
            ),
        ]

        normalized_rows = normalize_raw_fact_rows(
            rows,
            edinet_code="E00000",
            security_code="9432",
        )

        self.assertEqual(len(normalized_rows), 1)
        self.assertEqual(
            normalized_rows[0]["metric_key"],
            "CostOfSalesAndSellingGeneralAndAdministrativeExpensesCurrent",
        )
        self.assertEqual(normalized_rows[0]["source_tag"], "OperatingExpensesIFRS")
        self.assertEqual(normalized_rows[0]["value_num"], 120.0)


if __name__ == "__main__":
    unittest.main()
